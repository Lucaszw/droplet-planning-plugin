"""
Copyright 2015 Christian Fobel

This file is part of droplet_planning_plugin.

droplet_planning_plugin is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

dmf_control_board is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with droplet_planning_plugin.  If not, see <http://www.gnu.org/licenses/>
"""
from collections import OrderedDict
from datetime import datetime
from threading import Timer
import json
import logging
import signal
import sys

from flatland import Integer, Form
from flatland.validation import ValueAtLeast
from flatland_helpers import flatlandToDict

import pandas as pd
from pandas_helpers import PandasJsonEncoder
import paho_mqtt_helpers as pmh
from si_prefix import si_format

logger = logging.getLogger(__name__)


class RouteController(object):
    '''
    Manage execution of a set of routes in lock-step.
    '''
    def __init__(self, parent=None):
        self.parent = parent
        self.route_info = {}

    @staticmethod
    def default_routes():
        return pd.DataFrame(None, columns=['route_i', 'electrode_i',
                                           'transition_i'], dtype='int32')

    def execute_routes(self, df_routes, transition_duration_ms,
                       on_complete=None, on_error=None, trail_length=1,
                       cyclic=True, acyclic=True):
        '''
        Begin execution of a set of routes.

        Args:

            df_routes (pandas.DataFrame) : Table of route transitions.
            transition_duration_ms (int) : Duration of each transition.
            on_complete (function) : Callback function called upon completed
                execution of all routes.
            on_error (function) : Callback function called upon error during
                execution of any route.
            cyclic (bool) : Execute cyclic routes (i.e., a route that ends on
                the same electrode that it starts on).
            acyclic (bool) : Execute acyclic routes.
        '''
        # Stop execution (if running).
        self.reset()

        route_info = {}
        self.route_info = route_info

        route_info['df_routes'] = df_routes
        route_info['transition_counter'] = 0
        route_info['transition_duration_ms'] = transition_duration_ms
        route_info['trail_length'] = trail_length

        # Find cycle routes, i.e., where first electrode matches last
        # electrode.
        route_starts = df_routes.groupby('route_i').nth(0)['electrode_i']
        route_ends = df_routes.groupby('route_i').nth(-1)['electrode_i']
        route_info['cycles'] = route_starts[route_starts == route_ends]
        cyclic_mask = df_routes.route_i.isin(route_info['cycles']
                                             .index.tolist())
        if not cyclic:
            df_routes = df_routes.loc[~cyclic_mask].copy()
        elif not acyclic:
            df_routes = df_routes.loc[cyclic_mask].copy()
        elif not cyclic and not acyclic:
            df_routes = df_routes.iloc[0:0]
        route_info['df_routes'] = df_routes.copy()
        route_info['electrode_ids'] = df_routes.electrode_i.unique()

        route_groups = route_info['df_routes'].groupby('route_i')
        # Get the number of transitions in each drop route.
        route_info['route_lengths'] = route_groups['route_i'].count()
        route_info['df_routes']['route_length'] = (route_info['route_lengths']
                                                   [route_info['df_routes']
                                                    .route_i].values)
        route_info['df_routes']['cyclic'] = (route_info['df_routes'].route_i
                                             .isin(route_info['cycles']
                                                   .index.tolist()))

        # Look up the drop routes for the current.
        route_info['routes'] = OrderedDict([(route_j, df_route_j)
                                            for route_j, df_route_j in
                                            route_groups])
        route_info['start_time'] = datetime.now()

        self.check_routes_progress(on_complete, on_error, False)

    def check_routes_progress(self, on_complete, on_error, continue_=True):
        '''
        Callback called by periodic timeout at intervals of
        `transition_duration_ms` until all routes are completed.
        '''
        if 'route_lengths' not in self.route_info:
            return False
        route_info = self.route_info
        try:
            stop_i = route_info['route_lengths'].max()
            logger.debug('[check_routes_progress] stop_i: %s', stop_i)
            if (route_info['transition_counter'] < stop_i):
                # There is at least one route with remaining transitions to
                # execute.
                self.execute_transition()
                route_info['transition_counter'] += 1

                # Execute remaining route transitions periodically, at the
                # specified interval duration.
                Timer(route_info['transition_duration_ms']/500.,
                      self.check_routes_progress,
                      (on_complete, on_error)).start()

            else:
                # All route transitions have executed.
                self.reset()
                if on_complete is not None:
                    on_complete(route_info['start_time'],
                                route_info['electrode_ids'])
        except:
            # An error occurred while executing routes.
            if on_error is not None:
                on_error()
        return False

    def execute_transition(self):
        '''
        Execute a single transition (corresponding to the current transition
        index) in each route with a sufficient number of transitions.
        '''
        route_info = self.route_info

        # Trail follows transition corresponding to *transition counter* by
        # the specified *trail length*.
        start_i = route_info['transition_counter']
        end_i = (route_info['transition_counter'] + route_info['trail_length']
                 - 1)

        if start_i == end_i:
            logger.debug('[execute_transition] %s', start_i)
        else:
            logger.debug('[execute_transition] %s-%s', start_i, end_i)

        df_routes = route_info['df_routes']
        start_i_mod = start_i % df_routes.route_length
        end_i_mod = end_i % df_routes.route_length

        #  1. Within the specified trail length of the current transition
        #     counter of a single pass.
        single_pass_mask = ((df_routes.transition_i >= start_i) &
                            (df_routes.transition_i <= end_i))
        #  2. Within the specified trail length of the current transition
        #     counter in the second route pass.
        second_pass_mask = (max(end_i, start_i) < 2 * df_routes.route_length)
        #  3. Start marker is higher than end marker, i.e., end has wrapped
        #     around to the start of the route.
        wrap_around_mask = ((end_i_mod < start_i_mod) &
                            ((df_routes.transition_i >= start_i_mod) |
                             (df_routes.transition_i <= end_i_mod + 1)))

        # Find active transitions based on the transition counter.
        active_transition_mask = (single_pass_mask |
                                  # Only consider wrap-around transitions for
                                  # the second pass of cyclic routes.
                                  (df_routes.cyclic & second_pass_mask &
                                   wrap_around_mask))
        # (subsequent_pass_mask | wrap_around_mask)))

        df_routes['active'] = active_transition_mask.astype(int)
        active_electrode_mask = (df_routes.groupby('electrode_i')['active']
                                 .sum())

        # An electrode may appear twice in the list of modified electrode
        # states in cases where the same channel is mapped to multiple
        # electrodes.
        #
        # Sort electrode states with "on" electrodes listed first so the "on"
        # state will take precedence when the electrode controller plugin drops
        # duplicate states for the same electrode.
        modified_electrode_states = (active_electrode_mask.astype(bool)
                                     .sort_values(ascending=False))

        data = {}
        data['electrode_states'] = modified_electrode_states
        data['save'] = False

        msg = json.dumps(data, cls=PandasJsonEncoder)

        # topic = 'microdrop/droplet-planning-plugin/set-electrode-states'
        topic = 'microdrop/put/electrodes-model/electrode-states'
        self.parent.mqtt_client.publish(topic, msg)

    def reset(self):
        '''
        Reset execution state.
        '''
        if 'timeout_id' in self.route_info:
            del self.route_info['timeout_id']

        if ('electrode_ids' in self.route_info and
           (self.route_info['electrode_ids'].shape[0] > 0)):
            # At least one route exists.
            # Deactivate all electrodes belonging to all routes.
            electrode_states = pd.Series(0, index=self.route_info
                                         ['electrode_ids'], dtype=int)

            data = {}
            data['electrode_states'] = electrode_states
            data['save'] = False

            self.parent.mqtt_client.publish('microdrop/droplet-planning-plugin'
                                            '/set-electrode-states',
                                            json.dumps(data,
                                                       cls=PandasJsonEncoder))

        self.route_info = {'transition_counter': 0}


class DropletPlanningPlugin(pmh.BaseMqttReactor):
    """
    This class is automatically registered with the PluginManager.
    """

    '''
    StepFields
    ---------

    A flatland Form specifying the per step options for the current plugin.
    Note that nested Form objects are not supported.

    Since we subclassed StepOptionsController, an API is available to access
    and modify these attributes.  This API also provides some nice features
    automatically:
        -all fields listed here will be included in the protocol grid view
            (unless properties=dict(show_in_gui=False) is used)
        -the values of these fields will be stored persistently for each step
    '''
    StepFields = Form.of(
        Integer.named('trail_length')
        .using(default=1, optional=True,
               validators=[ValueAtLeast(minimum=1)]),
        Integer.named('route_repeats')
        .using(default=1, optional=True, validators=[ValueAtLeast(minimum=1)]),
        Integer.named('repeat_duration_s').using(default=0, optional=True),
        Integer.named('transition_duration_ms')
        .using(optional=True, default=750,
               validators=[ValueAtLeast(minimum=0)]))

    def __init__(self, *args, **kwargs):
        self.name = self.plugin_name
        self.step_start_time = None
        self.route_controller = None
        self.should_exit = False
        self.step_number = None
        pmh.BaseMqttReactor.__init__(self)
        self._props = {
            "routes": None,
            "transition_duration_ms": None,
            "repeat_duration_s": None,
            "trail_length": None,
            "route_repeats": None
            }
        self.start()

    @property
    def repeat_duration_s(self):
        return self._props["repeat_duration_s"]

    @repeat_duration_s.setter
    def repeat_duration_s(self, value):
        self._props["repeat_duration_s"] = value

    @property
    def routes(self):
        if self._props["routes"] is None:
            self._props["routes"] = RouteController.default_routes()
        return self._props["routes"]

    @routes.setter
    def routes(self, value):
        self._routes = value
        msg = json.dumps(value, cls=PandasJsonEncoder)
        self.mqtt_client.publish("microdrop/put/routes-model/routes",
                                 msg, retain=False)

    @property
    def _routes(self):
        return self._props["routes"]

    @_routes.setter
    def _routes(self, value):
        if value is None:
            self.routes = RouteController.default_routes()
        else:
            self._props["routes"] = value

    @property
    def route_repeats(self):
        return self._props["route_repeats"]

    @route_repeats.setter
    def route_repeats(self, value):
        self._props["route_repeats"] = value

    @property
    def trail_length(self):
        return self._props["trail_length"]

    @trail_length.setter
    def trail_length(self, value):
        self._props["trail_length"] = value

    @property
    def transition_duration_ms(self):
        return self._props["transition_duration_ms"]

    @transition_duration_ms.setter
    def transition_duration_ms(self, value):
        self._props["transition_duration_ms"] = value

    def start(self):
        # Connect to MQTT broker.
        self._connect()
        # Start loop in background thread.
        signal.signal(signal.SIGINT, self.exit)
        self.mqtt_client.loop_forever()

    def on_disconnect(self, *args, **kwargs):
        # Startup Mqtt Loop after disconnected (unless should terminate)
        if self.should_exit:
            sys.exit()
        self._connect()
        self.mqtt_client.loop_forever()

    def exit(self, a=None, b=None):
        self.mqtt_client.publish('microdrop/droplet-planning-plugin/signal/'
                                 'plugin-exited', json.dumps(self.plugin_path),
                                 retain=False)
        self.should_exit = True
        self.mqtt_client.disconnect()

    def onFindExecutablePluginsCalled(self, payload, args):
        """Implement method for plugin to respond to protocol execution"""
        # this.addBinding(`${this.base}/${this.name}/signal/${topic}`, event);
        self.mqtt_client.publish("microdrop/droplet-planning-plugin/signal/"
                                 "executable-plugin-found", json.dumps(None))

    def onRunStep(self, payload, args):
        """Execute Step"""
        data = payload['step']
        # XXX: Depricating previous method to label schema
        if self.url_safe_plugin_name in data:
            step = data[self.url_safe_plugin_name]
        elif self.url_safe_plugin_name.replace("_", "-") in data:
            step = data[self.url_safe_plugin_name.replace("_", "-")]
        else:
            return
        self.execute_routes(step)

    def onAddRoute(self, payload, args):
        """ Called when add route message received """
        self.add_route(payload)

    def onExit(self, payload, args):
        """ Called when other plugins request termination of this plugin """
        self.exit()

    def onPutTransitionDurationMS(self, payload, args):
        self.transition_duration_ms = payload["transitionDurationMilliseconds"]

    def onPutTrailLength(self, payload, args):
        self.trail_length = payload["trailLength"]

    def onPutRoutes(self, payload, args):
        self._routes = payload["dropRoutes"]

    def onPutRouteRepeats(self, payload, args):
        self.route_repeats = payload["routeRepeats"]

    def onPutRepeatDurationSeconds(self, payload, args):
        self.repeat_duration_s = payload["repeatDurationSeconds"]

    def onExecuteRoutes(self, payload, args):
        self.execute_routes(payload)

    def onClearRoutes(self, payload, args):
        if payload:
            self.clear_routes(electrode_id=payload['electrode_id'])
        else:
            self.clear_routes()

    def listen(self):
        self.addGetRoute("microdrop/dmf-device-ui/clear-routes",
                         self.onClearRoutes)
        self.addGetRoute("microdrop/dmf-device-ui/execute-routes",
                         self.onExecuteRoutes)
        self.addGetRoute("microdrop/droplet-planning-plugin/exit", self.onExit)
        self.addGetRoute("microdrop/{pluginName}/add-route", self.onAddRoute)
        self.addGetRoute("microdrop/{pluginName}/signal/"
                         "find-executable-plugins",
                         self.onFindExecutablePluginsCalled)
        self.addGetRoute("microdrop/{pluginName}/signal/"
                         "run-step", self.onRunStep)
        self.onPutMsg("routes", self.onPutRoutes)
        self.onPutMsg("route-repeats", self.onPutRouteRepeats)
        self.onPutMsg("repeat-duration-s", self.onPutRepeatDurationSeconds)
        self.onPutMsg("trail-length", self.onPutTrailLength)
        self.onPutMsg("transition-duration-ms", self.onPutTransitionDurationMS)
        self.subscribe()

    def on_connect(self, client, userdata, flags, rc):
        self.listen()
        # Notify the broker that the plugin has started:
        self.mqtt_client.publish("microdrop/droplet-planning-plugin/signal/"
                                 "plugin-started",
                                 json.dumps(self.plugin_path), retain=False)

        # Publish the schema definition:
        form = flatlandToDict(self.StepFields)
        self.mqtt_client.publish('microdrop/droplet-planning-plugin/signal/'
                                 'update-schema', json.dumps(form),
                                 retain=True)

    def on_plugin_enable(self):
        self.route_controller = RouteController(self)
        form = flatlandToDict(self.StepFields)
        self.mqtt_client.publish('microdrop/droplet-planning-plugin/signal/'
                                 'update-schema', json.dumps(form),
                                 retain=True)

        defaults = {}
        for k, v in form.iteritems():
            defaults[k] = v['default']
        self.mqtt_client.publish('microdrop/droplet-planning-plugin/'
                                 'step-options',
                                 json.dumps([defaults],
                                            cls=PandasJsonEncoder),
                                 retain=True)

    def on_plugin_disable(self):
        """
        Handler called once the plugin instance is disabled.
        """
        pass

    def on_app_exit(self):
        """
        Handler called just before the Microdrop application exits.
        """
        pass

    ###########################################################################
    # Step event handler methods
    def on_error(self, *args):
        logger.error('Error executing routes.', exc_info=True)
        # An error occurred while initializing Analyst remote control.
        msg = {"plugin_name": self.name, "return_value": 'Fail'}
        self.mqtt_client.publish('microdrop/droplet-planning-plugin/'
                                 'signal/step-complete', json.dumps(msg))
        # self.mqtt_client.publish('microdrop/droplet-planning-plugin/'
        #                          'step-complete',
        #                          json.dumps(msg))

    def on_protocol_pause(self):
        self.kill_running_step()

    def kill_running_step(self):
        # Stop execution of any routes that are currently running.
        if self.route_controller is not None:
            self.route_controller.reset()

    def on_step_run(self):
        """
        Handler called whenever a step is executed. Note that this signal
        is only emitted in realtime mode or if a protocol is running.

        Plugins that handle this signal must emit the on_step_complete
        signal once they have completed the step. The protocol controller
        will wait until all plugins have completed the current step before
        proceeding.

        return_value can be one of:
            None
            'Repeat' - repeat the step
            or 'Fail' - unrecoverable error (stop the protocol)
        """
        self.kill_running_step()

        try:
            self.repeat_i = 0
            self.step_start_time = datetime.now()
            df_routes = self.routes
            self.route_controller.execute_routes(
                df_routes, self.transition_duration_ms,
                trail_length=self.trail_length,
                on_complete=self.on_step_routes_complete,
                on_error=self.on_error)
        except:
            self.on_error()

    def on_step_routes_complete(self, start_time, electrode_ids):
        '''
        Callback function executed when all concurrent routes for a step have
        completed a single run.

        If repeats are requested, either through repeat counts or a repeat
        duration, *cycle* routes (i.e., routes that terminate at the start
        electrode) will repeat as necessary.
        '''
        self.mqtt_client.publish('microdrop/droplet-planning-plugin/'
                                 'signal/step-complete', json.dumps(None))
        # self.mqtt_client.publish('microdrop/droplet-planning-plugin/'
        #                          'step-complete',
        #                          json.dumps(None))

        step_duration_s = (datetime.now() -
                           self.step_start_time).total_seconds()
        if ((self.repeat_duration_s > 0 and step_duration_s <
             self.repeat_duration_s) or (
                 self.repeat_i + 1 < self.route_repeats)):
            # Either repeat duration has not been met, or the specified number
            # of repetitions has not been met.  Execute another iteration of
            # the routes.
            self.repeat_i += 1
            df_routes = self.routes
            self.route_controller.execute_routes(
                df_routes, self.transition_duration_ms,
                trail_length=self.trail_length,
                cyclic=True, acyclic=False,
                on_complete=self.on_step_routes_complete,
                on_error=self.on_error)
        else:
            logger.info('Completed routes (%s repeats in %ss)', self.repeat_i +
                        1, si_format(step_duration_s))
            # Transitions along all droplet routes have been processed.
            # Signal step has completed and reset plugin step state.
            # msg = {"plugin_name": self.name, "return_value": None}
            # self.mqtt_client.publish('microdrop/droplet-planning-plugin/'
            # 'step-complete', json.dumps(msg))

    def on_step_options_swapped(self, plugin, old_step_number, step_number):
        """
        Handler called when the step options are changed for a particular
        plugin.  This will, for example, allow for GUI elements to be
        updated based on step specified.

        Parameters:
            plugin : plugin instance for which the step options changed
            step_number : step number that the options changed for
        """
        logger.info('[on_step_swapped] old step=%s, step=%s', old_step_number,
                    step_number)
        self.kill_running_step()

    def on_step_swapped(self, old_step_number, step_number):
        """
        Handler called when the current step is swapped.
        """
        logger.info('[on_step_swapped] old step=%s, step=%s', old_step_number,
                    step_number)
        self.kill_running_step()

    ###########################################################################
    # Step options dependent methods

    def add_route(self, electrode_ids):
        '''
        Add droplet route.

        Args:

            electrode_ids (list) : Ordered list of identifiers of electrodes on
                route.
        '''
        drop_routes = self.routes

        route_i = (drop_routes.route_i.max() + 1
                   if drop_routes.shape[0] > 0 else 0)
        drop_route = (pd.DataFrame(electrode_ids, columns=['electrode_i'])
                      .reset_index().rename(columns={'index': 'transition_i'}))
        drop_route.insert(0, 'route_i', route_i)
        drop_routes = drop_routes.append(drop_route, ignore_index=True)
        self.routes = drop_routes
        return {'route_i': route_i, 'drop_routes': drop_routes}

    def clear_routes(self, electrode_id=None, step_number=None):
        '''
        Clear all drop routes for protocol step that include the specified
        electrode (identified by string identifier).
        '''
        if electrode_id is None:
            # No electrode identifier specified.  Clear all step routes.
            df_routes = RouteController.default_routes()
        else:
            df_routes = self.routes
            # Find indexes of all routes that include electrode.
            routes_to_clear = df_routes.loc[df_routes.electrode_i ==
                                            electrode_id, 'route_i']
            # Remove all routes that include electrode.
            df_routes = df_routes.loc[~df_routes.route_i
                                      .isin(routes_to_clear.tolist())].copy()
        self.routes = df_routes

    def execute_routes(self, data):
        # TODO allow for passing of both electrode_id and route_i
        # Currently electrode_id only

        try:
            df_routes = self.routes

            if 'transition_duration_ms' in data:
                transition_duration_ms = data['transition_duration_ms']
            else:
                transition_duration_ms = self.transition_duration_ms

            if 'trail_length' in data:
                trail_length = data['trail_length']
            else:
                trail_length = self.trail_length

            if 'route_i' in data:
                df_routes = df_routes.loc[df_routes.route_i == data['route_i']]
            elif 'electrode_i' in data:
                if data['electrode_i'] is not None:
                    routes_to_execute = df_routes.loc[df_routes.electrode_i ==
                                                      data['electrode_i'],
                                                      'route_i']
                    df_routes = df_routes.loc[df_routes.route_i
                                              .isin(routes_to_execute
                                                    .tolist())].copy()

            route_controller = RouteController(self)
            route_controller.execute_routes(
                df_routes, transition_duration_ms,
                on_complete=self.on_step_routes_complete,
                trail_length=trail_length)
        except:
            logger.error(str(data), exc_info=True)


dpp = DropletPlanningPlugin()

# REVIEW: Removing get version (maybe bad idea?)
# from ._version import get_versions
# __version__ = get_versions()['version']
# del get_versions
