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

        self.parent.trigger("put-electrode-states", data)

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
        self.step_number = None
        pmh.BaseMqttReactor.__init__(self)
        self.routes = None
        self.transition_duration_ms = None
        self.repeat_duration_s = None
        self.trail_length = None
        self.route_repeats = None
        self.start()

    def onFindExecutablePluginsCalled(self, payload, args):
        """Implement method for plugin to respond to protocol execution"""
        self.trigger("executable-plugin-found", None)

    def on_run_step(self, payload, args):
        """Execute Step"""
        data = payload['step']
        # XXX: Depricating previous method to label schema
        if "routes-model" in data:
            step = data["routes-model"]
        else:
            return
        self.execute_routes(step)

    def on_add_route(self, payload, args):
        """ Called when add route message received """
        self.add_route(payload)

    def update_routes(self, new_routes):
        if new_routes is None:
            self.routes = RouteController.default_routes()
            self.trigger("put-routes", {'drop_routes': self.routes})
        else:
            self.routes = new_routes

    def on_update_routes(self, payload, args):
        # XXX: Migrate to using underscore based keys (drop_routes):
        if 'dropRoutes' in payload.keys():
            self.update_routes(payload['dropRoutes'])
        if 'drop_routes' in payload.keys():
            self.update_routes(payload['drop_routes'])

    def on_update_route_options(self, payload, args):
        if 'transition_duration_ms' in payload.keys():
            self.transition_duration_ms = payload['transition_duration_ms']
        if 'repeat_duration_s' in payload.keys():
            self.repeat_duration_s = payload['repeat_duration_s']
        if 'trail_length' in payload.keys():
            self.trail_length = payload['trail_length']
        if 'route_repeats' in payload.keys():
            self.route_repeats = payload['route_repeats']

        # XXX: Migrate to using underscore based keys:
        if 'dropRoutes' in payload.keys():
            self.update_routes(payload['dropRoutes'])
        if 'drop_routes' in payload.keys():
            self.update_routes(payload['drop_routes'])

    def onExecuteRoutes(self, payload, args):
        self.execute_routes(payload)

    def on_clear_routes(self, payload, args):
        if payload:
            self.clear_routes(electrode_id=payload['electrode_id'])
        else:
            self.clear_routes()

    def onRunningStateRequested(self, payload, args):
        self.trigger("send-running-state", self.plugin_path)

    def listen(self):
        self.onTriggerMsg("clear-routes", self.on_clear_routes)
        self.addGetRoute("microdrop/dmf-device-ui/execute-routes",
                         self.onExecuteRoutes)
        self.onTriggerMsg("add-route", self.on_add_route)
        # self.addGetRoute("microdrop/{pluginName}/add-route",self.on_add_route)

        self.onSignalMsg("{pluginName}", "find-executable-plugins",
                         self.onFindExecutablePluginsCalled)
        self.onSignalMsg("{pluginName}", "run-step", self.on_run_step)

        self.onStateMsg("routes-model", "routes", self.on_update_routes)
        self.onStateMsg("routes-model", "route-options",
                        self.on_update_route_options)

        self.bindTriggerMsg("routes-model", "update-schema", "update-schema")
        self.bindSignalMsg("step-complete", "step-complete")
        self.bindSignalMsg("executable-plugin-found",
                           "executable-plugin-found")

        self.bindPutMsg("electrodes-model", "electrode-states",
                        "put-electrode-states")
        self.bindPutMsg("routes-model", "routes", "put-routes")

        # TODO: Create MicrodropPlugin base class (that inherits from paho)
        self.onSignalMsg("web-server", "running-state-requested",
                         self.onRunningStateRequested)
        self.bindSignalMsg("running", "send-running-state")

        self.subscribe()

        # Publish the schema definition:
        form = flatlandToDict(self.StepFields)
        self.trigger("update-schema", form)

    def on_error(self, *args):
        logger.error('Error executing routes.', exc_info=True)
        # An error occurred while initializing Analyst remote control.
        msg = {"plugin_name": self.name, "return_value": 'Fail'}
        self.trigger("step-complete", msg)

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
        self.trigger("step-complete", None)

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

        self.trigger("put-routes", {'drop_routes': self.routes})
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
        self.trigger("put-routes", {'drop_routes': self.routes})

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
