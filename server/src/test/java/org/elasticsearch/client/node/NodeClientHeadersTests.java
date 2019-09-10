/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.AbstractClientHeadersTestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.Collections;
import java.util.HashMap;

public class NodeClientHeadersTests extends AbstractClientHeadersTestCase {

    private static final ActionFilters EMPTY_FILTERS = new ActionFilters(Collections.emptySet());

    @Override
    protected Client buildClient(Settings headersSettings, ActionType[] testedActions) {
        Settings settings = HEADER_SETTINGS;
        TaskManager taskManager = new TaskManager(settings, threadPool, Collections.emptySet());
        Actions actions = new Actions(testedActions, taskManager);
        NodeClient client = new NodeClient(settings, threadPool);
        client.initialize(actions, taskManager, () -> "test", null);
        return client;
    }

    private static class Actions extends HashMap<ActionType, TransportAction> {

        private Actions(ActionType[] actions, TaskManager taskManager) {
            for (ActionType action : actions) {
                put(action, new InternalTransportAction(action.name(), taskManager));
            }
        }
    }

    private static class InternalTransportAction extends TransportAction {

        private InternalTransportAction(String actionName, TaskManager taskManager) {
            super(actionName, EMPTY_FILTERS, taskManager);
        }

        @Override
        protected void doExecute(Task task, ActionRequest request, ActionListener listener) {
            listener.onFailure(new InternalException(actionName));
        }
    }


}
