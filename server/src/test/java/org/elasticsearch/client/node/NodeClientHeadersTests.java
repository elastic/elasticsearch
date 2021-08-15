/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.node;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.AbstractClientHeadersTestCase;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class NodeClientHeadersTests extends AbstractClientHeadersTestCase {

    private static final ActionFilters EMPTY_FILTERS = new ActionFilters(Collections.emptySet());

    @Override
    protected Client buildClient(Settings headersSettings, ActionType<?>[] testedActions) {
        Settings settings = HEADER_SETTINGS;
        Actions actions = new Actions(settings, threadPool, testedActions);
        NodeClient client = new NodeClient(settings, threadPool);
        client.initialize(actions, () -> "test", null,
            new NamedWriteableRegistry(Collections.emptyList()));
        return client;
    }

    private static class Actions extends HashMap<
        ActionType<? extends ActionResponse>,
        TransportAction<? extends ActionRequest, ? extends ActionResponse>> {

        private Actions(Settings settings, ThreadPool threadPool, ActionType<?>[] actions) {
            for (ActionType<?> action : actions) {
                put(action, new InternalTransportAction(settings, action.name(), threadPool));
            }
        }
    }

    private static class InternalTransportAction extends TransportAction<ActionRequest, ActionResponse> {

        private InternalTransportAction(Settings settings, String actionName, ThreadPool threadPool) {
            super(actionName, EMPTY_FILTERS, mock(Transport.Connection.class),
                new TaskManager(settings, threadPool, Collections.emptySet()));
        }

        @Override
        protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
            listener.onFailure(new InternalException(actionName));
        }
    }


}
