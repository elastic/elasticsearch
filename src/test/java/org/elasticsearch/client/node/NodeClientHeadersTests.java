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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.AbstractClientHeadersTests;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;

/**
 *
 */
public class NodeClientHeadersTests extends AbstractClientHeadersTests {

    private static final ActionFilters EMPTY_FILTERS = new ActionFilters(ImmutableSet.of());

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new ThreadPool("test");
    }

    @After
    public void cleanup() throws InterruptedException {
        terminate(threadPool);
    }

    @Override
    protected Client buildClient(Settings headersSettings, GenericAction[] testedActions) {
        Settings settings = HEADER_SETTINGS;

        Headers headers = new Headers(settings);
        Actions actions = new Actions(settings, threadPool, testedActions);

        NodeClusterAdminClient clusterClient = new NodeClusterAdminClient(threadPool, actions, headers);
        NodeIndicesAdminClient indicesClient = new NodeIndicesAdminClient(threadPool, actions, headers);
        NodeAdminClient adminClient = new NodeAdminClient(settings, clusterClient, indicesClient);
        return new NodeClient(settings, threadPool, adminClient, actions, headers);
    }

    private static class Actions extends HashMap<GenericAction, TransportAction> {

        private Actions(Settings settings, ThreadPool threadPool, GenericAction[] actions) {
            for (GenericAction action : actions) {
                put(action, new InternalTransportAction(settings, action.name(), threadPool));
            }
        }
    }

    private static class InternalTransportAction extends TransportAction {

        private InternalTransportAction(Settings settings, String actionName, ThreadPool threadPool) {
            super(settings, actionName, threadPool, EMPTY_FILTERS);
        }

        @Override
        protected void doExecute(ActionRequest request, ActionListener listener) {
            listener.onFailure(new InternalException(actionName, request));
        }
    }


}
