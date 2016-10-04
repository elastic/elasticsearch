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

package org.elasticsearch.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class BaseRestHandlerTests extends ESTestCase {

    public void testUnconsumedParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler(Settings.EMPTY) {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                request.param("consumed");
                return channel -> executed.set(true);
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("consumed", randomAsciiOfLength(8));
        params.put("unconsumed", randomAsciiOfLength(8));
        RestRequest request = new FakeRestRequest.Builder().withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> handler.handleRequest(request, channel, mock(NodeClient.class)));
        assertThat(e, hasToString(containsString("request [/] contains unused params: [unconsumed]")));
        assertFalse(executed.get());
    }

    public void testUnconsumedResponseParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler(Settings.EMPTY) {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                request.param("consumed");
                return channel -> executed.set(true);
            }

            @Override
            protected Set<String> responseParams() {
                return Collections.singleton("response_param");
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("consumed", randomAsciiOfLength(8));
        params.put("response_param", randomAsciiOfLength(8));
        RestRequest request = new FakeRestRequest.Builder().withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mock(NodeClient.class));
        assertTrue(executed.get());
    }

    public void testDefaultResponseParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler(Settings.EMPTY) {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                return channel -> executed.set(true);
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("format", randomAsciiOfLength(8));
        params.put("filter_path", randomAsciiOfLength(8));
        params.put("pretty", randomAsciiOfLength(8));
        params.put("human", randomAsciiOfLength(8));
        RestRequest request = new FakeRestRequest.Builder().withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mock(NodeClient.class));
        assertTrue(executed.get());
    }

    public void testCatResponseParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        AbstractCatAction handler = new AbstractCatAction(Settings.EMPTY) {
            @Override
            protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
                return channel -> executed.set(true);
            }

            @Override
            protected void documentation(StringBuilder sb) {

            }

            @Override
            protected Table getTableWithHeader(RestRequest request) {
                return null;
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("format", randomAsciiOfLength(8));
        params.put("h", randomAsciiOfLength(8));
        params.put("v", randomAsciiOfLength(8));
        params.put("ts", randomAsciiOfLength(8));
        params.put("pri", randomAsciiOfLength(8));
        params.put("bytes", randomAsciiOfLength(8));
        params.put("size", randomAsciiOfLength(8));
        params.put("time", randomAsciiOfLength(8));
        RestRequest request = new FakeRestRequest.Builder().withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mock(NodeClient.class));
        assertTrue(executed.get());
    }

}
