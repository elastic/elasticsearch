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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import java.util.Collection;

import static java.util.Collections.singleton;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that you can register plugins that add _cat and regular REST endpoints.
 */
@ClusterScope(scope = Scope.SUITE)
public class RestModuleIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(Node.HTTP_ENABLED, true).put(super.nodeSettings(nodeOrdinal)).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singleton(TestPlugin.class);
    }

    public void testRegisterCatHandler() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/_cat/fake").execute();
        assertEquals("dummy CAT", response.getBody());
        response = httpClient().method("GET").path("/_cat").execute();
        assertThat(response.getBody(), containsString("/_cat/fake - test thingy"));
    }

    public void testRegisterRestHandler() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/_fake").execute();
        assertEquals("dummy REST", response.getBody());
    }

    public static class TestPlugin extends Plugin {
        public TestPlugin(Settings settings) {
        }

        public void onModule(RestModule module) {
            module.add(FakeRestAction.class, FakeRestAction::new);
            module.add(FakeRestCatAction.class, FakeRestCatAction::new);
        }

        @Override
        public String name() {
            return "test-plugin";
        }

        @Override
        public String description() {
            return "Tests registering some rest handlers.";
        }
    }

    public static class FakeRestAction extends BaseRestHandler {
        public FakeRestAction(RestGlobalContext context) {
            super(context);
            context.getController().registerHandler(GET, "/_fake", this);
        }

        @Override
        protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
            channel.sendResponse(new BytesRestResponse(OK, "dummy REST"));
        }
    }

    public static class FakeRestCatAction extends AbstractCatAction {
        public FakeRestCatAction(RestGlobalContext context) {
            super(context);
            context.getController().registerHandler(GET, "/_cat/fake", this);
        }

        @Override
        protected void doRequest(RestRequest request, RestChannel channel, Client client) {
            channel.sendResponse(new BytesRestResponse(OK, "dummy CAT"));
        }

        @Override
        protected void documentation(StringBuilder sb) {
            sb.append("/_cat/fake - test thingy\n");
        }

        @Override
        protected Table getTableWithHeader(RestRequest request) {
            return null;
        }
    }
}
