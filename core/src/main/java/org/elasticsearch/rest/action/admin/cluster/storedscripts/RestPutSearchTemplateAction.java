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
package org.elasticsearch.rest.action.admin.cluster.storedscripts;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.Template;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 *
 */
public class RestPutSearchTemplateAction extends RestPutStoredScriptAction {

    @Inject
    public RestPutSearchTemplateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, false, client);

        //controller.registerHandler(GET, "/template", this);
        controller.registerHandler(POST, "/_search/template/{id}", this);
        controller.registerHandler(PUT, "/_search/template/{id}", this);

        controller.registerHandler(PUT, "/_search/template/{id}/_create", new CreateHandler(settings, controller, client));
        controller.registerHandler(POST, "/_search/template/{id}/_create", new CreateHandler(settings, controller, client));
    }

    final class CreateHandler extends BaseRestHandler {
        protected CreateHandler(Settings settings, RestController controller, Client client) {
            super(settings, client);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, final Client client) {
            request.params().put("op_type", "create");
            RestPutSearchTemplateAction.this.handleRequest(request, channel, client);
        }
    }

    @Override
    protected String getScriptLang(RestRequest request) {
        return Template.DEFAULT_LANG;
    }
}
