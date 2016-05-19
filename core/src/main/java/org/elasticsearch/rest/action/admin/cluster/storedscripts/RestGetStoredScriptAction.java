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

import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetStoredScriptAction extends BaseRestHandler {

    @Inject
    public RestGetStoredScriptAction(Settings settings, RestController controller, Client client) {
        this(settings, controller, true, client);
    }

    protected RestGetStoredScriptAction(Settings settings, RestController controller, boolean registerDefaultHandlers, Client client) {
        super(settings, client);
        if (registerDefaultHandlers) {
            controller.registerHandler(GET, "/_scripts/{lang}/{id}", this);
        }
    }

    protected String getScriptFieldName() {
        return Fields.SCRIPT;
    }

    protected String getScriptLang(RestRequest request) {
        return request.param("lang");
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        final GetStoredScriptRequest getRequest = new GetStoredScriptRequest(getScriptLang(request), request.param("id"));
        client.admin().cluster().getStoredScript(getRequest, new RestBuilderListener<GetStoredScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetStoredScriptResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields.LANG, getRequest.lang());
                builder.field(Fields._ID, getRequest.id());
                boolean found = response.getStoredScript() != null;
                builder.field(Fields.FOUND, found);
                RestStatus status = RestStatus.NOT_FOUND;
                if (found) {
                    builder.field(getScriptFieldName(), response.getStoredScript());
                    status = RestStatus.OK;
                }
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }

    private static final class Fields {
        private static final String SCRIPT = "script";
        private static final String LANG = "lang";
        private static final String _ID = "_id";
        private static final String FOUND = "found";
    }
}
