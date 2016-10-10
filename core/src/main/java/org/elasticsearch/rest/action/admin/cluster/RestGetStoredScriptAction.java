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
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.script.Script.StoredScriptSource;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetStoredScriptAction extends BaseRestHandler {
    private static final String ID = "id";
    private static final String FOUND = "found";
    private static final String BINDING = "binding";
    private static final String LANG = "lang";
    private static final String CODE = "code";

    @Inject
    public RestGetStoredScriptAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/_scripts/{id}", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetStoredScriptRequest getRequest = new GetStoredScriptRequest(request.param("id"));

        return channel -> client.admin().cluster().getStoredScript(getRequest, new RestBuilderListener<GetStoredScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetStoredScriptResponse response, XContentBuilder builder) throws Exception {
                StoredScriptSource source = response.getSource();
                boolean found = source != null;

                builder.startObject();
                builder.field(ID, getRequest.id());
                builder.field(FOUND, found);

                RestStatus status = RestStatus.NOT_FOUND;

                if (found) {
                    // builder.field(CONTEXT, source.context); TODO: once context is used start returning this
                    builder.field(LANG, source.lang);
                    builder.field(CODE, source.code);
                    status = RestStatus.OK;
                }

                builder.endObject();

                return new BytesRestResponse(status, builder);
            }
        });
    }
}
