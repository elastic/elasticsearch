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

import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptsRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptsResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetStoredScriptsAction extends BaseRestHandler {

    public RestGetStoredScriptsAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(GET, "/_scripts", this);
    }

    @Override
    public String getName() {
        return "get_all_stored_scripts_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        GetStoredScriptsRequest getRequest = new GetStoredScriptsRequest();


        return channel -> client.admin().cluster().getStoredScripts(getRequest, new
            RestBuilderListener<GetStoredScriptsResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetStoredScriptsResponse response, XContentBuilder builder)
                throws Exception {
                builder.startObject();

                Map<String, StoredScriptSource> storedScripts = response.getStoredScripts();
                for (Map.Entry<String, StoredScriptSource> storedScript : storedScripts.entrySet()) {
                    builder.startObject(storedScript.getKey());

                    StoredScriptSource source = storedScript.getValue();
                    builder.startObject(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
                    builder.field(StoredScriptSource.LANG_PARSE_FIELD.getPreferredName(), source.getLang());
                    builder.field(StoredScriptSource.SOURCE_PARSE_FIELD.getPreferredName(), source.getSource());

                    if (!source.getOptions().isEmpty()) {
                        builder.field(StoredScriptSource.OPTIONS_PARSE_FIELD.getPreferredName(), source.getOptions());
                    }

                    builder.endObject();
                    builder.endObject();
                }

                builder.endObject();

                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
