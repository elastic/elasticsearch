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
import org.elasticsearch.common.ParseField;
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

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetStoredScriptAction extends BaseRestHandler {

    public static final ParseField _ID_PARSE_FIELD = new ParseField("_id");

    public static final ParseField FOUND_PARSE_FIELD = new ParseField("found");

    public RestGetStoredScriptAction(Settings settings, RestController controller) {
        super(settings);

        // Note {lang} is actually {id} in the first handler.  It appears
        // parameters as part of the path must be of the same ordering relative
        // to name or they will not work as expected.
        controller.registerHandler(GET, "/_scripts/{lang}", this);
        controller.registerHandler(GET, "/_scripts/{lang}/{id}", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        String id;
        String lang;

        // In the case where only {lang} is not null, we make it {id} because of
        // name ordering issues in the handlers' paths.
        if (request.param("id") == null) {
            id = request.param("lang");;
            lang = null;
        } else {
            id = request.param("id");
            lang = request.param("lang");
        }

        if (lang != null) {
            deprecationLogger.deprecated(
                "specifying lang [" + lang + "] as part of the url path is deprecated");
        }

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest(id, lang);

        return channel -> client.admin().cluster().getStoredScript(getRequest, new RestBuilderListener<GetStoredScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetStoredScriptResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(_ID_PARSE_FIELD.getPreferredName(), id);

                if (lang != null) {
                    builder.field(StoredScriptSource.LANG_PARSE_FIELD.getPreferredName(), lang);
                }

                StoredScriptSource source = response.getSource();
                boolean found = source != null;
                builder.field(FOUND_PARSE_FIELD.getPreferredName(), found);

                if (found) {
                    if (lang == null) {
                        builder.startObject(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
                        builder.field(StoredScriptSource.LANG_PARSE_FIELD.getPreferredName(), source.getLang());
                        builder.field(StoredScriptSource.CODE_PARSE_FIELD.getPreferredName(), source.getCode());

                        if (source.getOptions().isEmpty() == false) {
                            builder.field(StoredScriptSource.OPTIONS_PARSE_FIELD.getPreferredName(), source.getOptions());
                        }

                        builder.endObject();
                    } else {
                        builder.field(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName(), source.getCode());
                    }
                }

                builder.endObject();

                return new BytesRestResponse(found ? RestStatus.OK : RestStatus.NOT_FOUND, builder);
            }
        });
    }
}
