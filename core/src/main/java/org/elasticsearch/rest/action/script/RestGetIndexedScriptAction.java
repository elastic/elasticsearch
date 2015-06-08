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
package org.elasticsearch.rest.action.script;

import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 *
 */
public class RestGetIndexedScriptAction extends BaseRestHandler {

    @Inject
    public RestGetIndexedScriptAction(Settings settings, RestController controller, Client client) {
        this(settings, controller, true, client);
    }

    protected RestGetIndexedScriptAction(Settings settings, RestController controller, boolean registerDefaultHandlers, Client client) {
        super(settings, controller, client);
        if (registerDefaultHandlers) {
            controller.registerHandler(GET, "/_scripts/{lang}/{id}", this);
        }
    }

    protected XContentBuilderString getScriptFieldName() {
        return Fields.SCRIPT;
    }

    protected String getScriptLang(RestRequest request) {
        return request.param("lang");
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        final GetIndexedScriptRequest getRequest = new GetIndexedScriptRequest(getScriptLang(request), request.param("id"));
        getRequest.version(request.paramAsLong("version", getRequest.version()));
        getRequest.versionType(VersionType.fromString(request.param("version_type"), getRequest.versionType()));
        client.getIndexedScript(getRequest, new RestBuilderListener<GetIndexedScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetIndexedScriptResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields.LANG, response.getScriptLang());
                builder.field(Fields._ID, response.getId());
                builder.field(Fields.FOUND, response.isExists());
                RestStatus status = RestStatus.NOT_FOUND;
                if (response.isExists()) {
                    builder.field(Fields._VERSION, response.getVersion());
                    builder.field(getScriptFieldName(), response.getScript());
                    status = RestStatus.OK;
                }
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }

    private static final class Fields {
        private static final XContentBuilderString SCRIPT = new XContentBuilderString("script");
        private static final XContentBuilderString LANG = new XContentBuilderString("lang");
        private static final XContentBuilderString _ID = new XContentBuilderString("_id");
        private static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        private static final XContentBuilderString FOUND = new XContentBuilderString("found");
    }
}
