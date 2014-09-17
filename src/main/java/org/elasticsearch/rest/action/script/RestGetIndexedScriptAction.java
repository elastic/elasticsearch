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

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

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

    protected String getScriptLang(RestRequest request) {
        return request.param("lang");
    }

    protected String getScriptFieldName() {
        return "script";
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        final GetIndexedScriptRequest getRequest = new GetIndexedScriptRequest(getScriptLang(request), request.param("id"));
        getRequest.version(request.paramAsLong("version", getRequest.version()));
        getRequest.versionType(VersionType.fromString(request.param("version_type"), getRequest.versionType()));
        client.getIndexedScript(getRequest, new RestResponseListener<GetIndexedScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetIndexedScriptResponse response) throws Exception {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                if (!response.isExists()) {
                    return new BytesRestResponse(NOT_FOUND, builder);
                } else {
                    try{
                        String script = response.getScript();
                        builder.startObject();
                        builder.field(getScriptFieldName(), script);
                        builder.endObject();
                        return new BytesRestResponse(OK, builder);
                    } catch( IOException|ClassCastException e ){
                        throw new ElasticsearchIllegalStateException("Unable to parse "  + response.getScript() + " as json",e);
                    }
                }
            }
        });
    }
}
