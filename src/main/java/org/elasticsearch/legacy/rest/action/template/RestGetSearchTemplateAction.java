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
package org.elasticsearch.legacy.rest.action.template;

import org.elasticsearch.legacy.ElasticsearchIllegalStateException;
import org.elasticsearch.legacy.action.get.GetRequest;
import org.elasticsearch.legacy.action.get.GetResponse;
import org.elasticsearch.legacy.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.legacy.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.xcontent.*;
import org.elasticsearch.legacy.rest.*;
import org.elasticsearch.legacy.rest.action.support.RestResponseListener;
import org.elasticsearch.legacy.script.ScriptService;
import org.elasticsearch.legacy.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.legacy.rest.RestRequest.Method.GET;
import static org.elasticsearch.legacy.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.legacy.rest.RestStatus.OK;

/**
 *
 */
public class RestGetSearchTemplateAction extends BaseRestHandler {

    @Inject
    public RestGetSearchTemplateAction(Settings settings, Client client,
                                       RestController controller, ScriptService scriptService) {
        super(settings, client);
        controller.registerHandler(GET, "/_search/template/{id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        final GetIndexedScriptRequest getRequest = new GetIndexedScriptRequest(ScriptService.SCRIPT_INDEX, "mustache", request.param("id"));
        RestResponseListener<GetIndexedScriptResponse> responseListener = new RestResponseListener<GetIndexedScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetIndexedScriptResponse response) throws Exception {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                if (!response.isExists()) {
                    return new BytesRestResponse(NOT_FOUND, builder);
                } else {
                    try{
                        String templateString = response.getScript();
                        builder.startObject();
                        builder.field("template",templateString);
                        builder.endObject();
                        return new BytesRestResponse(OK, builder);
                    } catch( IOException|ClassCastException e ){
                        throw new ElasticsearchIllegalStateException("Unable to parse "  + response.getScript() + " as json",e);
                    }
                }
            }
        };
        client.getIndexedScript(getRequest, responseListener);
    }
}
