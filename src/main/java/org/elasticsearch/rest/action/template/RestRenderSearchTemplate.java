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
package org.elasticsearch.rest.action.template;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.rest.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

public class RestRenderSearchTemplate extends BaseRestHandler {

    private ScriptService scriptService;

    @Inject
    protected RestRenderSearchTemplate(Settings settings, RestController controller,
                                       Client client, ScriptService scriptService) {
        super(settings, controller, client);
        this.scriptService = scriptService;
        controller.registerHandler(GET, "/_render/template", this);
        controller.registerHandler(POST, "/_render/template", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        try {
            TemplateQueryParser.TemplateContext templateContext = SearchService.getTemplateContext(request.content());
            ExecutableScript executable = this.scriptService.executable("mustache", templateContext.template(), templateContext.scriptType(), templateContext.params(), false);
            BytesReference processedQuery = (BytesReference) executable.run();
            builder.startObject();
            builder.field("template");
            try {
                builder.value(XContentHelper.convertToMap(processedQuery, false).v2());
            } catch (ElasticsearchParseException epe) {
                //This probably means the template didn't generate valid json
                builder.value(processedQuery.toUtf8());
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", e.getMessage()).endObject()));
            logger.error("Failed to parse [{}]",e,request.content().toUtf8());
        }
    }

}
