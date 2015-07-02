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

package org.elasticsearch.rest.action.admin.indices.validate.template;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.validate.template.RenderSearchTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.template.RenderSearchTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestRenderSearchTemplateAction extends BaseRestHandler {

    @Inject
    public RestRenderSearchTemplateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_render/template", this);
        controller.registerHandler(POST, "/_render/template", this);
        controller.registerHandler(GET, "/_render/template/{id}", this);
        controller.registerHandler(POST, "/_render/template/{id}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        RenderSearchTemplateRequest renderSearchTemplateRequest;
        BytesReference source = RestActions.getRestContent(request);
        XContentParser parser = XContentFactory.xContent(source).createParser(source);
        String templateId = request.param("id");
        final Template template;
        if (templateId == null) {
            template = Template.parse(parser, parseFieldMatcher);
        } else {
            Map<String, Object> params = null;
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse request. request body must be an object but found [{}] instead", token);
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (parseFieldMatcher.match(currentFieldName, ScriptField.PARAMS)) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        params = parser.map();
                    } else {
                        throw new ElasticsearchParseException("failed to parse request. field [{}] is expected to be an object, but found [{}] instead", currentFieldName, token);
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse request. unknown field [{}] of type [{}]", currentFieldName, token);
                }
            }
            template = new Template(templateId, ScriptType.INDEXED, MustacheScriptEngineService.NAME, null, params);
        }
        renderSearchTemplateRequest = new RenderSearchTemplateRequest();
        renderSearchTemplateRequest.template(template);
        client.admin().indices().renderSearchTemplate(renderSearchTemplateRequest, new RestBuilderListener<RenderSearchTemplateResponse>(channel) {

            @Override
            public RestResponse buildResponse(RenderSearchTemplateResponse response, XContentBuilder builder) throws Exception {
                builder.prettyPrint();
                response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(OK, builder);
            }});
    }
}
