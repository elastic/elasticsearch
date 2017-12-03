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

package org.elasticsearch.script.mustache;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRenderSearchTemplateAction extends BaseRestHandler {
    public RestRenderSearchTemplateAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_render/template", this);
        controller.registerHandler(POST, "/_render/template", this);
        controller.registerHandler(GET, "/_render/template/{id}", this);
        controller.registerHandler(POST, "/_render/template/{id}", this);
    }

    @Override
    public String getName() {
        return "render_search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Creates the render template request
        SearchTemplateRequest renderRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            renderRequest = RestSearchTemplateAction.parse(parser);
        }
        renderRequest.setSimulate(true);

        String id = request.param("id");
        if (id != null) {
            renderRequest.setScriptType(ScriptType.STORED);
            renderRequest.setScript(id);
        }

        return channel -> client.execute(SearchTemplateAction.INSTANCE, renderRequest, new RestToXContentListener<>(channel));
    }
}
