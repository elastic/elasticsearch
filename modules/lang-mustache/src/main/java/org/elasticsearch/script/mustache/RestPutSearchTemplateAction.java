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

import java.io.IOException;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.TemplateScript;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutSearchTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(RestPutSearchTemplateAction.class));

    public RestPutSearchTemplateAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerAsDeprecatedHandler(POST, "/_search/template/{id}", this,
            "The stored search template API is deprecated. Use stored scripts instead.", DEPRECATION_LOGGER);
        controller.registerAsDeprecatedHandler(PUT, "/_search/template/{id}", this,
            "The stored search template API is deprecated. Use stored scripts instead.", DEPRECATION_LOGGER);
    }

    @Override
    public String getName() {
        return "put_search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        BytesReference content = request.requiredContent();

        PutStoredScriptRequest put = new PutStoredScriptRequest(id, Script.DEFAULT_TEMPLATE_LANG, TemplateScript.CONTEXT.name,
            content, request.getXContentType());
        return channel -> client.admin().cluster().putStoredScript(put, new AcknowledgedRestListener<>(channel));
    }
}
