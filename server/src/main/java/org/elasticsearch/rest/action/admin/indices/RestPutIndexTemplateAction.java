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

package org.elasticsearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class RestPutIndexTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
            LogManager.getLogger(RestPutIndexTemplateAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
            " Specifying include_type_name in put index template requests is deprecated."+
            " The parameter will be removed in the next major version.";

    public RestPutIndexTemplateAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.PUT, "/_template/{name}", this);
        controller.registerHandler(RestRequest.Method.POST, "/_template/{name}", this);
    }

    @Override
    public String getName() {
        return "put_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest(request.param("name"));
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecatedAndMaybeLog("put_index_template_with_types", TYPES_DEPRECATION_MESSAGE);
        }
        if (request.hasParam("template")) {
            deprecationLogger.deprecated("Deprecated parameter[template] used, replaced by [index_patterns]");
            putRequest.patterns(Collections.singletonList(request.param("template")));
        } else {
            putRequest.patterns(Arrays.asList(request.paramAsStringArray("index_patterns", Strings.EMPTY_ARRAY)));
        }
        putRequest.order(request.paramAsInt("order", putRequest.order()));
        putRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putRequest.masterNodeTimeout()));
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", ""));

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false,
            request.getXContentType()).v2();
        sourceAsMap = RestCreateIndexAction.prepareMappings(sourceAsMap, includeTypeName);
        putRequest.source(sourceAsMap);

        return channel -> client.admin().indices().putTemplate(putRequest, new RestToXContentListener<>(channel));
    }
}
