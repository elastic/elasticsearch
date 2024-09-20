/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestPutIndexTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutIndexTemplateAction.class);
    public static final String DEPRECATION_WARNING = "Legacy index templates are deprecated in favor of composable templates.";
    private static final RestApiVersion DEPRECATION_VERSION = RestApiVersion.V_8;
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]"
        + " Specifying include_type_name in put index template requests is deprecated."
        + " The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, "/_template/{name}").deprecateAndKeep(DEPRECATION_WARNING).build(),
            Route.builder(PUT, "/_template/{name}").deprecateAndKeep(DEPRECATION_WARNING).build()
        );
    }

    @Override
    public String getName() {
        return "put_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest(request.param("name"));
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("template")) {
            deprecationLogger.compatibleCritical(
                "template_parameter_deprecation",
                "Deprecated parameter [template] used, replaced by [index_patterns]"
            );
            putRequest.patterns(List.of(request.param("template")));
        } else {
            putRequest.patterns(asList(request.paramAsStringArray("index_patterns", Strings.EMPTY_ARRAY)));
        }
        putRequest.order(request.paramAsInt("order", putRequest.order()));
        putRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", ""));

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();
        if (request.getRestApiVersion() == RestApiVersion.V_7) {
            if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
                deprecationLogger.compatibleCritical("put_index_template_with_types", TYPES_DEPRECATION_MESSAGE);
            }
            boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
            if (includeTypeName) {
                sourceAsMap = RestCreateIndexAction.prepareMappingsV7(sourceAsMap, request);
            } else {
                sourceAsMap = RestCreateIndexAction.prepareMappings(sourceAsMap);
            }
        } else {
            sourceAsMap = RestCreateIndexAction.prepareMappings(sourceAsMap);
        }
        if (request.getRestApiVersion() == RestApiVersion.V_7 && sourceAsMap.containsKey("template")) {
            deprecationLogger.compatibleCritical(
                "template_field_deprecation",
                "Deprecated field [template] used, replaced by [index_patterns]"
            );
            putRequest.patterns(List.of((String) sourceAsMap.remove("template")));
        }
        putRequest.source(sourceAsMap);

        return channel -> client.admin().indices().putTemplate(putRequest, new RestToXContentListener<>(channel));
    }
}
