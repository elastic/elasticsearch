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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;
import static org.elasticsearch.rest.action.admin.indices.RestCreateIndexAction.prepareMappings;

public class RestPutIndexTemplateAction extends BaseRestHandler {

    public static final String DEPRECATION_WARNING = "Legacy index templates are deprecated in favor of composable templates.";

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
        putRequest.patterns(asList(request.paramAsStringArray("index_patterns", Strings.EMPTY_ARRAY)));
        putRequest.order(request.paramAsInt("order", putRequest.order()));
        putRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", ""));
        putRequest.source(prepareMappings(XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2()));

        return channel -> client.admin().indices().putTemplate(putRequest, new RestToXContentListener<>(channel));
    }
}
