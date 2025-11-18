/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestResolveIndexAction extends BaseRestHandler {
    private static final Set<String> CAPABILITIES = Set.of("mode_filter");
    private final Settings settings;

    public RestResolveIndexAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return "resolve_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_resolve/index/{name}"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("name"));
        String modeParam = request.param("mode");
        final boolean crossProjectEnabled = settings != null && settings.getAsBoolean("serverless.cross_project.enabled", false);
        String projectRouting = null;
        if (crossProjectEnabled) {
            projectRouting = request.param("project_routing");
        }
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, ResolveIndexAction.Request.DEFAULT_INDICES_OPTIONS);
        if (crossProjectEnabled) {
            indicesOptions = IndicesOptions.builder(indicesOptions)
                .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
                .build();
        }
        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            indices,
            indicesOptions,
            modeParam == null
                ? null
                : Arrays.stream(modeParam.split(","))
                    .map(IndexMode::fromString)
                    .collect(() -> EnumSet.noneOf(IndexMode.class), EnumSet::add, EnumSet::addAll),
            projectRouting
        );
        return channel -> client.admin().indices().resolveIndex(resolveRequest, new RestToXContentListener<>(channel));
    }
}
