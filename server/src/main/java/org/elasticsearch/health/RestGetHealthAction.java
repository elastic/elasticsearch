/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetHealthAction extends BaseRestHandler {

    private static final String[] PATH_PREFIX_PARTS = new String[] { "_internal", "_health" };

    private final HealthService healthService;

    public RestGetHealthAction(HealthService healthService) {
        this.healthService = healthService;
    }

    @Override
    public String getName() {
        // TODO: Existing - "cluster_health_action", "cat_health_action"
        return "health_action";
    }

    @Override
    public List<Route> routes() {
        List<Route> routes = new ArrayList<>();
        String pathPrefix = getPathPrefix();
        Map<String, List<String>> componentsToIndicators = healthService.getComponentToIndicatorMapping();
        List<Route> componentRoutes = componentsToIndicators.keySet()
            .stream()
            .map(componentName -> new Route(GET, pathPrefix + "/" + componentName))
            .collect(toList());
        List<Route> indicatorRoutes = componentsToIndicators.entrySet()
            .stream()
            .flatMap(
                entry -> entry.getValue().stream().map(indicator -> new Route(GET, pathPrefix + "/" + entry.getKey() + "/" + indicator))
            )
            .collect(toList());
        routes.add(new Route(GET, pathPrefix));
        routes.addAll(componentRoutes);
        routes.addAll(indicatorRoutes);
        return routes;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String componentName = getComponentNameFromPath(request);
        String indicatorName = getIndicatorNameFromPath(request);
        GetHealthAction.Request getHealthRequest = componentName == null
            ? new GetHealthAction.Request(false)
            : new GetHealthAction.Request(componentName, indicatorName);
        return channel -> client.execute(GetHealthAction.INSTANCE, getHealthRequest, new RestToXContentListener<>(channel));
    }

    private String getPathPrefix() {
        return "/" + Arrays.stream(PATH_PREFIX_PARTS).collect(Collectors.joining("/"));
    }

    private String getComponentNameFromPath(RestRequest request) {
        String lastTokenOfPathPrefix = PATH_PREFIX_PARTS[PATH_PREFIX_PARTS.length - 1];
        String path = request.path();
        String[] pathParts = path.split("/+");
        if (pathParts.length < PATH_PREFIX_PARTS.length + 1) {
            return null;
        }
        if (pathParts[pathParts.length - 1].equals(lastTokenOfPathPrefix)) {
            return null;
        } else if (lastTokenOfPathPrefix.equals(pathParts[pathParts.length - 2])) {
            return pathParts[pathParts.length - 1];
        } else if (lastTokenOfPathPrefix.equals(pathParts[pathParts.length - 3])) {
            return pathParts[pathParts.length - 2];
        }
        return null;
    }

    private String getIndicatorNameFromPath(RestRequest request) {
        String lastTokenOfPathPrefix = PATH_PREFIX_PARTS[PATH_PREFIX_PARTS.length - 1];
        String path = request.path();
        String[] pathParts = path.split("/+");
        if (pathParts.length < PATH_PREFIX_PARTS.length + 2) {
            return null;
        }
        if (lastTokenOfPathPrefix.equals(pathParts[pathParts.length - 1])) {
            return null;
        } else if (lastTokenOfPathPrefix.equals(pathParts[pathParts.length - 2])) {
            return null;
        } else if (lastTokenOfPathPrefix.equals(pathParts[pathParts.length - 3])) {
            return pathParts[pathParts.length - 1];
        }
        return null;
    }
}
