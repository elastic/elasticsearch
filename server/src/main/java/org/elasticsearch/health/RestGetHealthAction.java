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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetHealthAction extends BaseRestHandler {

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
        routes.add(new Route(GET, "/_internal/_health"));
        Map<String, List<String>> componentsToIndicators = healthService.getComponentToIndicatorMapping();
        List<Route> componentRoutes = componentsToIndicators.keySet()
            .stream()
            .map(componentName -> new Route(GET, "/_internal/_health/" + componentName))
            .collect(Collectors.toList());
        routes.addAll(componentRoutes);
        for (Map.Entry<String, List<String>> entry : componentsToIndicators.entrySet()) {
            for (String indicator : entry.getValue()) {
                routes.add(new Route(GET, "/_internal/_health/" + entry.getKey() + "/" + indicator));
            }
        }
        return routes;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String componentName = getComponentNameFromPath(request);
        String indicatorName = getIndicatorNameFromPath(request);
        GetHealthAction.Request getHealthRequest = new GetHealthAction.Request(componentName, indicatorName);
        return channel -> client.execute(GetHealthAction.INSTANCE, getHealthRequest, new RestToXContentListener<>(channel));
    }

    private String getComponentNameFromPath(RestRequest request) {
        Path rawPath = Paths.get(request.rawPath());
        if (rawPath.getFileName().toString().equals("_health")) {
            return null;
        } else if (rawPath.getParent() != null && "_health".equals(rawPath.getParent().getFileName().toString())) {
            return rawPath.getFileName().toString();
        } else if (rawPath.getParent() != null
            && rawPath.getParent().getParent() != null
            && "_health".equals(rawPath.getParent().getParent().getFileName().toString())) {
                return rawPath.getParent().getFileName().toString();
            }
        return null;
    }

    private String getIndicatorNameFromPath(RestRequest request) {
        Path rawPath = Paths.get(request.rawPath());
        if (rawPath.getFileName().toString().equals("_health")) {
            return null;
        } else if (rawPath.getParent() != null && "_health".equals(rawPath.getParent().getFileName().toString())) {
            return null;
        } else if (rawPath.getParent() != null
            && rawPath.getParent().getParent() != null
            && "_health".equals(rawPath.getParent().getParent().getFileName().toString())) {
                return rawPath.getFileName().toString();
            }
        return null;
    }
}
