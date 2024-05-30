/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.entityanalytics.action.EntityRiskScoringAction;
import org.elasticsearch.xpack.entityanalytics.common.EntityTypeUtils;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringRequest;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * @see GraphExploreRequest
 */
@ServerlessScope(Scope.PUBLIC)
public class EntityRiskScoringRestHandler extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(Route.builder(POST, "/entity_analytics/risk_score/calculate").build());
    }

    @Override
    public String getName() {
        return "entity_risk_scoring";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        String[] AllEntityTypes = { "host", "user" };
        var category1Index = request.param("category_1_index");
        var entityTypes = EntityTypeUtils.fromStringArray(request.paramAsStringArray("entity_types", AllEntityTypes));

        return channel -> client.execute(
            EntityRiskScoringAction.INSTANCE,
            new EntityRiskScoringRequest(category1Index, entityTypes),
            new RestToXContentListener<>(channel)
        );
    }

}
