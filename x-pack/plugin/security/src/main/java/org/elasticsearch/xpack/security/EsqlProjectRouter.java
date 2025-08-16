/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

import java.util.List;

public class EsqlProjectRouter {

    private static final Logger logger = LogManager.getLogger(EsqlProjectRouter.class);

    private final Client client;

    public EsqlProjectRouter(Client client) {
        this.client = client;
    }

    public List<String> route(List<String> projects, String projectRoutingQuery) {
        // Note: this just demonstrates that we can invoke the full ES|QL engine from the security layer
        // we certainly don't want to just run a generic ES|QL query

        // Instead we should expose the EsqlProjectRoutingAction to be called the same way we
        // expose the EsqlQueryAction and call EsqlProjectRoutingAction here
        // (we will need to repeat the dance done in https://github.com/elastic/elasticsearch/issues/104413)

        // EsqlProjectRoutingAction will be localOnly as it will only access an on-the fly in-memory index
        // so there are no network costs associated

        @SuppressWarnings("unchecked")
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> b = (EsqlQueryRequestBuilder<
            EsqlQueryRequest,
            EsqlQueryResponse>) EsqlQueryRequestBuilder.newRequestBuilder(client);

        b.query(projectRoutingQuery).execute(new ActionListener<>() {
            @Override
            public void onResponse(EsqlQueryResponse response) {
                logger.info("EsqlProjectRouter response: {}", response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("EsqlProjectRouter failure", e);
            }
        });

        return projects;
    }
}
