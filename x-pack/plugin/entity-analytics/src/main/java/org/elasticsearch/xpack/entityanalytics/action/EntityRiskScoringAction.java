/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.entityanalytics.EntityRiskPlugin;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringRequest;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringResponse;
import org.elasticsearch.xpack.entityanalytics.models.EntityType;
import org.elasticsearch.xpack.entityanalytics.models.RiskScoreCalculator;
import org.elasticsearch.xpack.entityanalytics.models.RiskScoreQueryBuilder;

/*
 * TODO: comment here
 */
public class EntityRiskScoringAction extends ActionType<EntityRiskScoringResponse> {
    public static final EntityRiskScoringAction INSTANCE = new EntityRiskScoringAction();
    public static final String NAME = "indices:data/write/xpack/entity_risk_score";

    private EntityRiskScoringAction() {
        super(NAME);
    }

    /**
     * Performs a series of elasticsearch queries and aggregations to calculate entity risk score
     */
    public static class TransportEntityRiskScoringAction extends HandledTransportAction<
        EntityRiskScoringRequest,
        EntityRiskScoringResponse> {
        private static final Logger logger = LogManager.getLogger(TransportEntityRiskScoringAction.class);
        protected final XPackLicenseState licenseState;
        private final NodeClient client;
        private final NamedXContentRegistry xContentRegistry;

        @Inject
        public TransportEntityRiskScoringAction(
            NodeClient client,
            TransportService transportService,
            NamedXContentRegistry xContentRegistry,
            ActionFilters actionFilters,
            XPackLicenseState licenseState
        ) {
            super(
                EntityRiskScoringAction.NAME,
                transportService,
                actionFilters,
                EntityRiskScoringRequest::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.client = client;
            this.xContentRegistry = xContentRegistry;
            this.licenseState = licenseState;
        }

        @Override
        protected void doExecute(Task task, EntityRiskScoringRequest request, ActionListener<EntityRiskScoringResponse> listener) {
            if (EntityRiskPlugin.ENTITY_ANALYTICS_FEATURE.check(licenseState)) {
                calculateRiskScores(request.getCategory1Index(), request.getEntityTypes(), listener);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackField.ENTITY_ANALYTICS));
            }
        }

        private void calculateRiskScores(
            String category1Index,
            EntityType[] entityTypes,
            ActionListener<EntityRiskScoringResponse> listener
        ) {
            try {
                var sr = RiskScoreQueryBuilder.buildRiskScoreSearchRequest(category1Index, entityTypes);
                client.search(sr, new DelegatingActionListener<>(listener) {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        var result = RiskScoreCalculator.calculateRiskScores(entityTypes, searchResponse);
                        listener.onResponse(new EntityRiskScoringResponse(result));
                    }
                });
            } catch (Exception e) {
                logger.error("unable to execute the entity analytics query", e);
                listener.onFailure(e);
            }
        }
    }
}
