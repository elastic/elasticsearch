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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.entityanalytics.EntityRiskPlugin;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringRequest;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringResponse;
import org.elasticsearch.xpack.entityanalytics.models.EntityType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        private static CompositeAggregationBuilder buildEntityAggregation(EntityType entityType) {
            String identifierField = (entityType.equals(EntityType.Host)) ? "host.name" : "user.name";
            String aggregationName = (entityType.equals(EntityType.Host)) ? "host" : "user";
            List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
            sources.add(new TermsValuesSourceBuilder(identifierField).field(identifierField));

            return AggregationBuilders.composite(aggregationName, sources)
                .size(10)
                .subAggregation(
                    AggregationBuilders.topHits("top_inputs")
                        .size(100)
                        .fetchSource(
                            FetchSourceContext.of(
                                true,
                                new String[] {
                                    "kibana.alert.risk_score",
                                    "event.kind",
                                    "kibana.alert.rule.name",
                                    "kibana.alert.uuid",
                                    "@timestamp" },
                                null
                            )
                        )
                );
        }

        private static SearchRequest buildRiskScoreSearchRequest(String index, EntityType[] entityTypes) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            // Calculate current time and 30 days ago in Unix time as 'now-30d' was causing 'failed to create query: For input
            // string: \"now-30d\"'
            long now = Instant.now().toEpochMilli();
            long nowMinus30Days = Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli();

            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("@timestamp").gte(nowMinus30Days).lt(now))
                .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("kibana.alert.workflow_status", "closed")))
                .filter(QueryBuilders.existsQuery("kibana.alert.risk_score"))
                .should(QueryBuilders.matchAllQuery());

            FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
                boolQuery,
                new FieldValueFactorFunctionBuilder("kibana.alert.risk_score")
            );

            searchSourceBuilder.query(functionScoreQuery);

            // Adding separate composite aggregations for user and host
            for (EntityType entityType : entityTypes) {
                searchSourceBuilder.aggregation(buildEntityAggregation(entityType));
            }

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(searchSourceBuilder);

            System.out.println("BADGER REQUEST " + searchSourceBuilder);
            return searchRequest;
        }

        private static Map<String, Double> processResults(SearchResponse searchResponse) {
            System.out.println("BADGER RESPONSE " + searchResponse);
            Map<String, Double> results = new HashMap<>();
            CompositeAggregation userCompositeAgg = searchResponse.getAggregations().get("user");
            for (CompositeAggregation.Bucket eachBucket : userCompositeAgg.getBuckets()) {
                TopHits hits = eachBucket.getAggregations().get("top_inputs");
                var alertRiskScores = new ArrayList<Double>();
                hits.getHits().forEach(eachAlert -> {
                    var alertSource = eachAlert.getSourceAsMap();
                    alertRiskScores.add(Double.parseDouble(alertSource.get("kibana.alert.risk_score").toString()));
                });
                alertRiskScores.sort(Collections.reverseOrder());

                double totalScore = 0;
                for (int i = 0; i < alertRiskScores.size(); i++) {
                    totalScore += alertRiskScores.get(i) / Math.pow(i + 1, Constants.RISK_SCORING_SUM_VALUE);
                }
                double normalizedScore = (Constants.RISK_SCORING_NORMALIZATION_MAX * totalScore) / Constants.RISK_SCORING_SUM_MAX;
                results.put(eachBucket.getKey().get("user.name").toString(), normalizedScore);
            }
            return results;
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
                var sr = buildRiskScoreSearchRequest(category1Index, entityTypes);
                client.search(sr, new DelegatingActionListener<>(listener) {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        System.out.println("BADGER in onResponse");
                        var resp = processResults(searchResponse);
                        listener.onResponse(buildResponse(resp));
                    }
                });
            } catch (Exception e) {
                logger.error("unable to execute the entity analytics query", e);
                listener.onFailure(e);
            }
        }

        protected EntityRiskScoringResponse buildResponse(Map<String, Double> response) {
            return new EntityRiskScoringResponse(response);
        }
    }
}
