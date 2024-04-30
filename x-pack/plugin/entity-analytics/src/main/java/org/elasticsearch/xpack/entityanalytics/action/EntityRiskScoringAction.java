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
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.entityanalytics.EntityRiskPlugin;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringRequest;
import org.elasticsearch.xpack.entityanalytics.models.EntityRiskScoringResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EntityRiskScoringAction extends ActionType<EntityRiskScoringResponse> {
    public static final EntityRiskScoringAction INSTANCE = new EntityRiskScoringAction();
    public static final String NAME = "indices:data/write/xpack/entity_risk_score";

    private EntityRiskScoringAction() {
        super(NAME);
    }

    /**
     * Performs a series of elasticsearch queries and aggregations to calculate entity risk score
     */
    public static class TransportEntityRiskScoringAction
        extends HandledTransportAction<EntityRiskScoringRequest, EntityRiskScoringResponse> {
        private static final Logger logger = LogManager.getLogger(TransportEntityRiskScoringAction.class);

        private final NodeClient client;
        private final NamedXContentRegistry xContentRegistry;
        protected final XPackLicenseState licenseState;

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
                calculateRiskScores(request.getCategory1Index(), listener);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackField.ENTITY_ANALYTICS));
            }
        }

        private void calculateRiskScores(String category1Index, ActionListener<EntityRiskScoringResponse> listener) {
            try {
                var query = """
                  {
                    "size": 0,
                    "query": {
                      "function_score": {
                        "query": {
                          "bool": {
                            "filter": [
                              {
                                "range": {
                                  "@timestamp": {
                                    "gte": "now-30d",
                                    "lt": "now"
                                  }
                                }
                              },
                              {
                                "bool": {
                                  "must_not": {
                                    "term": {
                                      "kibana.alert.workflow_status": "closed"
                                    }
                                  }
                                }
                              },
                              {
                                "exists": {
                                  "field": "kibana.alert.risk_score"
                                }
                              }
                            ],
                            "should": [
                              {
                                "match_all": {}
                              }
                            ]
                          }
                        },
                        "field_value_factor": {
                          "field": "kibana.alert.risk_score"
                        }
                      }
                    },
                    "aggs": {
                      "user": {
                        "composite": {
                          "size": 10,
                          "sources": [
                            {
                              "user.name": {
                                "terms": {
                                  "field": "user.name"
                                }
                              }
                            }
                          ]
                        },
                        "aggs": {
                          "top_inputs": {
                            "sampler": {
                              "shard_size": 1000
                            },
                            "aggs": {
                              "risk_details": {
                                "top_hits": {
                                  "size": 10
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                """;


                client.search(createSearchRequest(category1Index, query), new DelegatingActionListener<>(listener) {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        Map<String, Double> results = new HashMap<>();
                        CompositeAggregation userCompositeAgg = searchResponse.getAggregations().get("user");
                        userCompositeAgg.getBuckets().forEach(eachBucket -> {
                            TopHits hits = (TopHits) eachBucket.getAggregations().get("top_inputs").getProperty("risk_details");
                            var alertRiskScores = new ArrayList<Double>();
                            hits.getHits().forEach(eachAlert -> {
                                var alertSource = eachAlert.getSourceAsMap();
                                alertRiskScores.add(Double.parseDouble(alertSource.get("kibana.alert.risk_score").toString()));
                            });
                            alertRiskScores.sort(Collections.reverseOrder());

                            double total_score = 0;
                            for (int i = 0; i < alertRiskScores.size(); i++) {
                                total_score += alertRiskScores.get(i) / Math.pow(i + 1, 1.5);
                            }

                            double normalizedScore = (100 * total_score) / 261.2;

                            results.put(eachBucket.getKey().get("user.name").toString(), normalizedScore);                        });

                        listener.onResponse(buildResponse(results));
                    }
                });
            } catch (Exception e) {
                logger.error("unable to execute the entity analytics query", e);
                listener.onFailure(e);
            }
        }

        private SearchRequest createSearchRequest(String category1Index, String query) throws IOException {
            var config = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry);

            var parser = JsonXContent.jsonXContent.createParser(config, query);

            return new SearchRequest(category1Index).source(
                SearchSourceBuilder.searchSource().parseXContent(parser, false, c -> false));
        }

        protected EntityRiskScoringResponse buildResponse(Map<String, Double> response) {
            return new EntityRiskScoringResponse(response);
        }
    }
}
