/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class VectorIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "test";
    private static final String VECTOR_FIELD = "vector";
    private static final String NUM_ID_FIELD = "num_id";

    private static void randomVector(float[] vector, int constant) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat() * constant;
        }
        VectorUtil.l2normalize(vector);
    }

    @Before
    public void setup() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("similarity", "dot_product")
            .startObject("index_options")
            .field("type", "hnsw")
            .endObject()
            .endObject()
            .startObject(NUM_ID_FIELD)
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        prepareCreate(INDEX_NAME).setMapping(mapping).setSettings(settings).get();
        ensureGreen(INDEX_NAME);
        float[] vector = new float[16];
        for (int i = 0; i < 150; i++) {
            randomVector(vector, i % 25 + 1);
            prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource(VECTOR_FIELD, vector, NUM_ID_FIELD, i).get();
        }
        forceMerge(true);
        refresh(INDEX_NAME);
    }

    public void testFilteredQueryStrategy() {
        float[] vector = new float[16];
        randomVector(vector, 25);
        int upperLimit = 35;
        var query = new KnnSearchBuilder(VECTOR_FIELD, vector, 1, 1, null, null).addFilterQuery(
            QueryBuilders.rangeQuery(NUM_ID_FIELD).lte(35)
        );
        assertResponse(client().prepareSearch(INDEX_NAME).setKnnSearch(List.of(query)).setSize(1).setProfile(true), acornResponse -> {
            assertNotEquals(0, acornResponse.getHits().getHits().length);
            var profileResults = acornResponse.getProfileResults();
            long vectorOpsSum = profileResults.values()
                .stream()
                .mapToLong(
                    pr -> pr.getQueryPhase()
                        .getSearchProfileDfsPhaseResult()
                        .getQueryProfileShardResult()
                        .stream()
                        .mapToLong(qpr -> qpr.getVectorOperationsCount().longValue())
                        .sum()
                )
                .sum();
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put(
                            DenseVectorFieldMapper.HNSW_FILTER_HEURISTIC.getKey(),
                            DenseVectorFieldMapper.FilterHeuristic.FANOUT.toString()
                        )
                )
                .get();
            assertResponse(client().prepareSearch(INDEX_NAME).setKnnSearch(List.of(query)).setSize(1).setProfile(true), fanoutResponse -> {
                assertNotEquals(0, fanoutResponse.getHits().getHits().length);
                var fanoutProfileResults = fanoutResponse.getProfileResults();
                long fanoutVectorOpsSum = fanoutProfileResults.values()
                    .stream()
                    .mapToLong(
                        pr -> pr.getQueryPhase()
                            .getSearchProfileDfsPhaseResult()
                            .getQueryProfileShardResult()
                            .stream()
                            .mapToLong(qpr -> qpr.getVectorOperationsCount().longValue())
                            .sum()
                    )
                    .sum();
                assertTrue(
                    "fanoutVectorOps [" + fanoutVectorOpsSum + "] is not gt acornVectorOps [" + vectorOpsSum + "]",
                    fanoutVectorOpsSum > vectorOpsSum
                        // if both switch to brute-force due to excessive exploration, they will both equal to upperLimit
                        || (fanoutVectorOpsSum == vectorOpsSum && vectorOpsSum == upperLimit + 1)
                );
            });
        });
    }

    public void testHnswEarlyTerminationQuery() {
        float[] vector = new float[16];
        randomVector(vector, 25);
        int upperLimit = 35;
        var query = new KnnSearchBuilder(VECTOR_FIELD, vector, 1, 1, null, null);
        assertResponse(client().prepareSearch(INDEX_NAME).setKnnSearch(List.of(query)).setSize(1).setProfile(true), response -> {
            assertNotEquals(0, response.getHits().getHits().length);
            var profileResults = response.getProfileResults();
            long vectorOpsSum = profileResults.values()
                .stream()
                .mapToLong(
                    pr -> pr.getQueryPhase()
                        .getSearchProfileDfsPhaseResult()
                        .getQueryProfileShardResult()
                        .stream()
                        .mapToLong(qpr -> qpr.getVectorOperationsCount().longValue())
                        .sum()
                )
                .sum();
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(DenseVectorFieldMapper.HNSW_EARLY_TERMINATION.getKey(), true))
                .get();
            assertResponse(
                client().prepareSearch(INDEX_NAME).setKnnSearch(List.of(query)).setSize(1).setProfile(true),
                earlyTerminationResponse -> {
                    assertNotEquals(0, earlyTerminationResponse.getHits().getHits().length);
                    var earlyTerminationResults = earlyTerminationResponse.getProfileResults();
                    long earlyTerminationVectorOpsSum = earlyTerminationResults.values()
                        .stream()
                        .mapToLong(
                            pr -> pr.getQueryPhase()
                                .getSearchProfileDfsPhaseResult()
                                .getQueryProfileShardResult()
                                .stream()
                                .mapToLong(qpr -> qpr.getVectorOperationsCount().longValue())
                                .sum()
                        )
                        .sum();
                    assertTrue(
                        "earlyTerminationVectorOps [" + earlyTerminationVectorOpsSum + "] is not lt vectorOps [" + vectorOpsSum + "]",
                        earlyTerminationVectorOpsSum < vectorOpsSum
                            // if both switch to brute-force due to excessive exploration, they will both equal to upperLimit
                            || (earlyTerminationVectorOpsSum == vectorOpsSum && vectorOpsSum == upperLimit + 1)
                    );
                }
            );
        });
    }

}
