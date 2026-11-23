/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/** Every HNSW index_options type is evaluated alike, so each run draws one type, its early-termination setting and its data at random. */
public class KnnEvalHnswIT extends ESIntegTestCase {

    private static final int DIMS = 64; // bbq_hnsw's minimum
    private static final String INDEX = "hnsw-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(KnnEvalPlugin.class);
    }

    private String indexOptionsType;
    private boolean quantized;
    private boolean earlyTermination;
    private int docs;

    private void createIndex() throws IOException {
        indexOptionsType = randomFrom("hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw");
        quantized = indexOptionsType.equals("hnsw") == false;
        earlyTermination = randomBoolean();
        docs = randomIntBetween(10, 30);
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(DenseVectorFieldMapper.HNSW_EARLY_TERMINATION.getKey(), earlyTermination)
            ).setMapping(Strings.format("""
                { "properties": { "emb": { "type": "dense_vector", "dims": %d, "index": true, "similarity": "l2_norm",
                  "index_options": { "type": "%s" } } } }""", DIMS, indexOptionsType))
        );
        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            float[] vector = new float[DIMS];
            for (int d = 0; d < DIMS; d++) {
                vector[d] = randomFloatBetween(-1.0f, 1.0f, true);
            }
            requests.add(prepareIndex(INDEX).setId(Integer.toString(i)).setSource("emb", vector));
        }
        indexRandom(true, requests);
    }

    private KnnEvalResponse evaluate(int k, String knnSettings) throws IOException {
        String json = Strings.format(
            """
                { "field": "emb", "k": %d, "query_source": { "from": "docs", "size": %d, "seed": %d }, "knn_settings": [ %s ] }""",
            k,
            randomIntBetween(1, docs),
            randomInt(),
            knnSettings
        );
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return client().execute(KnnEvalPlugin.KNN_EVAL_ACTION, new KnnEvalRequest(KnnEvalSpec.parse(parser), new String[] { INDEX }))
                .actionGet();
        }
    }

    public void testOmittedBaselineIsBoundedAndIndexInfoIsReported() throws IOException {
        createIndex();
        int k = randomIntBetween(1, 5);
        List<String> entries = new ArrayList<>();
        int entryCount = randomIntBetween(1, 3);
        int numCandidates = randomIntBetween(k, 2 * docs);
        for (int i = 0; i < entryCount; i++) {
            String rescore = quantized && randomBoolean()
                ? ", \"rescore_vector\": { \"oversample\": " + randomFloatBetween(1.0f, 10.0f, true) + " }"
                : "";
            entries.add("{ \"num_candidates\": " + numCandidates + rescore + " }");
            // strictly increasing: the spec rejects duplicate entries
            numCandidates += randomIntBetween(1, 5);
        }

        KnnEvalResponse response = evaluate(k, String.join(",", entries));

        assertThat(response.getFailures(), anEmptyMap());
        // 100 * k is below the floor for these k
        assertEquals(
            Integer.valueOf(KnnEvalFieldType.MIN_BASELINE_NUM_CANDIDATES),
            response.getBaseline().knnSettings().getNumCandidates()
        );
        assertEquals(quantized ? Float.valueOf(100.0f) : null, response.getBaseline().knnSettings().getRescoreOversample());
        assertFalse(response.getBaseline().knnSettings().isExact());
        assertEquals(Map.of(INDEX, new KnnEvalResponse.IndexInfo(earlyTermination)), response.getIndexInfo());
        assertEquals(entryCount, response.getResults().size());
        for (KnnEvalResponse.KnnSettingsResult result : response.getResults()) {
            if (result.recall() != null) {
                assertThat(result.recall(), both(greaterThanOrEqualTo(0.0)).and(lessThanOrEqualTo(1.0)));
            }
        }
    }

    public void testSettingsOfTheOtherFamilyAreRejected() throws IOException {
        createIndex();
        IllegalArgumentException visit = expectThrows(
            IllegalArgumentException.class,
            () -> evaluate(1, "{ \"visit_percentage\": " + randomFloatBetween(0.0f, 100.0f, true) + " }")
        );
        assertThat(
            visit.getMessage(),
            containsString("[visit_percentage] applies only to bbq_disk, but the field is [" + indexOptionsType + "]")
        );

        String oversample = "{ \"num_candidates\": 10, \"rescore_vector\": { \"oversample\": "
            + randomFloatBetween(1.0f, 10.0f, true)
            + " } }";
        if (quantized) {
            assertEquals(1, evaluate(1, oversample).getResults().size());
        } else {
            IllegalArgumentException rescore = expectThrows(IllegalArgumentException.class, () -> evaluate(1, oversample));
            assertThat(rescore.getMessage(), containsString("does not apply to an unquantized [hnsw] field"));
        }
    }
}
