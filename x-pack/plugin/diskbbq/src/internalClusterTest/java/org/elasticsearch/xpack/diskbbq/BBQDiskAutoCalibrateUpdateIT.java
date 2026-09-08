/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * {@code auto_calibrate} is an updatable {@code bbq_disk} index option, so a single index can hold segments
 * written before and after the flip. These tests index across the update and then force merge past
 * {@link IvfAutoCalibration#MIN_VECTORS_FOR_CALIBRATION} so merge-time calibration actually runs, asserting
 * that kNN recall against brute-force ground truth survives in both directions.
 *
 * <p>The {@code true -> false} direction is the interesting one: merge calibration may decide to precondition
 * a segment even when the mapping says {@code precondition: false}. That choice is persisted, so the query
 * must keep being transformed for that segment after {@code auto_calibrate} is switched off — otherwise
 * recall collapses.
 */
@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class BBQDiskAutoCalibrateUpdateIT extends ESIntegTestCase {

    private static final String INDEX = "bbq-disk-auto-calibrate";
    private static final String FIELD = "vector";
    private static final int DIMS = 32;
    private static final int K = 10;
    private static final float VISIT_PERCENTAGE = 100f;

    /** Split so the total only clears the calibration threshold once both batches are merged together. */
    private static final int FIRST_BATCH = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 500;
    private static final int SECOND_BATCH = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 500;
    private static final int EXTRA_BATCH = 1000;
    private static final int TOTAL_DOCS = FIRST_BATCH + SECOND_BATCH;
    private static final int MAX_DOCS = TOTAL_DOCS + EXTRA_BATCH;

    /**
     * A floor separating "working" from "broken" rather than a recall target. The clustered vectors below put
     * many near-ties inside the top-{@link #K}, which 1-bit quantization cannot resolve, so even a healthy index
     * only reaches roughly 0.5 here. A mismatch between the query transform and a segment's persisted
     * preconditioning drives recall to roughly zero, far below this.
     */
    private static final double MIN_RECALL = 0.3;

    private float[][] vectors;

    @Before
    public void resetLicensing() {
        enableLicensing();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }

    /**
     * Indexes under {@code auto_calibrate: false}, turns it on, indexes more, then force merges the whole
     * index in one go so merge calibration runs over segments written under both settings.
     */
    public void testRecallSurvivesEnablingAutoCalibrate() {
        createIndexWithAutoCalibrate(false);
        generateVectors();

        indexVectors(0, FIRST_BATCH);
        flushAndRefresh(INDEX);
        assertRecallAtLeast(FIRST_BATCH, "uncalibrated, before mapping update");

        updateAutoCalibrate(true);

        indexVectors(FIRST_BATCH, SECOND_BATCH);
        flushAndRefresh(INDEX);
        // segments written under both settings coexist at this point
        assertRecallAtLeast(TOTAL_DOCS, "mixed segments, calibration enabled");

        forceMergeToOneSegment();
        assertRecallAtLeast(TOTAL_DOCS, "after calibrating force merge");
    }

    /**
     * The hazardous direction: force merge past {@link IvfAutoCalibration#MIN_VECTORS_FOR_CALIBRATION} first, so
     * a calibrated segment exists, and only then turn {@code auto_calibrate} off. Calibration may have chosen to
     * precondition that segment despite {@code precondition} defaulting to false in the mapping; since the
     * segment stores transformed vectors, queries must keep transforming for it after the flip.
     */
    public void testRecallSurvivesDisablingAutoCalibrate() {
        createIndexWithAutoCalibrate(true);
        generateVectors();

        indexVectors(0, TOTAL_DOCS);
        flushAndRefresh(INDEX);
        forceMergeToOneSegment();
        assertRecallAtLeast(TOTAL_DOCS, "calibrated, before mapping update");

        updateAutoCalibrate(false);
        assertRecallAtLeast(TOTAL_DOCS, "calibrated segment, calibration disabled");

        // further indexing and merging under the disabled setting must not regress the calibrated segment
        indexVectors(TOTAL_DOCS, EXTRA_BATCH);
        flushAndRefresh(INDEX);
        assertRecallAtLeast(TOTAL_DOCS + EXTRA_BATCH, "after indexing with calibration disabled");

        forceMergeToOneSegment();
        assertRecallAtLeast(TOTAL_DOCS + EXTRA_BATCH, "after uncalibrated force merge");
    }

    private void updateAutoCalibrate(boolean autoCalibrate) {
        assertAcked(indicesAdmin().preparePutMapping(INDEX).setSource(mappingSource(autoCalibrate), XContentType.JSON));
        assertAutoCalibrateInMapping(autoCalibrate);
    }

    private void forceMergeToOneSegment() {
        indicesAdmin().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX);
    }

    private void createIndexWithAutoCalibrate(boolean autoCalibrate) {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.shard.check_on_startup", "false")
            ).setMapping(mappingSource(autoCalibrate))
        );
        ensureGreen(INDEX);
    }

    private static String mappingSource(boolean autoCalibrate) {
        return Strings.format("""
            {
              "properties": {
                "%s": {
                  "type": "dense_vector",
                  "dims": %d,
                  "index": true,
                  "similarity": "dot_product",
                  "index_options": {
                    "type": "bbq_disk",
                    "auto_calibrate": %s
                  }
                }
              }
            }""", FIELD, DIMS, autoCalibrate);
    }

    @SuppressWarnings("unchecked")
    private void assertAutoCalibrateInMapping(boolean expected) {
        Map<String, Object> mapping = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, INDEX)
            .get()
            .mappings()
            .get(INDEX)
            .sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> field = (Map<String, Object>) properties.get(FIELD);
        Map<String, Object> indexOptions = (Map<String, Object>) field.get("index_options");
        // auto_calibrate is only serialized when enabled
        assertEquals(expected, Boolean.TRUE.equals(indexOptions.get("auto_calibrate")));
    }

    /** Unit vectors drawn in a few loose clusters so nearest neighbours are well separated. */
    private void generateVectors() {
        Random rnd = new Random(randomLong());
        vectors = new float[MAX_DOCS][DIMS];
        int clusters = 16;
        float[][] centers = new float[clusters][DIMS];
        for (float[] center : centers) {
            for (int d = 0; d < DIMS; d++) {
                center[d] = (float) rnd.nextGaussian();
            }
            normalize(center);
        }
        for (int i = 0; i < MAX_DOCS; i++) {
            float[] center = centers[i % clusters];
            for (int d = 0; d < DIMS; d++) {
                vectors[i][d] = center[d] + 0.35f * (float) rnd.nextGaussian();
            }
            normalize(vectors[i]);
        }
    }

    private void indexVectors(int startId, int count) {
        int batchSize = 1000;
        for (int offset = 0; offset < count; offset += batchSize) {
            BulkRequestBuilder bulk = client().prepareBulk();
            int end = Math.min(offset + batchSize, count);
            for (int i = offset; i < end; i++) {
                int id = startId + i;
                bulk.add(client().prepareIndex(INDEX).setId(String.valueOf(id)).setSource(FIELD, boxed(vectors[id])));
            }
            BulkResponse response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
    }

    /**
     * Runs a kNN search for a handful of query vectors and asserts that the mean overlap with the exact
     * top-{@link #K} neighbours over the first {@code indexedCount} vectors clears {@link #MIN_RECALL}.
     */
    private void assertRecallAtLeast(int indexedCount, String stage) {
        int queries = 20;
        double totalRecall = 0;
        for (int q = 0; q < queries; q++) {
            float[] query = vectors[randomIntBetween(0, indexedCount - 1)];
            Set<String> expected = exactNeighbours(query, indexedCount);
            Set<String> actual = new LinkedHashSet<>();
            assertResponse(
                prepareSearch(INDEX).setKnnSearch(List.of(new KnnSearchBuilder(FIELD, query, K, K * 10, VISIT_PERCENTAGE, null, null)))
                    .setSize(K),
                response -> Arrays.stream(response.getHits().getHits()).forEach(hit -> actual.add(hit.getId()))
            );
            actual.retainAll(expected);
            totalRecall += (double) actual.size() / expected.size();
        }
        double recall = totalRecall / queries;
        logger.info("recall@{} {}: {}", K, stage, recall);
        assertThat("recall@" + K + " " + stage, recall, greaterThanOrEqualTo(MIN_RECALL));
    }

    /** Exact top-{@link #K} by dot product over the first {@code indexedCount} vectors. */
    private Set<String> exactNeighbours(float[] query, int indexedCount) {
        List<Integer> ids = new ArrayList<>(indexedCount);
        for (int i = 0; i < indexedCount; i++) {
            ids.add(i);
        }
        ids.sort((a, b) -> Float.compare(dot(query, vectors[b]), dot(query, vectors[a])));
        Set<String> top = new LinkedHashSet<>();
        for (int i = 0; i < K; i++) {
            top.add(String.valueOf(ids.get(i)));
        }
        return top;
    }

    private static float dot(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    private static void normalize(float[] v) {
        double norm = 0;
        for (float value : v) {
            norm += (double) value * value;
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < v.length; i++) {
            v[i] /= (float) norm;
        }
    }

    private static List<Float> boxed(float[] v) {
        List<Float> list = new ArrayList<>(v.length);
        for (float value : v) {
            list.add(value);
        }
        return list;
    }
}
