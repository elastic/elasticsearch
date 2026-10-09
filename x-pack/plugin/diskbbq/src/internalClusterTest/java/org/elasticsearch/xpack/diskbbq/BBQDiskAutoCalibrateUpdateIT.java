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
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

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
    /** Large enough for the multi-phase tests below, which index more than {@link #TOTAL_DOCS}. */
    private static final int MAX_DOCS = 16_000;

    /**
     * A floor separating "working" from "broken" rather than a recall target. The clustered vectors below put
     * many near-ties inside the top-{@link #K}, which 1-bit quantization cannot resolve, so even a healthy index
     * only reaches roughly 0.5 here. A mismatch between the query transform and a segment's persisted
     * preconditioning drives recall to roughly zero, far below this.
     */
    private static final double MIN_RECALL = 0.3;

    private float[][] vectors;
    /** Ids that are currently searchable: deleted ids are excluded from the brute-force ground truth. */
    private boolean[] live;

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

    /**
     * Flips {@code auto_calibrate} several times in a row, indexing, searching and force merging between every
     * flip, so segments written and merged under every combination of before/after settings accumulate in one
     * shard. Merges happen both below and above {@link IvfAutoCalibration#MIN_VECTORS_FOR_CALIBRATION}, with
     * calibration on and off, and recall is checked at every step.
     */
    public void testRecallSurvivesRepeatedToggling() {
        final int batch = 3500;
        createIndexWithAutoCalibrate(false);
        generateVectors();
        int indexed = 0;

        indexed = indexAndRefresh(indexed, batch);
        assertRecallAtLeast(indexed, "off: first batch");

        updateAutoCalibrate(true);
        indexed = indexAndRefresh(indexed, batch);
        assertRecallAtLeast(indexed, "on: segments from both settings");

        updateAutoCalibrate(false);
        indexed = indexAndRefresh(indexed, batch);
        assertRecallAtLeast(indexed, "off: segments from three batches");
        // above the calibration threshold, but calibration is off so this merge must not calibrate
        forceMergeToOneSegment();
        assertRecallAtLeast(indexed, "off: uncalibrated merge over segments written under on and off");

        updateAutoCalibrate(true);
        indexed = indexAndRefresh(indexed, batch);
        assertRecallAtLeast(indexed, "on: uncalibrated merged segment next to a fresh one");
        forceMergeToOneSegment();
        assertRecallAtLeast(indexed, "on: calibrating merge over a previously uncalibrated segment");

        updateAutoCalibrate(false);
        indexed = indexAndRefresh(indexed, EXTRA_BATCH);
        assertRecallAtLeast(indexed, "off: calibrated segment next to a fresh one");
        forceMergeToOneSegment();
        assertRecallAtLeast(indexed, "off: merge over a calibrated segment");

        updateAutoCalibrate(true);
        assertRecallAtLeast(indexed, "on: re-enabled after the last merge");
    }

    /**
     * Toggling on an index that never reaches {@link IvfAutoCalibration#MIN_VECTORS_FOR_CALIBRATION}, so no merge
     * ever calibrates, must keep working and keep the mapping and the segments consistent.
     */
    public void testTogglingBelowCalibrationThreshold() {
        createIndexWithAutoCalibrate(false);
        generateVectors();
        int indexed = 0;
        boolean autoCalibrate = false;
        for (int round = 0; round < 4; round++) {
            indexed = indexAndRefresh(indexed, 500);
            assertRecallAtLeast(indexed, "round " + round + ", auto_calibrate=" + autoCalibrate);
            forceMergeToOneSegment();
            assertRecallAtLeast(indexed, "round " + round + " merged, auto_calibrate=" + autoCalibrate);
            autoCalibrate = autoCalibrate == false;
            updateAutoCalibrate(autoCalibrate);
        }
        assertThat(indexed, lessThan(IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION));
    }

    /**
     * Deletes and overwrites leave tombstoned vectors inside segments written under the old setting. Merging them
     * under the new setting recalibrates over only the live vectors, and search must still return the live
     * neighbours, never a deleted or stale one. Runs from both starting values of {@code auto_calibrate}.
     */
    public void testDeletesAndOverwritesAcrossToggle() {
        for (boolean initial : new boolean[] { true, false }) {
            if (indexExists(INDEX)) {
                assertAcked(indicesAdmin().prepareDelete(INDEX));
            }
            createIndexWithAutoCalibrate(initial);
            generateVectors();
            indexVectors(0, TOTAL_DOCS);
            flushAndRefresh(INDEX);
            forceMergeToOneSegment();
            assertRecallAtLeast(TOTAL_DOCS, "initial auto_calibrate=" + initial);

            updateAutoCalibrate(initial == false);

            deleteDocs(0, TOTAL_DOCS / 10);
            overwriteDocs(TOTAL_DOCS / 10, TOTAL_DOCS / 5);
            flushAndRefresh(INDEX);
            assertRecallAtLeast(TOTAL_DOCS, "deleted and overwritten after flipping to " + (initial == false));

            forceMergeToOneSegment();
            assertRecallAtLeast(TOTAL_DOCS, "merged after deletes under auto_calibrate=" + (initial == false));

            updateAutoCalibrate(initial);
            overwriteDocs(TOTAL_DOCS / 5, TOTAL_DOCS / 4);
            indexVectors(TOTAL_DOCS, EXTRA_BATCH);
            flushAndRefresh(INDEX);
            forceMergeToOneSegment();
            assertRecallAtLeast(TOTAL_DOCS + EXTRA_BATCH, "flipped back to " + initial + " and merged");
        }
    }

    /**
     * Searches run continuously while the mapping is flipped and new segments are indexed, refreshed and merged
     * underneath them. A search planned against one value of {@code auto_calibrate} executes over segments
     * written under the other, so none may fail or come back short, and recall must hold once things settle.
     */
    public void testConcurrentSearchesDuringToggleAndIndexing() throws Exception {
        createIndexWithAutoCalibrate(false);
        generateVectors();
        indexVectors(0, TOTAL_DOCS);
        flushAndRefresh(INDEX);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicLong searches = new AtomicLong();
        long seed = randomLong();
        Thread searcher = new Thread(() -> {
            Random rnd = new Random(seed);
            while (stop.get() == false && failure.get() == null) {
                try {
                    float[] query = vectors[rnd.nextInt(TOTAL_DOCS)];
                    assertResponse(
                        prepareSearch(INDEX).setKnnSearch(
                            List.of(new KnnSearchBuilder(FIELD, query, K, K * 10, VISIT_PERCENTAGE, null, null))
                        ).setSize(K),
                        response -> assertThat(response.getHits().getHits().length, equalTo(K))
                    );
                    searches.incrementAndGet();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }
        }, "auto-calibrate-searcher");
        searcher.start();
        try {
            boolean autoCalibrate = false;
            int indexed = TOTAL_DOCS;
            for (int round = 0; round < 6; round++) {
                autoCalibrate = autoCalibrate == false;
                updateAutoCalibrate(autoCalibrate);
                indexed = indexAndRefresh(indexed, 400);
                if (round % 2 == 1) {
                    forceMergeToOneSegment();
                }
            }
            long searchesSoFar = searches.get();
            assertBusy(() -> assertTrue("searcher made no progress", failure.get() != null || searches.get() > searchesSoFar));
            stop.set(true);
            searcher.join();
            if (failure.get() != null) {
                throw new AssertionError("search failed while auto_calibrate was being toggled", failure.get());
            }
            assertRecallAtLeast(indexed, "after concurrent toggling");
        } finally {
            stop.set(true);
            searcher.join();
        }
    }

    private int indexAndRefresh(int startId, int count) {
        indexVectors(startId, count);
        flushAndRefresh(INDEX);
        return startId + count;
    }

    private void deleteDocs(int from, int to) {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = from; id < to; id++) {
            bulk.add(client().prepareDelete(INDEX, String.valueOf(id)));
            live[id] = false;
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    /** Replaces the vectors of {@code [from, to)} with fresh ones, so the old versions become deleted docs. */
    private void overwriteDocs(int from, int to) {
        Random rnd = new Random(randomLong());
        for (int id = from; id < to; id++) {
            for (int d = 0; d < DIMS; d++) {
                vectors[id][d] = (float) rnd.nextGaussian();
            }
            normalize(vectors[id]);
        }
        indexVectors(from, to - from);
    }

    private void updateAutoCalibrate(boolean autoCalibrate) {
        assertAcked(indicesAdmin().preparePutMapping(INDEX).setSource(mappingSource(autoCalibrate)));
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
        live = new boolean[MAX_DOCS];
        Arrays.fill(live, true);
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
            int queryId;
            do {
                queryId = randomIntBetween(0, indexedCount - 1);
            } while (live[queryId] == false);
            float[] query = vectors[queryId];
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
            if (live[i]) {
                ids.add(i);
            }
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
