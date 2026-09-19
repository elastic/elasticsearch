/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.vectors.diskbbq.AutoCalibrationVectorFixtures;
import org.elasticsearch.index.codec.vectors.diskbbq.CalibrationAwareReader;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * End-to-end test verifying that {@code index_options.bits} acts as a ceiling on the doc-bit width
 * that merge-time auto-calibration may select.
 */
@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class IvfAutoCalibrationMaxDocBitsIT extends ESIntegTestCase {
    private static final String VECTOR_FIELD = "vector";
    private static final int DIMS = 64;
    private static final int CLUSTER_SIZE = 384;
    private static final int MAX_PROBE_ATTEMPTS = 20;

    /**
     * Corpus size: above {@code MIN_VECTORS_FOR_CALIBRATION = 10_000} so the merge path
     * actually calibrates, and below {@code MAX_QUERY_SAMPLE + MAX_CORPUS_SAMPLE = 16_640} so the
     * merge path and the in-test probe see the same complete vector multiset.
     */
    private static final int CORPUS_SIZE = 12_000;

    private static List<float[]> corpus;

    @BeforeClass
    public static void findNonTrivialCorpus() throws IOException {
        StringJoiner seen = new StringJoiner(", ");
        for (int attempt = 0; attempt < MAX_PROBE_ATTEMPTS; attempt++) {
            // Escalate toward uniform random: start at 16 clusters, reach CORPUS_SIZE by the end.
            int numClusters = 16 + (CORPUS_SIZE - 16) * attempt / Math.max(1, MAX_PROBE_ATTEMPTS - 1);
            long seed = attempt * 0xDEADBEEFL;
            FloatVectorValues values = AutoCalibrationVectorFixtures.clusteredHeapVectors(CORPUS_SIZE, DIMS, numClusters, seed);

            IvfAutoCalibration probe = new IvfAutoCalibration(CLUSTER_SIZE);
            IvfSegmentConfig result = probe.calibrate(values, VectorSimilarityFunction.DOT_PRODUCT);
            seen.add(result.osqEncoding().toString());

            if (result.osqEncoding().bits() > 1) {
                corpus = extractVectors(values);
                return;
            }
        }
        fail(
            "Could not find a corpus where unbounded calibration picks more than 1 doc bit after "
                + MAX_PROBE_ATTEMPTS
                + " attempts. Encodings seen: ["
                + seen
                + "]. The calibration cost model may have changed."
        );
    }

    @Before
    public void resetLicensing() {
        enableLicensing();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
    }

    /**
     * Verifies that {@code bits: 7} lets calibration pick an encoding with more than one doc bit,
     * while {@code bits: 1} clamps the result to exactly one doc bit on the same corpus.
     */
    public void testCeilingBoundsCalibration() throws IOException {
        String uncappedIndex = "ceiling_uncapped";
        String cappedIndex = "ceiling_capped";

        // Index the corpus under bits=7 (uncapped) and bits=1 (ceiling).
        createIvfIndex(uncappedIndex, 7);
        createIvfIndex(cappedIndex, 1);
        bulkIndex(uncappedIndex, corpus);
        bulkIndex(cappedIndex, corpus);

        // Force merge both so each has exactly one calibrated segment.
        assertNoFailures(indicesAdmin().prepareForceMerge(uncappedIndex, cappedIndex).setMaxNumSegments(1).get());

        // Uncapped: calibration must have chosen more than 1 doc bit.
        QuantEncoding uncappedEncoding = readPersistedEncoding(uncappedIndex);
        assertTrue(
            "bits=7 index should have calibrated to more than 1 doc bit on this corpus, got: " + uncappedEncoding,
            uncappedEncoding.bits() > 1
        );

        // Capped: calibration must be confined to 1 doc bit.
        QuantEncoding cappedEncoding = readPersistedEncoding(cappedIndex);
        assertEquals("bits=1 index should be capped at exactly 1 doc bit", (byte) 1, cappedEncoding.bits());
    }

    /**
     * Verifies that lowering {@code bits} via {@code _mapping} lowers the ceiling for subsequent
     * force merges.
     */
    public void testLoweringBitsLowersCeiling() throws IOException {
        String indexName = "ceiling_lowered";
        createIvfIndex(indexName, 7);

        // Index across two batches so there are at least two segments before the first merge.
        int half = corpus.size() / 2;
        bulkIndex(indexName, corpus.subList(0, half));
        indicesAdmin().prepareFlush(indexName).get();
        bulkIndex(indexName, corpus.subList(half, corpus.size()));

        // First force merge with bits=7: calibration is uncapped.
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        QuantEncoding before = readPersistedEncoding(indexName);
        assertTrue("bits=7 index should have calibrated to more than 1 doc bit, got: " + before, before.bits() > 1);

        // Lower the ceiling by updating the mapping.
        XContentBuilder updatedMapping = ivfMapping(1);
        assertAcked(indicesAdmin().preparePutMapping(indexName).setSource(updatedMapping).get());

        // Index a small extra batch so there are again two segments. Total remains under
        // MAX_QUERY_SAMPLE + MAX_CORPUS_SAMPLE so the merge still sees the full set.
        List<float[]> extra = corpus.subList(0, Math.min(2000, corpus.size()));
        bulkIndex(indexName, extra);
        indicesAdmin().prepareFlush(indexName).get();

        // Second force merge with bits=1: calibration must now be capped.
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        QuantEncoding after = readPersistedEncoding(indexName);
        assertEquals("bits=1 ceiling should confine calibration to 1 doc bit after mapping update", (byte) 1, after.bits());
    }

    private void createIvfIndex(String indexName, int bits) throws IOException {
        prepareCreate(indexName).setMapping(ivfMapping(bits)).get(TEST_REQUEST_TIMEOUT);
        ensureGreen(indexName);
    }

    private XContentBuilder ivfMapping(int bits) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(VECTOR_FIELD)
            .field("type", "dense_vector")
            .field("element_type", "float")
            .field("dims", DIMS)
            .field("index", true)
            .field("similarity", "dot_product")
            .startObject("index_options")
            .field("type", "bbq_disk")
            .field("bits", bits)
            .field("cluster_size", CLUSTER_SIZE)
            .field("auto_calibrate", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    private void bulkIndex(String indexName, List<float[]> vectors) {
        var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (float[] v : vectors) {
            bulk.add(client().prepareIndex(indexName).setSource(VECTOR_FIELD, v));
        }
        // Index in one bulk call to minimize segment fragmentation before the explicit flush.
        var response = bulk.get(TEST_REQUEST_TIMEOUT);
        assertFalse("bulk indexing must not fail: " + response.buildFailureMessage(), response.hasFailures());
    }

    /**
     * Reads the {@link QuantEncoding} persisted in the (single) merged segment of the given index
     * by acquiring an engine searcher on the primary shard and unwrapping to
     * {@link CalibrationAwareReader}.
     */
    private QuantEncoding readPersistedEncoding(String indexName) {
        IndexShard shard = internalCluster().getInstance(IndicesService.class).indexServiceSafe(resolveIndex(indexName)).getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            List<QuantEncoding> encodings = new ArrayList<>();
            for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
                SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(ctx.reader());
                if (segmentReader == null) {
                    continue;
                }
                KnnVectorsReader kvr = segmentReader.getVectorReader();
                if (kvr instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
                    kvr = perField.getFieldReader(VECTOR_FIELD);
                }
                if (kvr instanceof CalibrationAwareReader car) {
                    FieldInfo fi = segmentReader.getFieldInfos().fieldInfo(VECTOR_FIELD);
                    if (fi != null) {
                        encodings.add(car.getQuantEncoding(fi));
                    }
                }
            }

            // After a force merge to 1 segment all leaves should agree; return the first.
            assertFalse("No calibrated segments found in index [" + indexName + "]", encodings.isEmpty());
            return encodings.getFirst();
        }
    }

    private static List<float[]> extractVectors(FloatVectorValues values) throws IOException {
        List<float[]> result = new ArrayList<>(values.size());
        var iter = values.iterator();
        while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            result.add(values.vectorValue(iter.docID()).clone());
        }
        return result;
    }
}
