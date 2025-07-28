/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.LocalCommitsRefs;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.bytes.BytesReference.bytes;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class HollowIndexEngineTests extends EngineTestCase {

    private StatelessCommitService statelessCommitService = Mockito.mock(StatelessCommitService.class);
    private HollowShardsService hollowShardsService = Mockito.mock(HollowShardsService.class);

    private EngineConfig hollowEngineConfig(Store store, LongSupplier globalCheckpointSupplier) {
        return config(
            defaultSettings,
            store,
            createTempDir(),
            NoMergePolicy.INSTANCE,
            null,
            null,
            null,
            globalCheckpointSupplier,
            globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY,
            new NoneCircuitBreakerService(),
            null,
            policy -> new HollowIndexEngineDeletionPolicy(new LocalCommitsRefs())
        );
    }

    public void testReadOnly() throws IOException {
        IOUtils.close(engine, store);
        var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            store.createEmpty();

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            try (var hollowIndexEngine = new HollowIndexEngine(hollowConfig, statelessCommitService, hollowShardsService, mapperService)) {
                var exception = LuceneTestCase.TEST_ASSERTS_ENABLED ? AssertionError.class : UnsupportedOperationException.class;
                expectThrows(exception, () -> hollowIndexEngine.index(null));
                expectThrows(exception, () -> hollowIndexEngine.delete(null));
                expectThrows(exception, () -> hollowIndexEngine.noOp(null));
            }
        }
    }

    public void testShardFieldStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);
            try (InternalEngine engine = createEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                    engine.syncTranslog();
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.flush(true, true);
            }

            ShardFieldStats expectedShardFieldStats;
            try (InternalEngine engine = createEngine(engineConfig)) {
                expectedShardFieldStats = engine.shardFieldStats();
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            try (var hollowIndexEngine = new HollowIndexEngine(hollowConfig, statelessCommitService, hollowShardsService, mapperService)) {
                assertEquals(expectedShardFieldStats, hollowIndexEngine.shardFieldStats());
            }
        }
    }

    public void testDocStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);
            try (InternalEngine engine = createEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                    engine.syncTranslog();
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.flush(true, true);
            }

            DocsStats expectedDocStats;
            try (InternalEngine engine = createEngine(engineConfig)) {
                expectedDocStats = engine.docStats();
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            try (var hollowIndexEngine = new HollowIndexEngine(hollowConfig, statelessCommitService, hollowShardsService, mapperService)) {
                assertEquals(expectedDocStats, hollowIndexEngine.docStats());
            }
        }
    }

    public void testSegmentStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
            int numDocs = randomIntBetween(8, 32);
            try (InternalEngine engine = createEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(String.valueOf(i), null)));
                    engine.syncTranslog();
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.flush(true, true);
            }

            try (InternalEngine engine = createEngine(engineConfig)) {
                var segmentsStats = engine.segmentsStats(true, true);
                assertThat(segmentsStats.getCount(), greaterThan(0L));
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            try (var hollowIndexEngine = new HollowIndexEngine(hollowConfig, statelessCommitService, hollowShardsService, mapperService)) {
                var segmentsStats = hollowIndexEngine.segmentsStats(true, true);
                assertThat(segmentsStats, equalTo(new SegmentsStats()));
            }
        }
    }

    @Override
    protected String defaultMapping() {
        return """
            {
              "properties": {
                "dv": {
                  "type": "dense_vector",
                  "dims": 3,
                  "similarity": "cosine"
                },
                "sv": {
                  "type": "sparse_vector"
                }
              }
            }
            """;
    }

    public void testVectorStats() throws Exception {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            var engineConfig = config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, globalCheckpoint::get);
            var documentMapper = engineConfig.getMapperService().documentMapper();
            var mappingLookup = engineConfig.getMapperService().mappingLookup();
            int numDocs = randomIntBetween(8, 32);
            try (InternalEngine engine = createEngine(engineConfig)) {
                for (int i = 0; i < numDocs; i++) {
                    var denseVectorSource = JsonXContent.contentBuilder().startObject().array("dv", randomVector(3)).endObject();
                    var denseVectorDoc = documentMapper.parse(new SourceToParse("dv_" + i, bytes(denseVectorSource), XContentType.JSON));
                    engine.index(indexForDoc(denseVectorDoc));

                    var sparseVectorSource = JsonXContent.contentBuilder()
                        .startObject()
                        .field("sv")
                        .value(Map.of("a", randomPositiveFloat(), "b", randomPositiveFloat()))
                        .endObject();
                    var sparseVectorDoc = documentMapper.parse(new SourceToParse("sv_" + i, bytes(sparseVectorSource), XContentType.JSON));
                    engine.index(indexForDoc(sparseVectorDoc));

                    engine.syncTranslog();
                    globalCheckpoint.set(engine.getPersistedLocalCheckpoint());
                }
                waitForOpsToComplete(engine, numDocs - 1);
                engine.flush(true, true);
            }
            try (InternalEngine e = createEngine(engineConfig)) {
                assertThat(e.denseVectorStats(mappingLookup).getValueCount(), greaterThan(0L));
                assertThat(e.sparseVectorStats(mappingLookup).getValueCount(), greaterThan(0L));
            }

            var hollowConfig = hollowEngineConfig(store, globalCheckpoint::get);
            try (var e = new HollowIndexEngine(hollowConfig, statelessCommitService, hollowShardsService, mapperService)) {
                assertThat(e.denseVectorStats(mappingLookup).getValueCount(), equalTo(0L));
                assertThat(e.sparseVectorStats(mappingLookup).getValueCount(), equalTo(0L));
            }
        }
    }

    private static float[] randomVector(int numDimensions) {
        float[] vector = new float[numDimensions];
        for (int j = 0; j < numDimensions; j++) {
            vector[j] = randomPositiveFloat();
        }
        return vector;
    }

    private static float randomPositiveFloat() {
        return randomFloatBetween(0, 1, true);
    }
}
