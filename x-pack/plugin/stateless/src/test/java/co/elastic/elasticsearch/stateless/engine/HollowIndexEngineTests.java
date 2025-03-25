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
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.store.Store;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class HollowIndexEngineTests extends EngineTestCase {

    private StatelessCommitService statelessCommitService = Mockito.mock(StatelessCommitService.class);
    private HollowShardsService hollowShardsService = Mockito.mock(HollowShardsService.class);

    public void testReadOnly() throws IOException {
        IOUtils.close(engine, store);
        var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty();

            try (var hollowIndexEngine = new HollowIndexEngine(config, statelessCommitService, hollowShardsService)) {
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

            try (var hollowIndexEngine = new HollowIndexEngine(engineConfig, statelessCommitService, hollowShardsService)) {
                assertEquals(expectedShardFieldStats, hollowIndexEngine.shardFieldStats());
            }
        }
    }
}
