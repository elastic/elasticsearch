/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class EngineDiskUtilsTests extends EngineTestCase {


    public void testHistoryUUIDIsSetIfMissing() throws IOException {
        final int numDocs = randomIntBetween(0, 3);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0,
                Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        engine.close();

        IndexWriterConfig iwc = new IndexWriterConfig(null)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        try (IndexWriter writer = new IndexWriter(store.directory(), iwc)) {
            Map<String, String> newCommitData = new HashMap<>();
            for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
                if (entry.getKey().equals(Engine.HISTORY_UUID_KEY) == false) {
                    newCommitData.put(entry.getKey(), entry.getValue());
                }
            }
            writer.setLiveCommitData(newCommitData.entrySet());
            writer.commit();
        }

        EngineDiskUtils.ensureIndexHasHistoryUUID(store.directory());

        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_0_0_beta1)
            .build());

        EngineConfig config = engine.config();
        EngineConfig newConfig = new EngineConfig(
            shardId, allocationId.getId(),
            threadPool, indexSettings, null, store, newMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(), config.getTranslogConfig(), TimeValue.timeValueMinutes(5),
            config.getExternalRefreshListener(), config.getInternalRefreshListener(), null, config.getTranslogRecoveryRunner(),
            new NoneCircuitBreakerService(), () -> SequenceNumbers.NO_OPS_PERFORMED);
        engine = new InternalEngine(newConfig);
        engine.recoverFromTranslog();
        assertVisibleCount(engine, numDocs, false);
        assertThat(engine.getHistoryUUID(), notNullValue());
    }

    public void testCurrentTranslogIDisCommitted() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            // create
            {
                EngineDiskUtils.createEmpty(store.directory(), config.getTranslogConfig().getTranslogPath(), shardId);
                ParsedDocument doc = testParsedDocument(Integer.toString(0), null, testDocument(), new BytesArray("{}"), null);
                Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0,
                    Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);

                try (InternalEngine engine = createEngine(config)) {
                    engine.index(firstIndexRequest);
                    globalCheckpoint.set(engine.getLocalCheckpointTracker().getCheckpoint());
                    expectThrows(IllegalStateException.class, () -> engine.recoverFromTranslog());
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                    assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                }
            }
            // open and recover tlog
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(config)) {
                        assertTrue(engine.isRecovering());
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        if (i == 0) {
                            assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        } else {
                            // creating an empty index will create the first translog gen and commit it
                            // opening the empty index will make the second translog file but not commit it
                            // opening the engine again (i=0) will make the third translog file, which then be committed
                            assertEquals("3", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        }
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                        engine.recoverFromTranslog();
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("3", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    }
                }
            }
            // open index with new tlog
            {
                EngineDiskUtils.createNewTranslog(store.directory(), config.getTranslogConfig().getTranslogPath(),
                    SequenceNumbers.NO_OPS_PERFORMED, shardId);
                try (InternalEngine engine = new InternalEngine(config)) {
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                    assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    engine.recoverFromTranslog();
                    assertEquals(2, engine.getTranslog().currentFileGeneration());
                    assertEquals(0L, engine.getTranslog().uncommittedOperations());
                }
            }

            // open and recover tlog with empty tlog
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(config)) {
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                        engine.recoverFromTranslog();
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("no changes - nothing to commit", "1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    }
                }
            }
        }
    }

    public void testHistoryUUIDCanBeForced() throws IOException {
        final int numDocs = randomIntBetween(0, 3);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0,
                Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        final String oldHistoryUUID = engine.getHistoryUUID();
        engine.close();
        EngineConfig config = engine.config();
        EngineDiskUtils.bootstrapNewHistoryFromLuceneIndex(store.directory(), config.getTranslogConfig().getTranslogPath(), shardId);

        EngineConfig newConfig = new EngineConfig(
            shardId, allocationId.getId(),
            threadPool, config.getIndexSettings(), null, store, newMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(), config.getTranslogConfig(), TimeValue.timeValueMinutes(5),
            config.getExternalRefreshListener(), config.getInternalRefreshListener(), null, config.getTranslogRecoveryRunner(),
            new NoneCircuitBreakerService(), () -> SequenceNumbers.NO_OPS_PERFORMED);
        engine = new InternalEngine(newConfig);
        engine.recoverFromTranslog();
        assertVisibleCount(engine, 0, false);
        assertThat(engine.getHistoryUUID(), not(equalTo(oldHistoryUUID)));
    }
}
