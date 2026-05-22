/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class LuceneChangesSnapshotTests extends SearchBasedChangesSnapshotTests {
    @Override
    protected Translog.Snapshot newRandomSnapshot(
        MapperService mapperService,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        return new LuceneChangesSnapshot(
            mapperService,
            engineSearcher,
            searchBatchSize,
            fromSeqNo,
            toSeqNo,
            requiredFullRange,
            singleConsumer,
            accessStats
        );
    }

    public void testAccessStoredFieldsSequentially() throws Exception {
        try (Store store = createStore(); Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            int smallBatch = between(5, 9);
            long seqNo = 0;
            for (int i = 0; i < smallBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null), 1, seqNo, true));
                seqNo++;
            }
            engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(1000), null), 1, 1000, true));
            seqNo = 11;
            int largeBatch = between(15, 100);
            for (int i = 0; i < largeBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null), 1, seqNo, true));
                seqNo++;
            }
            // disable optimization for a small batch
            Translog.Operation op;
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    0L,
                    between(1, smallBatch),
                    false,
                    randomBoolean(),
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
            // disable optimization for non-sequential accesses
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    between(1, 3),
                    between(20, 100),
                    false,
                    randomBoolean(),
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
            // enable optimization for sequential access of 10+ docs
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    11,
                    between(21, 100),
                    false,
                    true,
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertTrue(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertTrue(snapshot.useSequentialStoredFieldsReader());
            }
            // disable optimization if snapshot is accessed by multiple consumers
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    11,
                    between(21, 100),
                    false,
                    false,
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
        }
    }

    public void testColumnarStoredSourceRecovery() throws Exception {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());

        Settings columnarSettings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        IndexSettings columnarIndexSettings = IndexSettingsModule.newIndexSettings("index", columnarSettings);

        // Keyword-only mapping: no nested fields (not supported in columnar mode)
        String columnarMapping = """
            {"dynamic": false, "properties": {"value": {"type": "keyword"}}}
            """;
        var columnarMapperService = createMapperService(columnarSettings, columnarMapping);

        // Temporarily swap the instance mapperService so createEngine uses the columnar one
        var savedMapperService = this.mapperService;
        this.mapperService = columnarMapperService;
        int numDocs = between(1, 10);
        try (
            Store store = createStore(columnarIndexSettings, newDirectory());
            InternalEngine testEngine = createEngine(columnarIndexSettings, store, createTempDir(), NoMergePolicy.INSTANCE)
        ) {
            for (int i = 0; i < numDocs; i++) {
                var doc = parseDocument(columnarMapperService, Integer.toString(i), null);
                testEngine.index(replicaIndexForDoc(doc, 1, i, true));
            }
            testEngine.flush();

            try (
                Translog.Snapshot snapshot = testEngine.newChangesSnapshot(
                    "test",
                    0,
                    numDocs - 1,
                    true,
                    true,
                    false,
                    ByteSizeValue.ofMb(32).getBytes()
                )
            ) {
                Translog.Operation op;
                int count = 0;
                while ((op = snapshot.next()) != null) {
                    assertTrue("expected Index operation but got " + op.opType(), op instanceof Translog.Index);
                    assertNotNull(((Translog.Index) op).source());
                    count++;
                }
                assertThat(count, equalTo(numDocs));
            }
        } finally {
            this.mapperService = savedMapperService;
        }
    }
}
