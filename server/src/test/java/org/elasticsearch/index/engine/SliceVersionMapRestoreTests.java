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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SliceIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that the version-map/checkpoint restore on a dirty restart (when the persisted local checkpoint is below
 * max_seq_no, e.g. after a crash with in-flight ops) keys restored delete tombstones of a slice-enabled index by their
 * <em>compound</em> {@code (slice, id)} identity term — the same term live operations use — rather than the plain
 * {@code encodeId(id)}. A slice delete tombstone stores the compound term directly (and carries no {@code _routing}),
 * which is the case the plain reconstruction path would crash on.
 */
public class SliceVersionMapRestoreTests extends EngineTestCase {

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.SLICE_VALIDATED.getKey(), true)
            .build();
    }

    public void testRestoreKeepsSliceDeletesKeyedByCompoundUid() throws Exception {
        final Path translogPath = createTempDir();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            final EngineConfig config = config(defaultSettings, store, translogPath, NoMergePolicy.INSTANCE, globalCheckpoint::get);
            try (InternalEngine engine = createEngine(config)) {
                engine.advanceMaxSeqNoOfUpdatesOrDeletes(4);
                // The same id "1" indexed in two slices, then deleted in each. A seq_no gap at 2 keeps the local
                // checkpoint below max_seq_no so the restore runs for the deletes (seq_no 3 and 4) on reopen.
                applyOperation(engine, sliceIndex("1", "s1", 0, 1));
                applyOperation(engine, sliceIndex("1", "s2", 1, 1));
                applyOperation(engine, sliceDelete("1", "s1", 3, 2));
                applyOperation(engine, sliceDelete("1", "s2", 4, 2));
                engine.syncTranslog();
                assertThat(engine.getProcessedLocalCheckpoint(), equalTo(1L));
                assertThat(engine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(4L));
                // Ack all ops globally (before flushing, so it is persisted) so that the just-flushed commit — not the
                // empty initial one — is the safe commit reopened below; the local checkpoint stays at 1 (gap at 2), so
                // the deletes at seq_no 3 and 4 remain above it and the restore must process them.
                globalCheckpoint.set(engine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo());
                engine.flush(true, true);
                assertThat(engine.getPersistedLocalCheckpoint(), equalTo(1L));
            }
            // Reopen without recovering from translog: this runs restoreVersionMapAndCheckpointTracker for seq_no > 1.
            try (InternalEngine engine = new InternalEngine(config)) {
                final Map<BytesRef, VersionValue> versionMap = engine.getVersionMap();
                final BytesRef uidA = SliceIdFieldMapper.encodeCompoundId("1", "s1");
                final BytesRef uidB = SliceIdFieldMapper.encodeCompoundId("1", "s2");
                // The two restored deletes are keyed by the compound identity terms, not the (shared) plain encodeId.
                assertThat(versionMap.keySet(), equalTo(Set.of(uidA, uidB)));
                assertThat(versionMap.get(uidA), instanceOf(DeleteVersionValue.class));
                assertThat(versionMap.get(uidB), instanceOf(DeleteVersionValue.class));
                assertThat(versionMap.get(uidA).seqNo, equalTo(3L));
                assertThat(versionMap.get(uidB).seqNo, equalTo(4L));
                assertFalse("deletes must not be keyed by the plain id", versionMap.containsKey(Uid.encodeId("1")));
            }
        }
    }

    private Engine.Index sliceIndex(String id, String slice, long seqNo, long version) {
        final ParsedDocument doc = parseDocument(engine.engineConfig.getMapperService(), id, slice);
        return new Engine.Index(
            SliceIdFieldMapper.encodeCompoundId(id, slice),
            doc,
            seqNo,
            primaryTerm.get(),
            version,
            null,
            Engine.Operation.Origin.REPLICA,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private Engine.Delete sliceDelete(String id, String slice, long seqNo, long version) {
        return new Engine.Delete(
            id,
            SliceIdFieldMapper.encodeCompoundId(id, slice),
            seqNo,
            primaryTerm.get(),
            version,
            null,
            Engine.Operation.Origin.REPLICA,
            System.nanoTime(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }
}
