/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Verifies that ops-based recovery (the changes snapshot) correctly reconstructs slice-enabled ops
 */
public class SliceChangesSnapshotTests extends EngineTestCase {

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) // ops-based recovery requires soft-deletes
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.SLICE_VALIDATED.getKey(), true)
            .build();
    }

    public void testSliceDeleteRecoversCompoundUid() throws Exception {
        final String id = "doc-1";
        final String slice = "slice-7";
        // A delete is always recorded as a tombstone (even of an absent doc) so history/recovery can replay it. The engine
        // builds the tombstone from the compound term below and stores it (Store.YES) so the snapshot reads it back raw.
        final BytesRef compoundUid = Uid.encodeCompoundId(id, slice);
        engine.delete(
            new Engine.Delete(
                id,
                compoundUid,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                primaryTerm.get(),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            )
        );
        engine.refresh("test");

        try (
            Translog.Snapshot snapshot = engine.newChangesSnapshot(
                "test",
                0,
                Long.MAX_VALUE,
                false,
                randomBoolean(),
                randomBoolean(),
                ByteSizeValue.ofMb(32).getBytes()
            )
        ) {
            Translog.Operation op = snapshot.next();
            assertThat(op, instanceOf(Translog.Delete.class));
            Translog.Delete delete = (Translog.Delete) op;
            // The recovered Delete carries the compound term directly; replay deletes exactly the (slice, id) term.
            assertEquals(compoundUid, delete.uid());
            // The slice and the plain id are both recoverable from that term, so no separate routing is needed.
            assertEquals(id, Uid.decodeCompoundId(delete.uid()));
            assertEquals(slice, Uid.sliceFromCompoundId(delete.uid()));
            assertThat("only the single delete op should be present", snapshot.next(), nullValue());
        }
    }

    public void testSliceIndexRecoversCompoundUidAndRouting() throws Exception {
        final String id = "doc-1";
        final String slice = "slice-7";
        // Parse a live slice doc (preParse stores the compound _id plus the two indexed terms) and index it
        // under the compound identity term, mirroring the write path.
        ParsedDocument doc = parseDocument(engine.engineConfig.getMapperService(), id, slice);
        engine.index(new Engine.Index(Uid.encodeCompoundId(id, slice), primaryTerm.get(), doc));
        engine.refresh("test");

        try (
            Translog.Snapshot snapshot = engine.newChangesSnapshot(
                "test",
                0,
                Long.MAX_VALUE,
                false,
                randomBoolean(),
                randomBoolean(),
                ByteSizeValue.ofMb(32).getBytes()
            )
        ) {
            Translog.Operation op = snapshot.next();
            assertThat(op, instanceOf(Translog.Index.class));
            Translog.Index index = (Translog.Index) op;
            // The Index op carries the compound uid; replay decodes the plain id + routing from it.
            assertEquals(Uid.encodeCompoundId(id, slice), index.uid());
            assertEquals(id, Uid.decodeCompoundId(index.uid()));
            assertEquals(slice, Uid.sliceFromCompoundId(index.uid()));
            assertEquals(slice, index.routing());
            assertThat("only the single index op should be present", snapshot.next(), nullValue());
        }
    }
}
