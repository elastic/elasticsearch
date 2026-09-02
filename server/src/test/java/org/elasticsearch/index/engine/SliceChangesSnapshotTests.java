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
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SliceIdFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Verifies that ops-based recovery (the changes snapshot) correctly reconstructs slice-enabled ops
 */
public class SliceChangesSnapshotTests extends EngineTestCase {

    @BeforeClass
    public static void checkSliceFeatureFlag() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) // ops-based recovery requires soft-deletes
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .build();
    }

    public void testSliceDeleteRecoversCompoundUid() throws Exception {
        final String id = "doc-1";
        final String slice = "slice-7";
        // A delete is always recorded as a tombstone (even of an absent doc) so history/recovery can replay it. The engine
        // builds the tombstone from the compound term below and stores it (Store.YES) so the snapshot reads it back raw.
        final BytesRef compoundUid = SliceIdFieldMapper.encodeCompoundId(id, slice);
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
            assertEquals(id, SliceIdFieldMapper.decodeCompoundId(delete.uid()));
            assertEquals(slice, SliceIdFieldMapper.sliceFromCompoundId(delete.uid()));
            assertThat("only the single delete op should be present", snapshot.next(), nullValue());
        }
    }

    public void testSliceDeleteAmongSameIdDifferentSlicesRecoversCorrectSlice() throws Exception {
        final String id = "doc-1";
        final String deletedSlice = "slice-a";
        final String survivingSlice = "slice-b";
        // Two live docs share the plain id but live in different slices. Deleting one must recover a tombstone for that
        // slice's compound term - not the other's - proving the delete does not resolve the slice by looking up the id.
        var mapperService = engine.engineConfig.getMapperService();
        engine.index(
            new Engine.Index(
                SliceIdFieldMapper.encodeCompoundId(id, deletedSlice),
                primaryTerm.get(),
                parseDocument(mapperService, id, deletedSlice)
            )
        );
        engine.index(
            new Engine.Index(
                SliceIdFieldMapper.encodeCompoundId(id, survivingSlice),
                primaryTerm.get(),
                parseDocument(mapperService, id, survivingSlice)
            )
        );
        final BytesRef deletedUid = SliceIdFieldMapper.encodeCompoundId(id, deletedSlice);
        engine.delete(
            new Engine.Delete(
                id,
                deletedUid,
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
            Translog.Delete delete = null;
            boolean survivingIndexed = false;
            for (Translog.Operation op = snapshot.next(); op != null; op = snapshot.next()) {
                if (op instanceof Translog.Delete d) {
                    assertThat("expected a single delete op", delete, nullValue());
                    delete = d;
                } else if (op instanceof Translog.Index index
                    && SliceIdFieldMapper.encodeCompoundId(id, survivingSlice).equals(index.uid())) {
                        survivingIndexed = true;
                    }
            }
            assertNotNull("the delete op should be recovered", delete);
            // The delete carries the deleted slice's compound term, never the surviving slice's.
            assertEquals(deletedUid, delete.uid());
            assertEquals(deletedSlice, SliceIdFieldMapper.sliceFromCompoundId(delete.uid()));
            assertTrue("the surviving slice's index op must still be present", survivingIndexed);
        }
    }

    public void testSliceIndexRecoversCompoundUidAndRouting() throws Exception {
        final String id = "doc-1";
        final String slice = "slice-7";
        // Parse a live slice doc (preParse stores the compound _id plus the two indexed terms) and index it
        // under the compound identity term, mirroring the write path.
        ParsedDocument doc = parseDocument(engine.engineConfig.getMapperService(), id, slice);
        engine.index(new Engine.Index(SliceIdFieldMapper.encodeCompoundId(id, slice), primaryTerm.get(), doc));
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
            assertEquals(SliceIdFieldMapper.encodeCompoundId(id, slice), index.uid());
            assertEquals(id, SliceIdFieldMapper.decodeCompoundId(index.uid()));
            assertEquals(slice, SliceIdFieldMapper.sliceFromCompoundId(index.uid()));
            assertEquals(slice, index.routing());
            assertThat("only the single index op should be present", snapshot.next(), nullValue());
        }
    }
}
