/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Peer recovery and CCR replay operation history from the recovery source. With synthetic recovery source (the default, and mandatory for
 * {@code columnar_stored}) a past operation's {@code _source} is rebuilt from the current doc values. An in-place doc-values update patches
 * the live document, so an earlier operation must still reconstruct with its own value, not the update's: otherwise a copy that replays
 * that operation adopts a value that may not be durable yet, and diverges permanently if the update is lost on a failover. The engine keeps
 * this correct by capturing the field's pre-image on its first in-place update (see {@code DocValuesUpdatePreImage}).
 */
public class DocValuesUpdateRecoverySourceTests extends IndexShardTestCase {

    private static final String MAPPING = """
        { "properties": { "count": { "type": "long", "index": false, "doc_values": { "updatable": true } } } }
        """;

    /**
     * Reconstructing an operation from the changes snapshot (as peer recovery and CCR do) yields that operation's own value, not the value
     * of a later in-place update to the same document.
     */
    public void testRecoverySourceReconstructsOperationValueNotLaterInPlaceUpdate() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final IndexShard primary = newRecoveredColumnarPrimary();
        try {
            // Base operation: index count=1 at sequence number 0.
            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{\"count\":1}").getSeqNo();

            // In-place update of the same document to count=42: a new operation at sequence number 1 that patches the live document.
            applyUpdate(primary, 1L, "1", 42);
            primary.refresh("test");

            // The base operation indexed count=1; reconstructing it must not reflect the later in-place update to 42.
            assertThat(reconstructCount(primary, baseSeqNo), equalTo(1L));
        } finally {
            closeShards(primary);
        }
    }

    /**
     * The pre-image is the field's value before the <em>first</em> update, so repeated in-place updates still reconstruct the base
     * operation with the original value rather than an intermediate one.
     */
    public void testRecoverySourceReconstructsOriginalValueAfterRepeatedUpdates() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final IndexShard primary = newRecoveredColumnarPrimary();
        try {
            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{\"count\":1}").getSeqNo();
            applyUpdate(primary, 1L, "1", 42);
            applyUpdate(primary, 2L, "1", 99);
            primary.refresh("test");

            // The pre-image holds the value before the first update, so the base still reconstructs as 1 - not 42 (the value the second
            // update overwrote) and not 99 (the current value).
            assertThat(reconstructCount(primary, baseSeqNo), equalTo(1L));
        } finally {
            closeShards(primary);
        }
    }

    /**
     * When the base document had no value for the field, the first update captures an empty pre-image: reconstructing the base operation
     * must then produce a document without the field, not one carrying the updated value.
     */
    public void testRecoverySourceReconstructsAbsentValueWhenBaseHadNoValue() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final IndexShard primary = newRecoveredColumnarPrimary();
        try {
            // Base operation has no value for count.
            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{}").getSeqNo();
            applyUpdate(primary, 1L, "1", 42);
            primary.refresh("test");

            final Map<String, Object> source = reconstructSource(primary, baseSeqNo);
            assertThat("the base document had no count, so its reconstruction must not carry one", source.keySet(), not(hasItem("count")));
        } finally {
            closeShards(primary);
        }
    }

    /**
     * columnar_stored keeps a fixed index-time source blob but overlays each updatable field's current doc values on read, so
     * reconstructing a past operation must still resolve to the field's pre-image rather than the overlaid current value, exactly as for
     * pure synthetic source.
     */
    public void testColumnarStoredRecoveryReconstructsOriginalValueAfterInPlaceUpdate() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());

        final IndexShard primary = newRecoveredColumnarPrimary("columnar_stored");
        try {
            final long baseSeqNo = indexDoc(primary, "_doc", "1", "{\"count\":1}").getSeqNo();
            applyUpdate(primary, 1L, "1", 42);
            primary.refresh("test");

            // The stored blob holds count=1 and the overlay would otherwise splice in the current value (42); the captured pre-image keeps
            // the base operation reconstructing as 1.
            assertThat(reconstructCount(primary, baseSeqNo), equalTo(1L));
        } finally {
            closeShards(primary);
        }
    }

    private IndexShard newRecoveredColumnarPrimary() throws Exception {
        return newRecoveredColumnarPrimary(null);
    }

    private IndexShard newRecoveredColumnarPrimary(String sourceMode) throws Exception {
        final ShardId shardId = new ShardId("index", "_na_", 0);
        final Settings.Builder settings = indexSettings(IndexVersion.current(), 1, 0).put(
            IndexSettings.MODE.getKey(),
            IndexMode.COLUMNAR.getName()
        )
            // updatable fields need sequence numbers, which columnar disables by default
            .put("index.disable_sequence_numbers", false)
            // reconstruct operation history from doc values rather than a stored recovery source, as peer recovery and CCR do
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
        if (sourceMode != null) {
            settings.put("index.mapping.source.mode", sourceMode);
        }
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings.build())
            .putMapping(MAPPING)
            .primaryTerm(0, 1)
            .build();
        final IndexShard primary = newShard(shardId, true, "primary-node", indexMetadata, null);
        recoverShardFromStore(primary);
        return primary;
    }

    private static void applyUpdate(IndexShard primary, long version, String id, long value) throws IOException {
        final Engine.DocValuesUpdateResult result = primary.applyDocValuesUpdateOnPrimary(
            version,
            id,
            List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("count", value)),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
        assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
    }

    private static long reconstructCount(IndexShard shard, long seqNo) throws IOException {
        final Map<String, Object> source = reconstructSource(shard, seqNo);
        assertThat("reconstructed operation [" + seqNo + "] is missing a count", source.get("count"), notNullValue());
        return ((Number) source.get("count")).longValue();
    }

    /** Reads the operation at {@code seqNo} back from the operation history, exactly as peer recovery and CCR do, returning its source. */
    private static Map<String, Object> reconstructSource(IndexShard shard, long seqNo) throws IOException {
        try (
            Translog.Snapshot snapshot = shard.newChangesSnapshot("test", seqNo, seqNo, true, true, false, ByteSizeValue.ofMb(1).getBytes())
        ) {
            final Translog.Operation op = snapshot.next();
            assertThat(op, notNullValue());
            assertThat(op, instanceOf(Translog.Index.class));
            final Translog.Index indexOp = (Translog.Index) op;
            assertThat(indexOp.seqNo(), equalTo(seqNo));
            return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, indexOp.source().streamInput()).map();
        }
    }
}
