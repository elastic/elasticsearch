/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Parse-time tests for the batch-mapping fast path: drives {@link ShardBatchMapper} directly and
 * verifies the resulting columnar output. Engine indexing is intentionally not exercised here —
 * those interactions are covered by {@code ShardBatchIndexer} tests; this file's job is to lock
 * down the mapper's columnar parsing contract.
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    /**
     * COLUMNAR mode with synthetic recovery source. Synthetic recovery satisfies
     * {@link org.elasticsearch.index.mapper.SourceFieldMapper#supportsColumnarParse} (only a size
     * estimate is stored, not the full source), while keeping recovery source enabled so that
     * {@code RecoverySourceHandler} can open a changes snapshot for replica recovery.
     */
    private static final Settings COLUMNAR_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        IndexSettings.MODE.getKey(),
        IndexMode.COLUMNAR.getName()
    ).put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    private IndexShard newShardWithMapping(String mapping, Settings settings) throws IOException {
        IndexMetadata md = IndexMetadata.builder("index").putMapping(mapping).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(md.getIndex(), 0), true, "n1", md, null);
        recoverShardFromStore(shard);
        return shard;
    }

    private static IndexRequest indexRequest(String id) {
        return new IndexRequest("index").id(id);
    }

    /** Builds a single-document JSON {@link BytesReference} from alternating name/value pairs. */
    private static BytesReference doc(Object... kvPairs) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            for (int i = 0; i < kvPairs.length; i += 2) {
                b.field((String) kvPairs[i], kvPairs[i + 1]);
            }
            b.endObject();
            return BytesReference.bytes(b);
        }
    }

    /**
     * Calls {@code resolveMappers} then {@code mapColumnBatch} over the full batch (no chunking).
     * Returns the {@link EngineBatch}, or {@code null} if the columnar path was not taken.
     */
    private static EngineBatch mapBatch(IndexShard shard, BulkItemRequest[] items, SourceBatch batch) {
        final BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch.schema(),
            shard.mapperService().mappingLookup(),
            shard.indexSettings()
        );
        if (resolution == null) {
            return null;
        }
        return ShardBatchMapper.mapColumnBatch(items, batch, shard, 0, items.length, resolution, Engine.Operation.Origin.PRIMARY);
    }

    /**
     * Verifies that {@code mapColumnBatch} produces a non-null result for a simple keyword mapping
     * in COLUMNAR mode, confirming the columnar path is taken end-to-end.
     */
    public void testParseMappingsAddsMetadataFields() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "hello")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 10L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                assertTrue("_id should be present", fields.stream().anyMatch(f -> "_id".equals(f.name())));
                assertTrue("_seq_no should be present", fields.stream().anyMatch(f -> "_seq_no".equals(f.name())));
                assertTrue("_primary_term should be present", fields.stream().anyMatch(f -> "_primary_term".equals(f.name())));
                assertTrue("_version should be present", fields.stream().anyMatch(f -> "_version".equals(f.name())));
                // Keyword field "f" should also appear in the binary doc-values column.
                assertTrue("keyword field f should be present", fields.stream().anyMatch(f -> "f".equals(f.name())));
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * Verifies that a keyword value exceeding {@code ignore_above} does not crash the columnar path
     * and causes the field name to appear in the {@code _ignored} column.
     */
    public void testIgnoreAboveOnKeywordDoesNotFail() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword", "ignore_above": 5 }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            // "toolong" is 7 chars, exceeds ignore_above=5.
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "toolong")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed with ignore_above exceeded", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                // LuceneBinaryColumn stores field names as BytesRef, so check binaryValue(), not stringValue().
                final BytesRef fRef = new BytesRef("f");
                assertTrue(
                    "_ignored should contain field name f",
                    fields.stream().anyMatch(fld -> "_ignored".equals(fld.name()) && fRef.equals(fld.binaryValue()))
                );
                // The ignored value should not land in the binary doc-values column.
                assertFalse(
                    "f binary DV should be absent when value exceeds ignore_above",
                    fields.stream().anyMatch(fld -> "f".equals(fld.name()) && fld.binaryValue() != null)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * Verifies that an explicit JSON null for a keyword field produces a null slot in the
     * doc-values encoding — no term, no binary value, and no crash.
     */
    public void testNullValuesAreSkipped() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(new BytesArray("{\"f\":\"hello\"}"), new BytesArray("{\"f\":null}")),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                for (int i = 0; i < 2; i++) {
                    mc.setSeqNo(i, i + 1L);
                    mc.setVersion(i, 1L);
                }

                final MappedColumns.RowCursor cursor = mc.rowCursor();

                // Doc 0 has a real value — binary DV for "f" should be present.
                cursor.advance();
                List<IndexableField> doc0Fields = cursor.fields();
                assertTrue(
                    "doc0: f binary DV should be present for non-null value",
                    doc0Fields.stream().anyMatch(fld -> "f".equals(fld.name()) && fld.binaryValue() != null)
                );

                // Doc 1 has an explicit null — no binary DV blob for "f" (null slot, no value).
                cursor.advance();
                List<IndexableField> doc1Fields = cursor.fields();
                assertFalse(
                    "doc1: f binary DV should be absent for explicit null",
                    doc1Fields.stream().anyMatch(fld -> "f".equals(fld.name()) && fld.binaryValue() != null)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    // TODO(columnar): bring back once the corresponding field mappers support columnar parsing:
    // - testSupportedMapperTypes (date, long, double — keyword already covered above)
    // - testNumberMapperReceivesStringValue (long/double with a string source value)
    // - testParseMappingsSyntheticSourceAndIgnored
    // - testBooleanMapper
    // - testIpMapper
    // - testIpMapperIgnoreMalformed
    // - testTextMapper
}
