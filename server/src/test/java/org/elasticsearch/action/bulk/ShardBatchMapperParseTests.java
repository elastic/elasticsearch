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
import org.elasticsearch.transport.BytesRefRecycler;
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
        return ShardBatchMapper.mapColumnBatch(
            items,
            batch,
            shard,
            0,
            items.length,
            resolution,
            Engine.Operation.Origin.PRIMARY,
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
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

    private static final String FLATTENED_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "flat": { "type": "flattened" }
          }
        }""";

    /**
     * A flattened field with two keys produces the {@code _keyed} binary DV column and a {@code counts}
     * column. Verifies the columnar path is taken (non-null result) and that the output columns are present.
     */
    public void testFlattenedFieldProducesKeyedColumns() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (
                SourceBatch batch = EscfEncoder.encode(List.of(new BytesArray("{\"flat\":{\"k1\":\"a\",\"k2\":\"b\"}}")), XContentType.JSON)
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed for flattened field", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                // flat._keyed binary DV must be present.
                assertTrue(
                    "flat._keyed binary DV should be present",
                    fields.stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                );
                // flat._keyed.counts numeric DV must be present (2 slots: k1 and k2).
                assertTrue(
                    "flat._keyed.counts should be present",
                    fields.stream().anyMatch(f -> "flat._keyed.counts".equals(f.name()) && f.numericValue() != null)
                );
                final long counts = fields.stream()
                    .filter(f -> "flat._keyed.counts".equals(f.name()))
                    .mapToLong(f -> f.numericValue().longValue())
                    .findFirst()
                    .orElseThrow();
                assertEquals("expected 2 slots (k1 + k2)", 2L, counts);
            }
        } finally {
            closeShards(shard);
        }
    }

    /** A flattened field and a keyword field in the same batch both take the columnar path. */
    public void testFlattenedAndKeywordInSameBatch() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "host": { "type": "keyword" },
                "attrs": { "type": "flattened" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(new BytesArray("{\"host\":\"srv\",\"attrs\":{\"env\":\"prod\"}}")),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path for flattened + keyword combo", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                assertTrue("host keyword DV should be present", fields.stream().anyMatch(f -> "host".equals(f.name())));
                assertTrue(
                    "attrs._keyed should be present",
                    fields.stream().anyMatch(f -> "attrs._keyed".equals(f.name()) && f.binaryValue() != null)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /** An explicit null for a flattened field (no sub-keys) emits no _keyed column. */
    public void testFlattenedNullValueEmitsNoKeyedColumn() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(new BytesArray("{\"flat\":null}")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed for null flattened field", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                assertFalse(
                    "flat._keyed should be absent for an explicit null",
                    fields.stream().anyMatch(f -> "flat._keyed".equals(f.name()))
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /** A batch mixing a null doc and a doc with real keys: null doc produces no _keyed, keyed doc does. */
    public void testFlattenedMixedNullAndKeyedDocInSameBatch() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(new BytesArray("{\"flat\":null}"), new BytesArray("{\"flat\":{\"k\":\"v\"}}")),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull(result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setSeqNo(1, 2L);
                mc.setVersion(0, 1L);
                mc.setVersion(1, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();

                // Doc 0: null — no _keyed DV.
                cursor.advance();
                assertFalse(
                    "doc0 (null): flat._keyed should be absent",
                    cursor.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                );

                // Doc 1: has a key — _keyed DV present.
                cursor.advance();
                assertTrue(
                    "doc1 (keyed): flat._keyed should be present",
                    cursor.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * Verifies that leaf indexes computed once by {@code resolveMappers} are stable across chunks
     * produced by {@code EscfBatch#slice}. The test encodes 4 documents, resolves once, then maps
     * in two slices — [0,2) and [2,4). Both slices must produce the correct _keyed output.
     */
    public void testFlattenedGroupAcrossChunks() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = {
                new BulkItemRequest(0, indexRequest("d0")),
                new BulkItemRequest(1, indexRequest("d1")),
                new BulkItemRequest(2, indexRequest("d2")),
                new BulkItemRequest(3, indexRequest("d3")) };
            try (
                SourceBatch fullBatch = EscfEncoder.encode(
                    List.of(
                        new BytesArray("{\"flat\":{\"a\":\"1\"}}"),
                        new BytesArray("{\"flat\":{\"a\":\"2\"}}"),
                        new BytesArray("{\"flat\":{\"b\":\"3\"}}"),
                        new BytesArray("{\"flat\":{\"b\":\"4\"}}")
                    ),
                    XContentType.JSON
                )
            ) {
                final BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
                    fullBatch.schema(),
                    shard.mapperService().mappingLookup(),
                    shard.indexSettings()
                );
                assertNotNull("resolution must succeed for a plain flattened field", resolution);
                assertEquals("should have one group", 1, resolution.columnGroups().length);

                // First chunk [0, 2).
                EngineBatch chunk1 = ShardBatchMapper.mapColumnBatch(
                    items,
                    fullBatch,
                    shard,
                    0,
                    2,
                    resolution,
                    Engine.Operation.Origin.PRIMARY,
                    BytesRefRecycler.NON_RECYCLING_INSTANCE
                );
                assertNotNull("chunk1 mapping should succeed", chunk1);
                chunk1.columns().fillPrimaryTerm(1L);
                chunk1.columns().setSeqNo(0, 1L);
                chunk1.columns().setSeqNo(1, 2L);
                chunk1.columns().setVersion(0, 1L);
                chunk1.columns().setVersion(1, 1L);
                MappedColumns.RowCursor c1 = chunk1.columns().rowCursor();
                c1.advance();
                assertTrue("chunk1 doc0: flat._keyed present", c1.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name())));
                c1.advance();
                assertTrue("chunk1 doc1: flat._keyed present", c1.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name())));

                // Second chunk [2, 4).
                EngineBatch chunk2 = ShardBatchMapper.mapColumnBatch(
                    items,
                    fullBatch,
                    shard,
                    2,
                    4,
                    resolution,
                    Engine.Operation.Origin.PRIMARY,
                    BytesRefRecycler.NON_RECYCLING_INSTANCE
                );
                assertNotNull("chunk2 mapping should succeed", chunk2);
                chunk2.columns().fillPrimaryTerm(1L);
                chunk2.columns().setSeqNo(0, 3L);
                chunk2.columns().setSeqNo(1, 4L);
                chunk2.columns().setVersion(0, 1L);
                chunk2.columns().setVersion(1, 1L);
                MappedColumns.RowCursor c2 = chunk2.columns().rowCursor();
                c2.advance();
                assertTrue("chunk2 doc2: flat._keyed present", c2.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name())));
                c2.advance();
                assertTrue("chunk2 doc3: flat._keyed present", c2.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name())));
            }
        } finally {
            closeShards(shard);
        }
    }
}
