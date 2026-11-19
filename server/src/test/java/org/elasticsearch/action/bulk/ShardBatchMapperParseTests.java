/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.Level;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private EngineBatch mapBatch(IndexShard shard, BulkItemRequest[] items, SourceBatch batch) {
        final BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch.schema(),
            shard.mapperService().mappingLookup(),
            shard.indexSettings()
        );
        if (resolution == null) {
            return null;
        }
        return releaseAfterTest(
            ShardBatchMapper.mapColumnBatch(
                items,
                batch,
                shard,
                0,
                items.length,
                resolution,
                Engine.Operation.Origin.PRIMARY,
                recycler,
                DocumentParsingProvider.EMPTY_INSTANCE
            )
        );
    }

    /**
     * Leak-tracking recycler, so that the leak check in {@code ESTestCase#after} covers every page the mappers take on the real
     * {@code mapColumnBatch} path, including the path that falls back after a mapper fails. A non-recycling recycler would hide a leak.
     */
    private final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));

    private final List<EngineBatch> engineBatches = new ArrayList<>();

    /** Remembers {@code engineBatch}, which may be {@code null} after a fallback, so that it is released when the test ends. */
    private EngineBatch releaseAfterTest(EngineBatch engineBatch) {
        if (engineBatch != null) {
            engineBatches.add(engineBatch);
        }
        return engineBatch;
    }

    /** Runs before {@code ESTestCase#after}, which fails the test if a page of the recycler is still out. */
    @After
    public void releaseEngineBatches() {
        Releasables.close(engineBatches);
        engineBatches.clear();
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
     * Verifies that {@code ignore_above} is a no-op in strictly columnar index modes. Values exceeding the limit
     * must be present in the binary doc-values column and must NOT appear in {@code _ignored}.
     */
    public void testIgnoreAboveIsNoOpOnKeywordInColumnar() throws IOException {
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
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "toolong")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                // LuceneBinaryColumn stores field names as BytesRef, so check binaryValue(), not stringValue().
                final BytesRef expected = new BytesRef("\u0001\u0008toolong");
                assertTrue(
                    "f binary DV should contain the value when ignore_above is a no-op",
                    fields.stream().anyMatch(fld -> "f".equals(fld.name()) && expected.equals(fld.binaryValue()))
                );
                assertFalse(
                    "_ignored should be absent when ignore_above is a no-op",
                    fields.stream().anyMatch(fld -> "_ignored".equals(fld.name()))
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
                EngineBatch chunk1 = releaseAfterTest(
                    ShardBatchMapper.mapColumnBatch(
                        items,
                        fullBatch,
                        shard,
                        0,
                        2,
                        resolution,
                        Engine.Operation.Origin.PRIMARY,
                        recycler,
                        DocumentParsingProvider.EMPTY_INSTANCE
                    )
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
                EngineBatch chunk2 = releaseAfterTest(
                    ShardBatchMapper.mapColumnBatch(
                        items,
                        fullBatch,
                        shard,
                        2,
                        4,
                        resolution,
                        Engine.Operation.Origin.PRIMARY,
                        recycler,
                        DocumentParsingProvider.EMPTY_INSTANCE
                    )
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

    /**
     * End-to-end through the real driver: a keyword field with a keyword multi-field takes the columnar path, and the sub-field's
     * Lucene fields are emitted alongside the parent's from the parent's own source column.
     */
    public void testMultiFieldSubFieldIsMappedFromTheParentColumn() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": {
                  "type": "keyword",
                  "fields": { "raw": { "type": "keyword", "ignore_above": 4 } }
                }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "abc"), doc("f", "abcdefgh")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed for a multi-field mapping", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 10L);
                mc.setSeqNo(1, 11L);
                mc.setVersion(0, 1L);
                mc.setVersion(1, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                List<IndexableField> fields = cursor.fields();
                assertTrue("parent field f should be present", fields.stream().anyMatch(f -> "f".equals(f.name())));
                assertTrue("sub-field f.raw should be present", fields.stream().anyMatch(f -> "f.raw".equals(f.name())));

                cursor.advance();
                fields = cursor.fields();
                assertTrue("parent field f should still be present", fields.stream().anyMatch(f -> "f".equals(f.name())));
                assertTrue(
                    "f.raw should be present even for over-limit values (ignore_above is a no-op in columnar mode)",
                    fields.stream().anyMatch(f -> "f.raw".equals(f.name()))
                );
                assertFalse(
                    "_ignored must not be populated in columnar mode (ignore_above is a no-op)",
                    fields.stream().anyMatch(f -> "_ignored".equals(f.name()))
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    public void testTextMultiFieldSubFieldIsMappedFromTheParentColumn() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": {
                  "type": "keyword",
                  "fields": { "txt": { "type": "text" } }
                }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "hello"), doc("f", "world")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed for a keyword+text multi-field mapping", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 10L);
                mc.setSeqNo(1, 11L);
                mc.setVersion(0, 1L);
                mc.setVersion(1, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                List<IndexableField> fields = cursor.fields();
                assertTrue("parent keyword field f should be present", fields.stream().anyMatch(f -> "f".equals(f.name())));
                assertTrue("text sub-field f.txt should be present", fields.stream().anyMatch(f -> "f.txt".equals(f.name())));

                cursor.advance();
                fields = cursor.fields();
                assertTrue("parent keyword field f should be present for doc2", fields.stream().anyMatch(f -> "f".equals(f.name())));
                assertTrue("text sub-field f.txt should be present for doc2", fields.stream().anyMatch(f -> "f.txt".equals(f.name())));
            }
        } finally {
            closeShards(shard);
        }
    }

    /** A sub-field whose mapper has no columnar support disqualifies the batch, which falls back to the row path. */
    public void testUnsupportedMultiFieldSubMapperFallsBack() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": {
                  "type": "keyword",
                  "fields": { "raw": { "type": "binary" } }
                }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "hello")), XContentType.JSON)) {
                assertNull("a binary sub-field must force a fallback", mapBatch(shard, items, batch));
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * A geo_point mapped as a group mapper can be used as a keyword multi-field, but the base class
     * gate (group mapper requires no multi-fields) causes a fallback before map time.
     */
    public void testGeoPointGroupMapperMultiFieldFallsBack() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": {
                  "type": "keyword",
                  "fields": { "geo": { "type": "geo_point" } }
                }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "hello")), XContentType.JSON)) {
                assertNull("a geo_point group-mapper sub-field must force a fallback", mapBatch(shard, items, batch));
            }
        } finally {
            closeShards(shard);
        }
    }

    /** Object-form geo_point documents are batched and indexed on the columnar path. */
    public void testGeoPointObjectBatchMode() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "loc": { "type": "geo_point" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            List<BytesReference> sources = List.of(
                new BytesArray("{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}"),
                new BytesArray("{\"loc\":{\"lat\":48.9,\"lon\":2.3}}")
            );
            try (SourceBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
                assertNotNull("object-form geo_point docs should be batched on the columnar path", mapBatch(shard, items, batch));
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * A geo_point spelled at its own path (string form) causes the columnar fast path to fall back
     * cleanly. The fallback must not emit a partial column and leave the batch in an inconsistent state.
     */
    public void testGeoPointOwnPathFallsBack() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "loc": { "type": "geo_point" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            List<BytesReference> sources = List.of(new BytesArray("{\"loc\":\"51.5,-0.1\"}"));
            try (SourceBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
                assertNull("a string-form geo_point must fall back from the columnar path", mapBatch(shard, items, batch));
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * When a {@code nullability=false, on_failure=FAIL} field is entirely absent from the ESCF
     * schema (the document simply omits it), the columnar path must fall back to sequential so the
     * sequential path can reject the document with a 4xx error.
     *
     * <p>The bug: the field's mapper is never invoked when its column is absent from the schema, so
     * no enforcement fires and the document is silently accepted. After the fix, the columnar path
     * detects the missing required field, throws {@code UnsupportedOperationException}, which is
     * caught by the fallback handler, and {@link #mapBatch} returns {@code null}.
     */
    public void testRequiredFieldAbsentFromSchemaCausesColumnarFallback() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": {
                  "type": "keyword",
                  "nullability": false
                }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            // Document omits "f" entirely — no column appears in the ESCF schema.
            try (SourceBatch batch = EscfEncoder.encode(List.of(new BytesArray("{}")), XContentType.JSON)) {
                // After the fix: null (columnar fallback → sequential rejects with 4xx).
                // Before the fix: non-null (silent 201, nullability constraint unenforced).
                assertNull(
                    "a nullability=false field absent from the ESCF schema must cause columnar fallback",
                    mapBatch(shard, items, batch)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * When a {@code nullability=false, on_failure=IGNORE} field is entirely absent from the ESCF
     * schema, the columnar path must add the field to {@code _ignored} for every document in the
     * batch rather than silently accepting the document without any indication.
     *
     * <p>The bug: the field's mapper is never invoked when its column is absent, so
     * {@code addIgnoredFieldColumnar} is never called, and {@code _ignored} stays empty.
     */
    public void testRequiredFieldAbsentFromSchemaAppearsInIgnoredOnIgnoreFailure() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": {
                  "type": "keyword",
                  "nullability": false, "on_failure": "ignore"
                }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(new BytesArray("{}")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("on_failure=IGNORE must not cause columnar fallback", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                // After the fix: _ignored contains an entry for field "f".
                // Before the fix: _ignored is absent (nullability constraint silently ignored).
                assertTrue(
                    "_ignored must be present when nullability=false on_failure=ignore and f is absent from the schema",
                    fields.stream().anyMatch(fld -> "_ignored".equals(fld.name()))
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * A multi-field of a {@code nullability=false} field is required as well, but has no source column of its own: it is mapped from its
     * parent's column. So it counts as present whenever the parent does, and the batch neither lists it in {@code _ignored} nor, under
     * {@code on_failure=FAIL}, falls back to the row path. When the parent is absent, the multi-field is missing too, as on the row path,
     * including when the parent itself is {@code nullability=true} and only the multi-field inherits the index-level {@code false}.
     */
    public void testRequiredMultiFieldIsPresentWhenItsParentIs() throws IOException {
        final boolean parentNullable = randomBoolean();
        final String mapping = Strings.format("""
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword", %s"fields": { "raw": { "type": "keyword" } } },
                "other": { "type": "keyword", "nullability": true }
              }
            }""", parentNullable ? "\"nullability\": true, " : "");
        final FieldMapper.DocValuesParameter.Values.OnFailure onFailure = randomFrom(
            FieldMapper.DocValuesParameter.Values.OnFailure.values()
        );
        final Settings settings = Settings.builder()
            .put(COLUMNAR_SETTINGS)
            .put(FieldMapper.DOC_VALUES_NULLABILITY_SETTING.getKey(), false)
            .put(FieldMapper.DOC_VALUES_ON_FAILURE_SETTING.getKey(), onFailure)
            .build();

        IndexShard shard = newShardWithMapping(mapping, settings);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", randomAlphanumericOfLength(5))), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("a required multi-field must count as present when its parent is", result);
                assertEquals(List.of(Set.of()), ignoredFieldsPerDoc(result));
            }
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("other", randomAlphanumericOfLength(5))), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                if (onFailure == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE) {
                    assertNotNull("on_failure=IGNORE must not cause columnar fallback", result);
                    assertEquals(List.of(parentNullable ? Set.of("f.raw") : Set.of("f", "f.raw")), ignoredFieldsPerDoc(result));
                } else {
                    assertNull("a required field absent from the ESCF schema must cause columnar fallback", result);
                }
            }
            // The parent's column exists but one document lacks it: the multi-field's own per-document check must catch that. Only checked
            // with a nullable parent, since a required parent fails its own check first and the fallback would hide which check fired.
            if (parentNullable) {
                final BulkItemRequest[] twoItems = {
                    new BulkItemRequest(0, indexRequest("doc1")),
                    new BulkItemRequest(1, indexRequest("doc2")) };
                final List<BytesReference> docs = List.of(
                    doc("f", randomAlphanumericOfLength(5)),
                    doc("other", randomAlphanumericOfLength(5))
                );
                try (SourceBatch batch = EscfEncoder.encode(docs, XContentType.JSON)) {
                    EngineBatch result = mapBatch(shard, twoItems, batch);
                    if (onFailure == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE) {
                        assertNotNull("on_failure=IGNORE must not cause columnar fallback", result);
                        assertEquals(List.of(Set.of(), Set.of("f.raw")), ignoredFieldsPerDoc(result));
                    } else {
                        assertNull("a document without a required multi-field's parent must cause columnar fallback", result);
                    }
                }
            }
        } finally {
            closeShards(shard);
        }
    }

    /** The field names each document of {@code result} lists in {@code _ignored}, in document order. */
    private static List<Set<String>> ignoredFieldsPerDoc(EngineBatch result) {
        final MappedColumns mc = result.columns();
        mc.fillPrimaryTerm(1L);
        for (int doc = 0; doc < mc.docCount(); doc++) {
            mc.setSeqNo(doc, doc);
            mc.setVersion(doc, 1L);
        }
        final MappedColumns.RowCursor cursor = mc.rowCursor();
        final List<Set<String>> ignored = new ArrayList<>();
        for (int doc = 0; doc < mc.docCount(); doc++) {
            cursor.advance();
            ignored.add(
                cursor.fields()
                    .stream()
                    .filter(fld -> "_ignored".equals(fld.name()))
                    .map(fld -> fld.binaryValue().utf8ToString())
                    .collect(Collectors.toSet())
            );
        }
        return ignored;
    }

    public void testSingleValuedColumnarKeywordBatchWritesTheBareValue() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword", "multi_value": false }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "hello")), XContentType.JSON)) {
                final EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                final IndexableField dvField = fields.stream()
                    .filter(fld -> "f".equals(fld.name()) && fld.fieldType().docValuesType() == DocValuesType.BINARY)
                    .findFirst()
                    .orElse(null);
                assertNotNull("keyword doc values for f should be present in batch output", dvField);
                assertEquals(new BytesRef("hello"), dvField.binaryValue());
                final Map<String, String> attributes = ((FieldType) dvField.fieldType()).getAttributes();
                assertTrue("the framing is not a field attribute", attributes == null || attributes.isEmpty());
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * A {@code nullability=false} field that is missing, null, or an array with no non-null value has no value, as on the sequential path.
     * Whatever the field's type, {@code FieldMapper.mapColumnBatch} records it in {@code _ignored} for that document only under
     * {@code on_failure=ignore}, keeping the batch columnar, as {@code testRequiredFieldAbsentFromSchemaAppearsInIgnoredOnIgnoreFailure}
     * does for a field no document has.
     */
    public void testRequiredFieldWithoutValueAppearsInIgnoredOnIgnoreFailure() throws IOException {
        assertRequiredFieldWithoutValue(true);
    }

    /** Under {@code on_failure=fail} the same document makes the batch fall back, so that the sequential path rejects it. */
    public void testRequiredFieldWithoutValueCausesColumnarFallback() throws IOException {
        assertRequiredFieldWithoutValue(false);
    }

    private void assertRequiredFieldWithoutValue(boolean ignore) throws IOException {
        final String type = randomFrom("keyword", "text", "ip", "long", "double");
        final boolean singleValued = type.equals("keyword") && randomBoolean();
        final String mapping = Strings.format("""
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "%s", "nullability": false, "on_failure": "%s", "multi_value": %s }
              }
            }""", type, ignore ? "ignore" : "fail", singleValued == false);
        // A number field refuses a column holding a null outright, as the encoder packs it as a UNION column, so only an empty array and a
        // missing field reach its check. "{}" is a document without the field.
        final boolean number = type.equals("long") || type.equals("double");
        final List<String> valuelessShapes = number
            ? List.of("{\"f\":[]}", "{}")
            : List.of("{\"f\":[]}", "{\"f\":[[]]}", "{\"f\":[null]}", "{\"f\":[null,null]}", "{\"f\":null}", "{}");
        // At least one document with values, so the column has the field's type, and at least one without.
        final int docCount = randomIntBetween(2, 6);
        final int valueDoc = randomIntBetween(0, docCount - 1);
        final boolean[] valueless = new boolean[docCount];
        final BytesReference[] sources = new BytesReference[docCount];
        final BulkItemRequest[] items = new BulkItemRequest[docCount];
        for (int doc = 0; doc < docCount; doc++) {
            valueless[doc] = doc == (valueDoc + 1) % docCount || (doc != valueDoc && randomBoolean());
            sources[doc] = valueless[doc] ? new BytesArray(randomFrom(valuelessShapes)) : arrayDoc(type, singleValued ? 1 : 2);
            items[doc] = new BulkItemRequest(doc, indexRequest("doc" + doc));
        }

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            try (SourceBatch batch = EscfEncoder.encode(List.of(sources), XContentType.JSON)) {
                if (ignore == false) {
                    // Any refusal makes the batch fall back the same way, so check that it is the nullability check that refused it: the
                    // one for a missing or null document if there is one, otherwise the one for the first document without a value.
                    String reason = null;
                    for (int doc = 0; doc < docCount && reason == null; doc++) {
                        final String source = sources[doc].utf8ToString();
                        if (source.equals("{}") || source.equals("{\"f\":null}")) {
                            reason = "has a null or absent value";
                        }
                    }
                    for (int doc = 0; doc < docCount && reason == null; doc++) {
                        if (valueless[doc]) {
                            reason = "has no value in document [" + doc + "]";
                        }
                    }
                    MockLog.assertThatLogger(
                        () -> assertNull(
                            "a nullability=false field without a value must cause columnar fallback under on_failure=fail",
                            mapBatch(shard, items, batch)
                        ),
                        ShardBatchMapper.class,
                        new MockLog.PatternAndExceptionSeenEventExpectation(
                            "nullability fallback",
                            ShardBatchMapper.class.getCanonicalName(),
                            Level.WARN,
                            "columnar batch mapping failed on .*",
                            UnsupportedOperationException.class,
                            "mapColumnBatch: nullability=false field [f] " + reason
                        )
                    );
                    return;
                }
                final EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("a nullability=false field without a value must not cause columnar fallback under on_failure=ignore", result);
                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                for (int doc = 0; doc < items.length; doc++) {
                    mc.setSeqNo(doc, doc);
                    mc.setVersion(doc, 1L);
                }
                final MappedColumns.RowCursor cursor = mc.rowCursor();
                for (int doc = 0; doc < items.length; doc++) {
                    cursor.advance();
                    assertEquals(
                        "_ignored for doc [" + doc + "] holding " + sources[doc].utf8ToString(),
                        valueless[doc],
                        cursor.fields().stream().anyMatch(fld -> "_ignored".equals(fld.name()))
                    );
                }
            }
        } finally {
            closeShards(shard);
        }
    }

    /** A document whose {@code f} is an array of {@code count} random values of {@code type}. */
    private static BytesReference arrayDoc(String type, int count) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().startArray("f");
            for (int i = 0; i < count; i++) {
                switch (type) {
                    case "keyword", "text" -> b.value(randomAlphanumericOfLength(5));
                    case "ip" -> b.value(NetworkAddress.format(randomIp(randomBoolean())));
                    // Elements of one width class (int, float), so the encoder packs them as an ARRAY column rather than a UNION.
                    case "long" -> b.value(randomInt());
                    case "double" -> b.value(randomIntBetween(-1000, 1000) + 0.5);
                    default -> throw new AssertionError("unexpected type [" + type + "]");
                }
            }
            b.endArray().endObject();
            return BytesReference.bytes(b);
        }
    }
}
