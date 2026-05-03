/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Parse-time tests for the batch-mapping fast path: drives {@link ShardBatchMapper} directly and
 * verifies the resulting {@link LuceneDocument} fields carry the expected values. Engine indexing
 * is intentionally not exercised here — those interactions are covered by {@code ShardBatchIndexer}
 * tests; this file's job is to lock down the mapper's per-row parsing contract.
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    private static final Settings SYNTHETIC_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        "index.mapping.source.mode",
        "synthetic"
    ).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    private IndexShard newShardWithMapping(String mapping) throws IOException {
        return newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);
    }

    private IndexShard newShardWithMapping(String mapping, Settings settings) throws IOException {
        IndexMetadata md = IndexMetadata.builder("index").putMapping(mapping).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(md.getIndex(), 0), true, "n1", md, null);
        recoverShardFromStore(shard);
        return shard;
    }

    private static IndexRequest indexRequest(String id) {
        return new IndexRequest("index").id(id);
    }

    private List<Engine.Index> parseBatch(IndexShard shard, EirfBatch batch, BulkItemRequest[] items) throws IOException {
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
        assertNotNull("expected batch path to support this mapping", resolution);
        List<Engine.Index> ops = ShardBatchMapper.parseMappings(items, batch, shard, items.length, 0, resolution);
        assertNotNull("parseMappings returned null (fallback signal)", ops);
        assertThat(ops, hasSize(items.length));
        return ops;
    }

    private static LuceneDocument rootDoc(Engine.Index op) {
        return op.parsedDoc().rootDoc();
    }

    public void testSupportedMapperTypes() throws Exception {
        String mapping = """
            {
              "properties": {
                "ts":    { "type": "date" },
                "host":  { "type": "keyword" },
                "value": { "type": "long" },
                "score": { "type": "double" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        int numDocs = 5;
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest("id-" + i));
        }

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            for (int i = 0; i < numDocs; i++) {
                builder.startDocument();
                builder.setLong("ts", 1_700_000_000_000L + i);
                builder.setString("host", "host-" + i);
                builder.setLong("value", 100L + i);
                builder.setDouble("score", 1.5 + i);
                builder.endDocument();
            }
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                for (int i = 0; i < numDocs; i++) {
                    LuceneDocument doc = rootDoc(ops.get(i));
                    assertThat(doc.getField("ts").numericValue().longValue(), equalTo(1_700_000_000_000L + i));
                    assertThat(doc.getBinaryValue("host"), equalTo(new BytesRef("host-" + i)));
                    assertThat(doc.getField("value").numericValue().longValue(), equalTo(100L + i));
                    // Double doc-values field stores Double.doubleToLongBits(value); decode for assertion.
                    long scoreBits = doc.getField("score").numericValue().longValue();
                    assertThat(Double.longBitsToDouble(scoreBits), equalTo(1.5 + i));
                }
            }
        }

        closeShards(shard);
    }

    public void testResolveFallsBackForUnsupportedMapper() throws Exception {
        // A text field with index_prefixes is not in the v1 support list — resolveMappers should
        // return null so the caller falls back to the sequential path.
        String mapping = """
            {
              "properties": {
                "host": { "type": "keyword" },
                "body": { "type": "text", "index_prefixes": {} }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("host", "a");
            builder.setString("body", "hello world");
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
                assertNull("batch path should refuse text+index_prefixes leaves and trigger sequential fallback", resolution);
            }
        }

        closeShards(shard);
    }

    public void testIgnoredLeafUnderDynamicFalseParentIsDropped() throws Exception {
        String mapping = """
            {
              "dynamic": "false",
              "properties": {
                "host":  { "type": "keyword" }
              }
            }""";
        // Stored source: with synthetic source the non-batch path would route the unmapped `extra`
        // through _ignored_source, which the batch path does not implement yet.
        IndexShard shard = newShardWithMapping(mapping, STORED_SOURCE_SETTINGS);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("host", "alpha");
            builder.setString("extra", "silent"); // unmapped under dynamic=false parent → ignored
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
                assertNotNull(resolution);
                int hostLeaf = batch.schema().findLeaf("host", 0);
                int extraLeaf = batch.schema().findLeaf("extra", 0);
                assertNotNull(resolution.columnMappers()[hostLeaf]);
                assertNull(resolution.columnMappers()[extraLeaf]);

                List<Engine.Index> ops = ShardBatchMapper.parseMappings(items, batch, shard, items.length, 0, resolution);
                assertNotNull(ops);
                assertThat(ops, hasSize(1));
                LuceneDocument doc = rootDoc(ops.get(0));
                assertThat(doc.getBinaryValue("host"), equalTo(new BytesRef("alpha")));
                assertThat(doc.getFields("extra"), empty());
            }
        }

        closeShards(shard);
    }

    public void testNumberMapperReceivesStringValue() throws Exception {
        // The contract is: NumberFieldMapper should parse string EIRF values via its usual
        // parseCreateField path; resolveMappers does not pre-match column EIRF types.
        String mapping = """
            {
              "properties": {
                "v": { "type": "long" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("v", "42"); // string into a long-typed mapper
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                LuceneDocument doc = rootDoc(ops.get(0));
                assertThat(doc.getField("v").numericValue().longValue(), equalTo(42L));
            }
        }

        closeShards(shard);
    }

    public void testIgnoreAboveOnKeywordDoesNotFail() throws Exception {
        String mapping = """
            {
              "properties": {
                "host": { "type": "keyword", "ignore_above": 4 }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("host", "way-too-long-value"); // exceeds ignore_above
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                LuceneDocument doc = rootDoc(ops.get(0));
                assertThat(doc.getFields("host"), empty());
                assertTrue(
                    "ignore_above should record the field name in _ignored",
                    doc.getFields("_ignored").stream().anyMatch(f -> "host".equals(f.stringValue()))
                );
            }
        }

        closeShards(shard);
    }

    /**
     * Drives {@link ShardBatchMapper#parseMappings} directly (no fallback safety net) and inspects
     * the {@link ParsedDocument} produced by {@code parseRow}. Each metadata-mapper hook that the
     * batch path relies on contributes a Lucene field by a known name, so checking for those names
     * tells us preParse/postParse actually ran. If the metadata phases are skipped, parseRow either
     * NPEs (no _id) or emits a doc missing _routing/_source/_version/_seq_no — both make this test fail.
     */
    public void testParseMappingsAddsMetadataFields() throws Exception {
        String mapping = """
            {
              "properties": {
                "host":  { "type": "keyword" },
                "value": { "type": "long" }
              }
            }""";
        // Use stored source so SourceFieldMapper.preParse adds a _source StoredField we can inspect.
        IndexShard shard = newShardWithMapping(mapping, STORED_SOURCE_SETTINGS);

        IndexRequest indexRequest = indexRequest("doc-1").routing("route-1");
        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest) };

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("host", "alpha");
            builder.setLong("value", 7L);
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                ParsedDocument parsed = ops.getFirst().parsedDoc();
                LuceneDocument doc = parsed.rootDoc();

                // ProvidedIdFieldMapper.preParse — sets context.id and adds an indexed _id field.
                assertThat("ParsedDocument id should be populated by IdFieldMapper.preParse", parsed.id(), equalTo("doc-1"));
                assertNotNull("_id Lucene field must be present", doc.getField(IdFieldMapper.NAME));

                // RoutingFieldMapper.preParse — adds a stored _routing field whose string value is the routing key.
                IndexableField routingField = doc.getField(RoutingFieldMapper.NAME);
                assertNotNull("_routing must be added by RoutingFieldMapper.preParse", routingField);
                assertThat(routingField.stringValue(), equalTo("route-1"));

                // VersionFieldMapper.preParse — adds a placeholder _version doc-values field.
                assertNotNull("_version must be added by VersionFieldMapper.preParse", doc.getField(VersionFieldMapper.NAME));

                // SeqNoFieldMapper.postParse — adds the _seq_no placeholder doc-values field.
                assertNotNull("_seq_no must be added by SeqNoFieldMapper.postParse", doc.getField(SeqNoFieldMapper.NAME));

                // SourceFieldMapper.preParse — adds the _source stored field with the row-synthesized JSON.
                IndexableField sourceField = doc.getField(SourceFieldMapper.NAME);
                assertNotNull("_source must be added by SourceFieldMapper.preParse", sourceField);
                assertNotNull(sourceField.binaryValue());
            }
        }

        closeShards(shard);
    }

    /**
     * Synthetic-source variant of the metadata-field audit. Combines coverage for:
     * <ul>
     *   <li>{@link SourceFieldMapper#preParse} (synthetic branch — adds {@code _recovery_source}
     *       or {@code _recovery_source_size} and does <em>not</em> add a stored {@code _source})</li>
     *   <li>{@link IgnoredFieldMapper#postParse} (records {@code _ignored} for fields that hit
     *       {@code ignore_above})</li>
     * </ul>
     *
     * <p>Note that {@code IgnoredSourceFieldMapper.postParse} is not exercised by an
     * {@code ignore_above} trigger: {@code KeywordFieldMapper} stores the ignored bytes directly
     * into a synthetic-source fallback field rather than via
     * {@code DocumentParserContext#addIgnoredFieldValue}, so {@code getIgnoredFieldValues()}
     * stays empty and the postParse hook is a no-op for this case. Exercising it would require
     * an {@code ignore_malformed} trigger on a numeric column.
     */
    public void testParseMappingsSyntheticSourceAndIgnored() throws Exception {
        String mapping = """
            {
              "properties": {
                "host": { "type": "keyword", "ignore_above": 4 }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("doc-1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("host", "way-too-long"); // exceeds ignore_above
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                LuceneDocument doc = rootDoc(ops.getFirst());

                // Synthetic source: stored _source is absent, but a _recovery_source flavor must be present.
                assertNull("stored _source must not be present in synthetic mode", doc.getField(SourceFieldMapper.NAME));
                boolean hasRecovery = doc.getField(SourceFieldMapper.RECOVERY_SOURCE_NAME) != null
                    || doc.getField(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME) != null;
                assertTrue(
                    "SourceFieldMapper.preParse must add either _recovery_source or _recovery_source_size in synthetic mode",
                    hasRecovery
                );

                // ignore_above triggered → IgnoredFieldMapper.postParse records the field name.
                assertNotNull("_ignored must be added when a column exceeds ignore_above", doc.getField(IgnoredFieldMapper.NAME));
            }
        }

        closeShards(shard);
    }

    public void testNullValuesAreSkipped() throws Exception {
        String mapping = """
            {
              "properties": {
                "host": { "type": "keyword" },
                "v":    { "type": "long" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setNull("host");
            builder.setNull("v");
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                LuceneDocument doc = rootDoc(ops.get(0));
                assertThat(doc.getFields("host"), empty());
                assertThat(doc.getFields("v"), empty());
            }
        }

        closeShards(shard);
    }

    public void testBooleanMapper() throws Exception {
        String mapping = """
            {
              "properties": {
                "active": { "type": "boolean" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        int numDocs = 3;
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest("id-" + i));
        }
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setBoolean("active", true);
            builder.endDocument();
            builder.startDocument();
            builder.setBoolean("active", false);
            builder.endDocument();
            builder.startDocument();
            builder.setNull("active");
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);

                LuceneDocument trueDoc = rootDoc(ops.get(0));
                List<IndexableField> trueFields = trueDoc.getFields("active");
                assertFalse("expected indexable fields for true", trueFields.isEmpty());
                assertTrue(
                    "boolean true should encode as numeric 1",
                    trueFields.stream().anyMatch(f -> f.numericValue() != null && f.numericValue().longValue() == 1L)
                );

                LuceneDocument falseDoc = rootDoc(ops.get(1));
                List<IndexableField> falseFields = falseDoc.getFields("active");
                assertFalse("expected indexable fields for false", falseFields.isEmpty());
                assertTrue(
                    "boolean false should encode as numeric 0",
                    falseFields.stream().anyMatch(f -> f.numericValue() != null && f.numericValue().longValue() == 0L)
                );

                LuceneDocument nullDoc = rootDoc(ops.get(2));
                assertThat(nullDoc.getFields("active"), empty());
            }
        }

        closeShards(shard);
    }

    public void testIpMapper() throws Exception {
        String mapping = """
            {
              "properties": {
                "addr": { "type": "ip" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        int numDocs = 2;
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest("id-" + i));
        }
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("addr", "192.168.1.1");
            builder.endDocument();
            builder.startDocument();
            builder.setString("addr", "::1");
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);

                BytesRef expectedV4 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.1")));
                BytesRef expectedV6 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1")));
                assertThat(rootDoc(ops.get(0)).getField("addr").binaryValue(), equalTo(expectedV4));
                assertThat(rootDoc(ops.get(1)).getField("addr").binaryValue(), equalTo(expectedV6));
            }
        }

        closeShards(shard);
    }

    public void testIpMapperIgnoreMalformed() throws Exception {
        String mapping = """
            {
              "properties": {
                "addr": { "type": "ip", "ignore_malformed": true }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, STORED_SOURCE_SETTINGS);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("addr", "not-an-ip");
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                LuceneDocument doc = rootDoc(ops.get(0));
                assertThat(doc.getFields("addr"), empty());
                assertTrue(
                    "ignore_malformed should record the field name in _ignored",
                    doc.getFields("_ignored").stream().anyMatch(f -> "addr".equals(f.stringValue()))
                );
            }
        }

        closeShards(shard);
    }

    public void testTextMapper() throws Exception {
        String mapping = """
            {
              "properties": {
                "body": { "type": "text" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, STORED_SOURCE_SETTINGS);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("body", "the quick brown fox");
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                List<Engine.Index> ops = parseBatch(shard, batch, items);
                LuceneDocument doc = rootDoc(ops.get(0));
                assertThat(doc.getField("body").stringValue(), equalTo("the quick brown fox"));
            }
        }

        closeShards(shard);
    }
}
