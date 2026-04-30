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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end tests for the batch-mapping fast path: verifies {@link ShardBatchMapper} resolves
 * mappers, builds valid {@link org.elasticsearch.index.engine.Engine.Index} operations, and the
 * resulting documents end up in Lucene with the expected field names.
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    private static final Settings SYNTHETIC_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        "index.mapping.source.mode",
        "synthetic"
    ).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    private static final ShardId SHARD_ID = new ShardId("index", "_na_", 0);

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

    public void testSupportedMapperTypesIndexEndToEnd() throws Exception {
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
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
                BulkPrimaryExecutionContext ctx = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, ctx, future);
                future.actionGet();

                assertFalse(ctx.hasMoreOperationsToExecute());
                for (int i = 0; i < numDocs; i++) {
                    BulkItemResponse response = items[i].getPrimaryResponse();
                    assertThat(response, notNullValue());
                    assertFalse(response.isFailed());
                    assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                }
            }
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
        }

        closeShards(shard);
    }

    public void testResolveFallsBackForUnsupportedMapper() throws Exception {
        // Boolean is not in the v1 support list — the batch path should fall back to sequential,
        // which still produces valid documents (tests that there's no regression in fallback).
        String mapping = """
            {
              "properties": {
                "host":   { "type": "keyword" },
                "active": { "type": "boolean" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("host", "a");
            builder.setBoolean("active", true);
            builder.endDocument();
            try (EirfBatch batch = builder.build()) {
                BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
                assertNull("batch path should refuse boolean leaves and trigger sequential fallback", resolution);

                // Also verify the sequential fallback path (via performBatchIndexOnPrimary) leaves items
                // available for the caller to process via the non-batch path.
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
                BulkPrimaryExecutionContext ctx = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, ctx, future);
                future.actionGet();
                assertTrue(
                    "fallback contract: context should still have items queued for sequential path",
                    ctx.hasMoreOperationsToExecute()
                );
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
        // Use stored source here: with synthetic source, the non-batch path would store
        // the unmapped `extra` in _ignored_source so the reconstructed source still matches
        // the translog. The v1 batch path does not implement that ignored-source bridge yet,
        // so we verify the drop behavior against a stored-source index for now.
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

                BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
                BulkPrimaryExecutionContext ctx = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, ctx, future);
                future.actionGet();

                assertFalse(ctx.hasMoreOperationsToExecute());
                BulkItemResponse response = items[0].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse(response.isFailed());
            }
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
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
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
                BulkPrimaryExecutionContext ctx = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, ctx, future);
                future.actionGet();

                assertFalse(ctx.hasMoreOperationsToExecute());
                BulkItemResponse response = items[0].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse(response.isFailed());
            }
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
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
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
                BulkPrimaryExecutionContext ctx = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, ctx, future);
                future.actionGet();

                assertFalse(ctx.hasMoreOperationsToExecute());
                BulkItemResponse response = items[0].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse(response.isFailed());
            }
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
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
                BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
                assertNotNull("mapping should be resolvable for batch path", resolution);

                List<Engine.Index> ops = ShardBatchMapper.parseMappings(items, batch, shard, 1, 0, resolution);
                assertNotNull("parseMappings must succeed (no fallback) for this test to be meaningful", ops);
                assertThat(ops.size(), equalTo(1));

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
                BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
                assertNotNull(resolution);

                List<Engine.Index> ops = ShardBatchMapper.parseMappings(items, batch, shard, 1, 0, resolution);
                assertNotNull("parseMappings must succeed (no fallback) for this test to be meaningful", ops);
                assertThat(ops.size(), equalTo(1));

                LuceneDocument doc = ops.getFirst().parsedDoc().rootDoc();

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
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
                BulkPrimaryExecutionContext ctx = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, ctx, future);
                future.actionGet();

                assertFalse(ctx.hasMoreOperationsToExecute());
                BulkItemResponse response = items[0].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse(response.isFailed());
            }
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
        }

        closeShards(shard);
    }

}
