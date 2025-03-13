/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.LiveVersionMapTestUtils;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongSupplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;

public class ShardGetServiceTests extends IndexShardTestCase {

    private GetResult getForUpdate(IndexShard indexShard, String id, long ifSeqNo, long ifPrimaryTerm) throws IOException {
        return indexShard.getService().getForUpdate(id, ifSeqNo, ifPrimaryTerm, new String[] { RoutingFieldMapper.NAME });
    }

    public void testGetForUpdate() throws IOException {
        Settings settings = indexSettings(IndexVersion.current(), 1, 1).build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        LongSupplier translogInMemorySegmentCount = ((InternalEngine) primary.getEngine()).translogInMemorySegmentsCount::get;
        long translogInMemorySegmentCountExpected = 0;
        Engine.IndexResult test = indexDoc(primary, "test", "0", "{\"foo\" : \"bar\"}");
        assertTrue(primary.getEngine().refreshNeeded());
        GetResult testGet = getForUpdate(primary, "0", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM);
        assertFalse(testGet.getFields().containsKey(RoutingFieldMapper.NAME));
        assertEquals(testGet.sourceRef().utf8ToString(), "{\"foo\" : \"bar\"}");
        assertEquals(translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 1); // we refreshed
        }

        Engine.IndexResult test1 = indexDoc(primary, "1", "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        assertTrue(primary.getEngine().refreshNeeded());
        GetResult testGet1 = getForUpdate(primary, "1", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM);
        assertEquals(testGet1.sourceRef().utf8ToString(), "{\"foo\" : \"baz\"}");
        assertTrue(testGet1.getFields().containsKey(RoutingFieldMapper.NAME));
        assertEquals("foobar", testGet1.getFields().get(RoutingFieldMapper.NAME).getValue());
        assertEquals(translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 1); // we read from the translog
        }
        primary.getEngine().refresh("test");
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 2);
        }

        // now again from the reader
        Engine.IndexResult test2 = indexDoc(primary, "1", "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        assertTrue(primary.getEngine().refreshNeeded());
        testGet1 = getForUpdate(primary, "1", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM);
        assertEquals(testGet1.sourceRef().utf8ToString(), "{\"foo\" : \"baz\"}");
        assertTrue(testGet1.getFields().containsKey(RoutingFieldMapper.NAME));
        assertEquals("foobar", testGet1.getFields().get(RoutingFieldMapper.NAME).getValue());
        assertEquals(translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());

        final long primaryTerm = primary.getOperationPrimaryTerm();
        testGet1 = getForUpdate(primary, "1", test2.getSeqNo(), primaryTerm);
        assertEquals(testGet1.sourceRef().utf8ToString(), "{\"foo\" : \"baz\"}");
        assertEquals(translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());

        expectThrows(VersionConflictEngineException.class, () -> getForUpdate(primary, "1", test2.getSeqNo() + 1, primaryTerm));
        expectThrows(VersionConflictEngineException.class, () -> getForUpdate(primary, "1", test2.getSeqNo(), primaryTerm + 1));
        closeShards(primary);
    }

    public void testGetFromTranslogWithStringSourceMappingOptionsAndStoredFields() throws IOException {
        String docToIndex = """
            {"foo" : "foo", "bar" : "bar"}
            """;
        boolean noSource = randomBoolean();
        String sourceOptions = noSource ? "\"enabled\": false" : randomBoolean() ? "\"excludes\": [\"fo*\"]" : "\"includes\": [\"ba*\"]";
        runGetFromTranslogWithOptions(docToIndex, sourceOptions, null, noSource ? "" : "{\"bar\":\"bar\"}", "\"text\"", "foo", false);
    }

    public void testGetFromTranslogWithLongSourceMappingOptionsAndStoredFields() throws IOException {
        String docToIndex = """
            {"foo" : 7, "bar" : 42}
            """;
        boolean noSource = randomBoolean();
        String sourceOptions = noSource ? "\"enabled\": false" : randomBoolean() ? "\"excludes\": [\"fo*\"]" : "\"includes\": [\"ba*\"]";
        runGetFromTranslogWithOptions(docToIndex, sourceOptions, null, noSource ? "" : "{\"bar\":42}", "\"long\"", 7L, false);
    }

    public void testGetFromTranslogWithSyntheticSource() throws IOException {
        String docToIndex = """
            {"foo":7,"bar":42}
            """;
        String expectedFetchedSource = """
            {"bar":42,"foo":7}""";
        var settings = Settings.builder().put("index.mapping.source.mode", "synthetic").build();
        runGetFromTranslogWithOptions(docToIndex, "", settings, expectedFetchedSource, "\"long\"", 7L, true);
    }

    public void testGetFromTranslogWithDenseVector() throws IOException {
        float[] vector = new float[2048];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        String docToIndex = Strings.format("""
            {
                "bar": %s,
                "foo": "foo"
            }
            """, Arrays.toString(vector));
        runGetFromTranslogWithOptions(docToIndex, "\"enabled\": true", null, docToIndex, "\"text\"", "foo", "\"dense_vector\"", false);
    }

    private void runGetFromTranslogWithOptions(
        String docToIndex,
        String sourceOptions,
        Settings settings,
        String expectedResult,
        String fieldType,
        Object expectedFooVal,
        boolean sourceOnlyFetchCreatesInMemoryReader
    ) throws IOException {
        runGetFromTranslogWithOptions(
            docToIndex,
            sourceOptions,
            settings,
            expectedResult,
            fieldType,
            expectedFooVal,
            fieldType,
            sourceOnlyFetchCreatesInMemoryReader
        );
    }

    private void runGetFromTranslogWithOptions(
        String docToIndex,
        String sourceOptions,
        Settings additionalSettings,
        String expectedResult,
        String fieldTypeFoo,
        Object expectedFooVal,
        String fieldTypeBar,
        boolean sourceOnlyFetchCreatesInMemoryReader
    ) throws IOException {

        var indexSettingsBuilder = indexSettings(IndexVersion.current(), 1, 1);
        if (additionalSettings != null) {
            indexSettingsBuilder.put(additionalSettings);
        }
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping(Strings.format("""
            {
              "properties": {
                "foo": {
                  "type": %s,
                  "store": true
                },
                "bar": { "type": %s }
              },
              "_source": { %s }
              }
            }""", fieldTypeFoo, fieldTypeBar, sourceOptions)).settings(indexSettingsBuilder).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, EngineTestCase.randomReaderWrapper());
        recoverShardFromStore(primary);
        LongSupplier translogInMemorySegmentCount = ((InternalEngine) primary.getEngine()).translogInMemorySegmentsCount::get;
        long translogInMemorySegmentCountExpected = 0;
        Engine.IndexResult res = indexDoc(primary, "test", "0", docToIndex);
        assertTrue(res.isCreated());
        assertTrue(primary.getEngine().refreshNeeded());
        GetResult testGet = getForUpdate(primary, "0", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM);
        assertFalse(testGet.getFields().containsKey(RoutingFieldMapper.NAME));
        assertFalse(testGet.getFields().containsKey("foo"));
        assertFalse(testGet.getFields().containsKey("bar"));
        assertThat(testGet.sourceRef() == null ? "" : testGet.sourceRef().utf8ToString(), equalTo(expectedResult));
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 1); // we refreshed
        }

        indexDoc(primary, "1", docToIndex, XContentType.JSON, "foobar");
        assertTrue(primary.getEngine().refreshNeeded());
        GetResult testGet1 = getForUpdate(primary, "1", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM);
        assertEquals(testGet1.sourceRef() == null ? "" : testGet1.sourceRef().utf8ToString(), expectedResult);
        assertTrue(testGet1.getFields().containsKey(RoutingFieldMapper.NAME));
        assertFalse(testGet.getFields().containsKey("foo"));
        assertFalse(testGet.getFields().containsKey("bar"));
        assertEquals("foobar", testGet1.getFields().get(RoutingFieldMapper.NAME).getValue());
        if (sourceOnlyFetchCreatesInMemoryReader) {
            translogInMemorySegmentCountExpected++;
        }
        assertEquals(translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 1); // we read from the translog
        }
        primary.getEngine().refresh("test");
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 2);
        }

        Engine.IndexResult test2 = indexDoc(primary, "2", docToIndex, XContentType.JSON, "foobar");
        assertTrue(primary.getEngine().refreshNeeded());
        GetResult testGet2 = primary.getService()
            .get("2", new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertEquals(testGet2.sourceRef() == null ? "" : testGet2.sourceRef().utf8ToString(), expectedResult);
        assertTrue(testGet2.getFields().containsKey(RoutingFieldMapper.NAME));
        assertTrue(testGet2.getFields().containsKey("foo"));
        assertEquals(expectedFooVal, testGet2.getFields().get("foo").getValue());
        assertEquals(++translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 2); // we read from the translog
        }
        primary.getEngine().refresh("test");
        try (Engine.Searcher searcher = primary.getEngine().acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
            assertEquals(searcher.getIndexReader().maxDoc(), 3);
        }

        testGet2 = primary.getService()
            .get("2", new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertEquals(testGet2.sourceRef() == null ? "" : testGet2.sourceRef().utf8ToString(), expectedResult);
        assertTrue(testGet2.getFields().containsKey(RoutingFieldMapper.NAME));
        assertTrue(testGet2.getFields().containsKey("foo"));
        assertEquals(expectedFooVal, testGet2.getFields().get("foo").getValue());
        assertEquals(translogInMemorySegmentCountExpected, translogInMemorySegmentCount.getAsLong());

        closeShards(primary);
    }

    public void testTypelessGetForUpdate() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index")
            .putMapping("""
                { "properties": { "foo":  { "type": "text"}}}""")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .primaryTerm(0, 1)
            .build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(shard);
        Engine.IndexResult indexResult = indexDoc(shard, "some_type", "0", "{\"foo\" : \"bar\"}");
        assertTrue(indexResult.isCreated());

        GetResult getResult = getForUpdate(shard, "0", UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM);
        assertTrue(getResult.isExists());

        closeShards(shard);
    }

    public void testGetFromTranslog() throws IOException {
        Settings settings = indexSettings(IndexVersion.current(), 1, 1).build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        InternalEngine engine = (InternalEngine) primary.getEngineOrNull();

        // Initially there hasn't been any switches from unsafe to safe maps in the live version map
        assertEquals(engine.getLastUnsafeSegmentGenerationForGets(), engine.getLastCommittedSegmentInfos().getGeneration());
        var map = engine.getLiveVersionMap();
        assertFalse(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertFalse(LiveVersionMapTestUtils.isUnsafe(map));

        // Make the map unsafe by indexing a doc that will be indexed in the append-only mode
        var indexResult = indexDoc(primary, null, "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        assertFalse(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertTrue(LiveVersionMapTestUtils.isUnsafe(map));

        // Issue a get that would enforce safe access mode and switches the maps from unsafe to safe
        var getResult = primary.getService()
            .getFromTranslog("2", new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertNull(getResult);
        var lastUnsafeGeneration = engine.getLastUnsafeSegmentGenerationForGets();
        // last unsafe generation is set to last committed gen after the refresh triggered by realtime get
        assertThat(lastUnsafeGeneration, equalTo(engine.getLastCommittedSegmentInfos().getGeneration()));
        assertTrue(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertFalse(LiveVersionMapTestUtils.isUnsafe(map));

        // A flush shouldn't change the recorded last unsafe generation for gets
        PlainActionFuture<Engine.FlushResult> flushFuture = new PlainActionFuture<>();
        engine.flush(true, true, flushFuture);
        var flushResult = flushFuture.actionGet();
        assertTrue(flushResult.flushPerformed());
        assertThat(flushResult.generation(), equalTo(lastUnsafeGeneration + 1));
        assertThat(engine.getLastUnsafeSegmentGenerationForGets(), equalTo(lastUnsafeGeneration));
        // No longer in translog
        getResult = primary.getService()
            .getFromTranslog(
                indexResult.getId(),
                new String[] { "foo" },
                true,
                1,
                VersionType.INTERNAL,
                FetchSourceContext.FETCH_SOURCE,
                false
            );
        assertNull(getResult);
        // But normal get would still work!
        getResult = primary.getService()
            .get(indexResult.getId(), new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertNotNull(getResult);
        assertTrue(getResult.isExists());
        assertEquals(engine.getLastUnsafeSegmentGenerationForGets(), lastUnsafeGeneration);

        // As long as in safe mode, last unsafe generation stays the same
        assertTrue(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertFalse(LiveVersionMapTestUtils.isUnsafe(map));
        indexDoc(primary, "1", "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        // The first get in safe mode, would trigger a refresh, since we need to start tracking translog locations in the live version map
        getResult = primary.getService()
            .getFromTranslog("1", new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertTrue(getResult.isExists());
        assertEquals(engine.getLastUnsafeSegmentGenerationForGets(), lastUnsafeGeneration);
        getResult = primary.getService()
            .getFromTranslog("2", new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertNull(getResult);
        assertEquals(engine.getLastUnsafeSegmentGenerationForGets(), lastUnsafeGeneration);

        // After two refreshes (one for tracking translog locations, i.e., source="realtime_get") and the following)
        // with no safe access needed, it should switch to append-only. (see https://github.com/elastic/elasticsearch/pull/27752)
        assertTrue(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertFalse(LiveVersionMapTestUtils.isUnsafe(map));
        indexDoc(primary, null, "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        engine.refresh("test");
        assertFalse(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertFalse(LiveVersionMapTestUtils.isUnsafe(map));

        // Redo the same: make the map unsafe and see that the recorded last unsafe generation gets updated, upon a get.
        indexDoc(primary, null, "{\"foo\" : \"baz\"}", XContentType.JSON, "foobar");
        assertFalse(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertTrue(LiveVersionMapTestUtils.isUnsafe(map));
        getResult = primary.getService()
            .getFromTranslog("2", new String[] { "foo" }, true, 1, VersionType.INTERNAL, FetchSourceContext.FETCH_SOURCE, false);
        assertNull(getResult);
        var lastUnsafeGeneration2 = engine.getLastUnsafeSegmentGenerationForGets();
        assertTrue(lastUnsafeGeneration2 > lastUnsafeGeneration);
        assertTrue(LiveVersionMapTestUtils.isSafeAccessRequired(map));
        assertFalse(LiveVersionMapTestUtils.isUnsafe(map));

        closeShards(primary);
    }
}
