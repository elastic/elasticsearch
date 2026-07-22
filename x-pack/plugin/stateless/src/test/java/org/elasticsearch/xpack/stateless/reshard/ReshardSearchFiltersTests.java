/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReshardSearchFiltersTests extends ESTestCase {
    private Directory directory;
    private ReshardSearchFilters reshardSearchFilters;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        reshardSearchFilters = new ReshardSearchFilters(Settings.EMPTY);
    }

    @Override
    public void tearDown() throws Exception {
        reshardSearchFilters.close();
        directory.close();
        super.tearDown();
    }

    private ReshardUnownedBitsetCache cache() {
        return reshardSearchFilters.unownedBitsetCache();
    }

    public void testReaderWithoutDocuments() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig());
        iw.close();
        try (
            DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(directory),
                new ShardId(new Index("index", "_na_"), 0)
            )
        ) {
            var indexMetadata = IndexMetadata.builder("index")
                .numberOfShards(1)
                .numberOfReplicas(0)
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .build();
            var query = new ShardSplittingQuery(indexMetadata, 0, false);
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query, cache());

            assertEquals(0, directoryReader.numDocs());
            assertEquals(0, directoryReader.leaves().size());

            assertEquals(0, wrapper.numDocs());
            assertEquals(0, directoryReader.leaves().size());
        }
    }

    public void testReaderWithDocuments() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(100));

        var docs = randomIntBetween(2, 10);

        for (int i = 0; i < docs; i++) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.NO));
            iw.addDocument(document);
        }
        iw.commit();

        try (
            DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(iw),
                new ShardId(new Index("index", "_na_"), 0)
            );
            iw
        ) {
            var indexMetadata = IndexMetadata.builder("index")
                .numberOfShards(1)
                .numberOfReplicas(0)
                .settings(indexSettings(IndexVersion.current(), 1, 0))
                .build();
            var query = new ShardSplittingQuery(indexMetadata, 0, false);
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query, cache());

            assertEquals(docs, directoryReader.numDocs());
            assertEquals(1, directoryReader.leaves().size());
            assertNull(directoryReader.leaves().get(0).reader().getLiveDocs()); // since there is no deletes

            // Identical since there is only one shard
            assertEquals(docs, wrapper.numDocs());
            assertEquals(1, wrapper.leaves().size());
            // We can't tell in the current implementation if there are any unowned documents right away.
            // So we return a non-null bitset with all bits unset.
            for (int i = 0; i < docs; i++) {
                assertTrue(wrapper.leaves().get(0).reader().getLiveDocs().get(i));
            }
            Map<String, Object> cacheStatsBeforeWrapper2 = cache().usageStats();
            assertEquals(1L, cacheStatsBeforeWrapper2.get("misses"));
            assertEquals(0L, cacheStatsBeforeWrapper2.get("hits"));

            var deletedId = randomIntBetween(0, docs - 1);
            iw.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(deletedId))));
            try (DirectoryReader wrapper2 = DirectoryReader.openIfChanged(wrapper)) {
                assertEquals(docs - 1, wrapper2.numDocs());
                Map<String, Object> cacheStatsAfterWrapper2NumDocs = cache().usageStats();
                assertEquals(1L, cacheStatsAfterWrapper2NumDocs.get("misses"));
                assertEquals(1L, cacheStatsAfterWrapper2NumDocs.get("hits"));
                assertEquals(1, wrapper2.leaves().size());
                assertEquals(docs, wrapper2.leaves().get(0).reader().getLiveDocs().length());
                for (int i = 0; i < docs; i++) {
                    if (i == deletedId) {
                        assertFalse(wrapper2.leaves().get(0).reader().getLiveDocs().get(i));
                    } else {
                        assertTrue(wrapper2.leaves().get(0).reader().getLiveDocs().get(i));
                    }
                }
            }
        }
    }

    public void testReaderWithUnownedDocuments() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(100));

        var docs = randomIntBetween(10, 20);

        for (int i = 0; i < docs; i++) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.NO));
            iw.addDocument(document);
        }
        iw.commit();

        try (
            DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(iw),
                new ShardId(new Index("index", "_na_"), 0)
            );
            iw
        ) {
            var indexMetadata = IndexMetadata.builder("index")
                .numberOfShards(2)
                .numberOfReplicas(0)
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .build();
            var query = new ShardSplittingQuery(indexMetadata, 0, false);
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query, cache());

            assertEquals(docs, directoryReader.numDocs());
            assertEquals(1, directoryReader.leaves().size());
            assertNull(directoryReader.leaves().get(0).reader().getLiveDocs()); // since there is no deletes

            var routing = IndexRouting.fromIndexMetadata(indexMetadata);
            var routesToShard0 = IntStream.range(0, docs)
                .boxed()
                .map(Object::toString)
                .filter(id -> routing.getShard(id, null) == 0)
                .collect(Collectors.toSet());

            assertEquals(routesToShard0.size(), wrapper.numDocs());
            assertEquals(1, wrapper.leaves().size());

            // Live docs with deletes of unowned documents applied.
            var liveDocs = wrapper.leaves().get(0).reader().getLiveDocs();
            assertLiveDocsMatchShardOwnership(liveDocs, docs, routesToShard0);

            Map<String, Object> cacheStatsBeforeWrapper2 = cache().usageStats();
            assertEquals(1L, cacheStatsBeforeWrapper2.get("misses"));
            assertEquals(0L, cacheStatsBeforeWrapper2.get("hits"));

            // Delete one of the owned documents.
            int deletedOwnedId = randomFrom(
                IntStream.range(0, docs).boxed().filter(i -> routesToShard0.contains(Integer.toString(i))).toList()
            );
            iw.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(deletedOwnedId))));
            Set<String> ownedIdsAfterDeletingOwned = new HashSet<>(routesToShard0);
            ownedIdsAfterDeletingOwned.remove(Integer.toString(deletedOwnedId));

            try (DirectoryReader wrapper2 = DirectoryReader.openIfChanged(wrapper)) {
                assertEquals(ownedIdsAfterDeletingOwned.size(), wrapper2.numDocs());
                assertEquals(1, wrapper2.leaves().size());

                // Live docs with deletes of unowned documents and delete of `deletedOwnedId` applied.
                var liveDocs2 = wrapper2.leaves().get(0).reader().getLiveDocs();

                Map<String, Object> cacheStatsBeforeWrapper3 = cache().usageStats();
                assertEquals(1L, cacheStatsBeforeWrapper3.get("misses"));
                assertEquals(1L, cacheStatsBeforeWrapper3.get("hits"));

                assertEquals(docs, liveDocs2.length());
                assertLiveDocsMatchShardOwnership(liveDocs2, docs, ownedIdsAfterDeletingOwned);
            }

            // Delete one of the unowned documents - should not change anything from the previous state.
            int deletedUnownedId = randomFrom(
                IntStream.range(0, docs).boxed().filter(i -> routesToShard0.contains(Integer.toString(i)) == false).toList()
            );
            iw.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(deletedUnownedId))));

            try (DirectoryReader wrapper3 = DirectoryReader.openIfChanged(wrapper)) {
                assertEquals(ownedIdsAfterDeletingOwned.size(), wrapper3.numDocs());
                assertEquals(1, wrapper3.leaves().size());

                // Live docs with deletes of unowned documents, delete of `deletedOwnedId`, and delete of `deletedUnownedId` applied.
                var liveDocs3 = wrapper3.leaves().get(0).reader().getLiveDocs();

                Map<String, Object> cacheStatsAfterWrapper3 = cache().usageStats();
                assertEquals(1L, cacheStatsAfterWrapper3.get("misses"));
                assertEquals(2L, cacheStatsAfterWrapper3.get("hits"));

                assertEquals(docs, liveDocs3.length());
                assertLiveDocsMatchShardOwnership(liveDocs3, docs, ownedIdsAfterDeletingOwned);
            }
        }
    }

    public void testAllDocumentsUnowned() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(100));

        var docs = randomIntBetween(2, 10);

        for (int i = 0; i < docs; i++) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.NO));
            iw.addDocument(document);
        }
        iw.commit();

        try (
            DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(iw),
                new ShardId(new Index("index", "_na_"), 0)
            );
            iw
        ) {
            var indexMetadata = IndexMetadata.builder("index")
                .numberOfShards(1)
                .numberOfReplicas(0)
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .build();
            // ShardSplittingQuery with this bogus shardId will return all documents.
            var query = new ShardSplittingQuery(indexMetadata, 222, false);
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query, cache());

            assertEquals(docs, directoryReader.numDocs());
            assertEquals(1, directoryReader.leaves().size());
            assertNull(directoryReader.leaves().get(0).reader().getLiveDocs()); // since there is no deletes

            assertEquals(0, wrapper.numDocs());
            assertEquals(1, wrapper.leaves().size());

            var liveDocs = wrapper.leaves().get(0).reader().getLiveDocs();
            for (int i = 0; i < docs; i++) {
                assertFalse(liveDocs.get(i));
            }
        }
    }

    public void testUnownedBitsetCacheSharesSegmentCoreBetweenDirectoryReaderWraps() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(100));

        var docs = randomIntBetween(5, 15);
        for (int i = 0; i < docs; i++) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.NO));
            iw.addDocument(document);
        }
        iw.commit();

        Settings cacheSettings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "256mb").build();
        ReshardSearchFilters reshardSearchFilters = new ReshardSearchFilters(cacheSettings);
        ReshardUnownedBitsetCache cache = reshardSearchFilters.unownedBitsetCache();
        assertNotNull(cache);

        try (
            DirectoryReader directoryReader1 = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(iw),
                new ShardId(new Index("index", "_na_"), 0)
            );
            DirectoryReader directoryReader2 = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(iw),
                new ShardId(new Index("index", "_na_"), 0)
            );
            iw
        ) {
            var indexMetadata = IndexMetadata.builder("index")
                .numberOfShards(2)
                .numberOfReplicas(0)
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .build();
            var query = new ShardSplittingQuery(indexMetadata, 0, false);
            var routing = IndexRouting.fromIndexMetadata(indexMetadata);
            var routesToShard0 = IntStream.range(0, docs)
                .boxed()
                .map(Object::toString)
                .filter(id -> routing.getShard(id, null) == 0)
                .collect(Collectors.toSet());
            try (
                var wrapper1 = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader1, query, cache);
                var wrapper2 = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader2, query, cache)
            ) {
                var liveDocsOnMiss = wrapper1.leaves().get(0).reader().getLiveDocs();
                assertEquals(routesToShard0.size(), wrapper1.numDocs());
                assertLiveDocsMatchShardOwnership(liveDocsOnMiss, docs, routesToShard0);

                Map<String, Object> afterFirst = cache.usageStats();
                assertEquals(1L, afterFirst.get("misses"));

                var liveDocsOnHit = wrapper2.leaves().get(0).reader().getLiveDocs();
                assertEquals(wrapper1.numDocs(), wrapper2.numDocs());
                assertLiveDocsMatchShardOwnership(liveDocsOnHit, docs, routesToShard0);
                for (int i = 0; i < docs; i++) {
                    assertEquals(liveDocsOnMiss.get(i), liveDocsOnHit.get(i));
                }

                Map<String, Object> afterSecond = cache.usageStats();
                assertEquals(1L, afterSecond.get("misses"));
                assertEquals(1L, afterSecond.get("hits"));

                cache.verifyInternalConsistency();
            }
        } finally {
            reshardSearchFilters.close();
        }
    }

    public void testFilterQueryMatchingNoDocumentsLeavesReaderUnfiltered() throws IOException {
        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(100));
        var docs = randomIntBetween(1, 10);
        for (int i = 0; i < docs; i++) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.NO));
            iw.addDocument(document);
        }
        iw.commit();
        try (
            DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(
                DirectoryReader.open(iw),
                new ShardId(new Index("index", "_na_"), 0)
            );
            iw
        ) {
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, Queries.NO_DOCS_INSTANCE, cache());
            assertEquals(docs, directoryReader.numDocs());
            assertEquals(docs, wrapper.numDocs());
            assertEquals(1, wrapper.leaves().size());
            var delegateLeaf = directoryReader.leaves().get(0).reader();
            var wrappedLeaf = wrapper.leaves().get(0).reader();
            assertSame(delegateLeaf.getLiveDocs(), wrappedLeaf.getLiveDocs());
        }
    }

    public void testWrappingForPitRelocation() throws IOException {
        var indexMetadata = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 4, 0)).build();
        var latestRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(100));

        // Place a document in every shard.
        // Later when we test with PIT that uses two shards, there should be two documents per shard.
        for (int i = 0; i < 4; i++) {
            var id = ReshardingTestHelpers.makeIdThatRoutesToShard(latestRouting, i);
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO));
            iw.addDocument(document);
        }
        iw.commit();

        // We are emulating a 1 -> 2 split that was in progress when PIT was opened.
        ShardId sourceShardId = new ShardId(new Index("index", "_na_"), 0);
        ShardId targetShardId = new ShardId(new Index("index", "_na_"), 1);

        var relocatedReshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(1, 2);

        Settings cacheSettings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "256mb").build();
        ReshardSearchFilters reshardSearchFilters = new ReshardSearchFilters(cacheSettings);

        try (DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(iw), sourceShardId); iw) {
            var mapperService = mock(MapperService.class);
            when(mapperService.hasNested()).thenReturn(false);

            var noReshardingMetadataSource = reshardSearchFilters.maybeWrapDirectoryReaderForPitRelocation(
                directoryReader,
                sourceShardId,
                indexMetadata,
                mapperService,
                null,
                SplitShardCountSummary.UNSET
            );
            // No relocated resharding metadata so no filtering needed.
            assertEquals(directoryReader, noReshardingMetadataSource);

            var olderSummary = reshardSearchFilters.maybeWrapDirectoryReaderForPitRelocation(
                directoryReader,
                sourceShardId,
                indexMetadata,
                mapperService,
                relocatedReshardingMetadata,
                SplitShardCountSummary.fromInt(1)
            );
            // The summary is older based on relocated resharding metadata so no filtering is applied (all documents come from the source
            // shard).
            assertEquals(directoryReader, olderSummary);

            var currentSummarySource = reshardSearchFilters.maybeWrapDirectoryReaderForPitRelocation(
                directoryReader,
                sourceShardId,
                indexMetadata,
                mapperService,
                relocatedReshardingMetadata,
                SplitShardCountSummary.fromInt(2)
            );
            // The summary is current based on relocated resharding metadata so filters should be applied.
            assertNotEquals(directoryReader, currentSummarySource);
            var sourceLiveDocs = currentSummarySource.leaves().get(0).reader().getLiveDocs();
            assertEquals(4, sourceLiveDocs.length());
            // We get documents that route to current shard 0 and current shard 2 since we "reversed" the emulated split 2 -> 4.
            assertTrue(sourceLiveDocs.get(0));
            assertTrue(sourceLiveDocs.get(2));

            var currentSummaryTarget = reshardSearchFilters.maybeWrapDirectoryReaderForPitRelocation(
                directoryReader,
                targetShardId,
                indexMetadata,
                mapperService,
                relocatedReshardingMetadata,
                SplitShardCountSummary.fromInt(2)
            );
            assertNotEquals(directoryReader, currentSummaryTarget);
            var targetLiveDocs = currentSummaryTarget.leaves().get(0).reader().getLiveDocs();
            assertEquals(4, targetLiveDocs.length());
            // We get documents that route to current shard 1 and current shard 3 since we "reversed" the emulated split 2 -> 4.
            assertTrue(targetLiveDocs.get(1));
            assertTrue(targetLiveDocs.get(3));
        }
    }

    // lower level tests that search filter decision-making is as expected

    // to be removed when all callers of acquireSearcherSupplier provide the appropriate summary
    public void testShouldFilterAllowsUnsetSummary() {
        assertFalse(ReshardSearchFilters.shouldFilter(testShardId(0), SplitShardCountSummary.UNSET, 128, null));
    }

    public void testShouldFilterAllowsIrrelevantSummary() {
        assertFalse(ReshardSearchFilters.shouldFilter(testShardId(0), SplitShardCountSummary.IRRELEVANT, 128, null));
    }

    // if there is no split in progress and the request and the shard agree on summary, don't filter
    public void testShouldFilterNoSplit() {
        final var shardCount = randomIntBetween(1, 5);
        final var shardNumber = randomIntBetween(0, shardCount - 1);

        final var indexMetadata = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), shardCount, 0)).build();
        final var requestSummary = SplitShardCountSummary.forSearch(indexMetadata, shardNumber);

        assertFalse(ReshardSearchFilters.shouldFilter(testShardId(shardNumber), requestSummary, shardCount, null));
    }

    public void testTargetShardFilteringDecision() {
        final var origShardCount = randomIntBetween(1, 5);
        final var newShardCount = origShardCount * 2;
        final var targetShard = randomIntBetween(origShardCount, newShardCount - 1);

        // Search shards are already started before SPLIT state is applied and will serve searches while seeing HANDOFF.
        var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2)
            .transitionSplitTargetToNewState(testShardId(targetShard), IndexReshardingState.Split.TargetShardState.HANDOFF);

        // We should filter until the target shard is DONE.
        // Note that summary of origShardCount is invalid on the target shards by the definition of the shard count summary.
        assertTrue(
            ReshardSearchFilters.shouldFilter(
                testShardId(targetShard),
                SplitShardCountSummary.fromInt(newShardCount),
                newShardCount,
                reshardingMetadata
            )
        );

        reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
            testShardId(targetShard),
            IndexReshardingState.Split.TargetShardState.SPLIT
        );
        assertTrue(
            ReshardSearchFilters.shouldFilter(
                testShardId(targetShard),
                SplitShardCountSummary.fromInt(newShardCount),
                newShardCount,
                reshardingMetadata
            )
        );

        reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
            testShardId(targetShard),
            IndexReshardingState.Split.TargetShardState.DONE
        );
        assertFalse(
            ReshardSearchFilters.shouldFilter(
                testShardId(targetShard),
                SplitShardCountSummary.fromInt(newShardCount),
                newShardCount,
                reshardingMetadata
            )
        );

        assertFalse(
            ReshardSearchFilters.shouldFilter(testShardId(targetShard), SplitShardCountSummary.fromInt(newShardCount), newShardCount, null)
        );
    }

    public void testSourceShardFilteringDecision() {
        final var origShardCount = 2;
        final var newShardCount = 4;
        final var sourceShard = 0;
        final var targetShard = 2;

        var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2);

        // We should filter until the source shard is DONE unless the summary is older.
        assertFalse(
            ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(origShardCount),
                newShardCount,
                reshardingMetadata
            )
        );
        assertTrue(
            ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(newShardCount),
                newShardCount,
                reshardingMetadata
            )
        );

        var reshardingMetadataAtReadyForCleanup = reshardingMetadata.transitionSplitTargetToNewState(
            testShardId(targetShard),
            IndexReshardingState.Split.TargetShardState.HANDOFF
        )
            .transitionSplitTargetToNewState(testShardId(targetShard), IndexReshardingState.Split.TargetShardState.SPLIT)
            .transitionSplitTargetToNewState(testShardId(targetShard), IndexReshardingState.Split.TargetShardState.DONE)
            .transitionSplitSourceToNewState(testShardId(sourceShard), IndexReshardingState.Split.SourceShardState.READY_FOR_CLEANUP);

        assertThrows(
            StaleRequestException.class,
            () -> ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(origShardCount),
                newShardCount,
                reshardingMetadataAtReadyForCleanup
            )
        );
        assertTrue(
            ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(newShardCount),
                newShardCount,
                reshardingMetadataAtReadyForCleanup
            )
        );

        var reshardingMetadataAtDone = reshardingMetadataAtReadyForCleanup.transitionSplitSourceToNewState(
            testShardId(sourceShard),
            IndexReshardingState.Split.SourceShardState.DONE
        );

        assertThrows(
            StaleRequestException.class,
            () -> ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(origShardCount),
                newShardCount,
                reshardingMetadataAtDone
            )
        );
        assertFalse(
            ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(newShardCount),
                newShardCount,
                reshardingMetadataAtDone
            )
        );

        assertThrows(
            StaleRequestException.class,
            () -> ReshardSearchFilters.shouldFilter(
                testShardId(sourceShard),
                SplitShardCountSummary.fromInt(origShardCount),
                newShardCount,
                null
            )
        );
        assertFalse(
            ReshardSearchFilters.shouldFilter(testShardId(sourceShard), SplitShardCountSummary.fromInt(newShardCount), newShardCount, null)
        );
    }

    private static void assertLiveDocsMatchShardOwnership(Bits liveDocs, int totalDocs, Set<String> ownedIds) {
        for (int i = 0; i < totalDocs; i++) {
            assertEquals(ownedIds.contains(Integer.toString(i)), liveDocs.get(i));
        }
    }

    private ShardId testShardId(int shardNumber) {
        return new ShardId(new Index("index", "_na_"), shardNumber);
    }
}
