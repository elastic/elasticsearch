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

    // lower level tests that search filter decision-making is as expected

    // to be removed when all callers of acquireSearcherSupplier provide the appropriate summary
    public void testShouldFilterAllowsUnsetSummary() {
        assertFalse(ReshardSearchFilters.shouldFilter(testShardId(0), SplitShardCountSummary.UNSET, 128, null));
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

    public void testAdjustMetadataForPitRelocation() {
        var relocatedReshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(2, 2);

        var noReshardingMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 4, randomIntBetween(0, 8)))
            .build();
        var noReshardingMetadataAdjusted = ReshardSearchFilters.adjustMetadataForPitRelocation(
            noReshardingMetadata,
            relocatedReshardingMetadata
        );
        assertEquals(4, noReshardingMetadataAdjusted.getNumberOfShards());
        assertEquals(relocatedReshardingMetadata, noReshardingMetadataAdjusted.getReshardingMetadata());

        var noReshardingMetadataOneSplitAhead = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 8, randomIntBetween(0, 8)))
            .build();
        var noReshardingMetadataOneSplitAheadAdjusted = ReshardSearchFilters.adjustMetadataForPitRelocation(
            noReshardingMetadataOneSplitAhead,
            relocatedReshardingMetadata
        );
        assertEquals(4, noReshardingMetadataOneSplitAheadAdjusted.getNumberOfShards());
        assertEquals(relocatedReshardingMetadata, noReshardingMetadataOneSplitAheadAdjusted.getReshardingMetadata());

        var noReshardingMetadataMultipleSplitsAhead = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 32, randomIntBetween(0, 8)))
            .build();
        var noReshardingMetadataMultipleSplitsAheadAdjusted = ReshardSearchFilters.adjustMetadataForPitRelocation(
            noReshardingMetadataMultipleSplitsAhead,
            relocatedReshardingMetadata
        );
        assertEquals(4, noReshardingMetadataMultipleSplitsAheadAdjusted.getNumberOfShards());
        assertEquals(relocatedReshardingMetadata, noReshardingMetadataMultipleSplitsAheadAdjusted.getReshardingMetadata());

        var anotherSplitInProgress = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 16, randomIntBetween(0, 8)))
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(8, 16))
            .build();
        var anotherSplitInProgressAdjusted = ReshardSearchFilters.adjustMetadataForPitRelocation(
            anotherSplitInProgress,
            relocatedReshardingMetadata
        );
        assertEquals(4, anotherSplitInProgressAdjusted.getNumberOfShards());
        assertEquals(relocatedReshardingMetadata, anotherSplitInProgressAdjusted.getReshardingMetadata());
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
