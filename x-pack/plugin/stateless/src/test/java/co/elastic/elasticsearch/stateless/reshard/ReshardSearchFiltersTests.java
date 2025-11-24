/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReshardSearchFiltersTests extends ESTestCase {
    private Directory directory;

    @Before
    public void setUpDirectory() {
        directory = newDirectory();
    }

    @After
    public void cleanDirectory() throws Exception {
        directory.close();
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
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query);

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
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query);

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

            var deletedId = randomIntBetween(0, docs - 1);
            iw.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(deletedId))));
            try (DirectoryReader wrapper2 = DirectoryReader.openIfChanged(wrapper)) {
                assertEquals(docs - 1, wrapper2.numDocs());
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
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query);

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

            for (int i = 0; i < docs; i++) {
                String id = Integer.toString(i);
                boolean expected = routesToShard0.contains(id);
                assertEquals(expected, liveDocs.get(i));
            }

            // Delete one of the owned documents.
            int deletedOwnedId = randomFrom(
                IntStream.range(0, docs).boxed().filter(i -> routesToShard0.contains(Integer.toString(i))).toList()
            );
            iw.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(deletedOwnedId))));

            try (DirectoryReader wrapper2 = DirectoryReader.openIfChanged(wrapper)) {
                assertEquals(routesToShard0.size() - 1, wrapper2.numDocs());
                assertEquals(1, wrapper2.leaves().size());

                // Live docs with deletes of unowned documents and delete of `deletedOwnedId` applied.
                var liveDocs2 = wrapper2.leaves().get(0).reader().getLiveDocs();

                assertEquals(docs, liveDocs2.length());
                for (int i = 0; i < docs; i++) {
                    if (i == deletedOwnedId) {
                        assertFalse(liveDocs2.get(i));
                    } else {
                        String id = Integer.toString(i);
                        boolean expected = routesToShard0.contains(id);
                        assertEquals(expected, liveDocs2.get(i));
                    }
                }
            }

            // Delete one of the unowned documents - should not change anything from the previous state.
            int deletedUnownedId = randomFrom(
                IntStream.range(0, docs).boxed().filter(i -> routesToShard0.contains(Integer.toString(i)) == false).toList()
            );
            iw.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(deletedUnownedId))));

            try (DirectoryReader wrapper3 = DirectoryReader.openIfChanged(wrapper)) {
                assertEquals(routesToShard0.size() - 1, wrapper3.numDocs());
                assertEquals(1, wrapper3.leaves().size());

                // Live docs with deletes of unowned documents, delete of `deletedOwnedId`, and delete of `deletedUnownedId` applied.
                var liveDocs3 = wrapper3.leaves().get(0).reader().getLiveDocs();

                assertEquals(docs, liveDocs3.length());
                for (int i = 0; i < docs; i++) {
                    if (i == deletedOwnedId) {
                        assertFalse(liveDocs3.get(i));
                    } else {
                        String id = Integer.toString(i);
                        boolean expected = routesToShard0.contains(id);
                        assertEquals(expected, liveDocs3.get(i));
                    }
                }
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
            var wrapper = new ReshardSearchFilters.QueryFilterDirectoryReader(directoryReader, query);

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

    // lower level tests that search filter decision-making is as expected

    // to be removed when all callers of acquireSearcherSupplier provide the appropriate summary
    public void testShouldFilterAllowsUnsetSummary() {
        assertFalse(ReshardSearchFilters.shouldFilter(SplitShardCountSummary.UNSET, null, testShardId(0)));
    }

    // if there is no split in progress and the request and the shard agree on summary, don't filter
    public void testShouldFilterNoSplit() {
        final var shardCount = randomIntBetween(1, 5);
        final var shardNumber = randomIntBetween(0, shardCount - 1);

        final var indexMetadata = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), shardCount, 0)).build();
        final var requestSummary = SplitShardCountSummary.forSearch(indexMetadata, shardNumber);

        assertFalse(ReshardSearchFilters.shouldFilter(requestSummary, indexMetadata, testShardId(shardNumber)));
    }

    // requests that see a target shard at SPLIT or DONE should filter source
    public void testShouldFilterPostSplitTarget() {
        final var origShardCount = randomIntBetween(1, 5);
        final var newShardCount = origShardCount * 2;
        final var targetShard = randomIntBetween(origShardCount, newShardCount - 1);
        final var newSplit = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2).getSplit();
        final var sourceShard = newSplit.sourceShard(targetShard);

        var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2)
            .transitionSplitTargetToNewState(testShardId(targetShard), IndexReshardingState.Split.TargetShardState.HANDOFF)
            .transitionSplitTargetToNewState(testShardId(targetShard), IndexReshardingState.Split.TargetShardState.SPLIT);
        if (randomBoolean()) {
            reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                testShardId(targetShard),
                IndexReshardingState.Split.TargetShardState.DONE
            );
        }
        final var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), origShardCount, 0))
            .reshardingMetadata(reshardingMetadata)
            .build();
        final var requestSummary = SplitShardCountSummary.forSearch(indexMetadata, sourceShard);

        assertTrue(ReshardSearchFilters.shouldFilter(requestSummary, indexMetadata, testShardId(sourceShard)));
    }

    // requests that predate the most recent split shouldn't be filtered
    public void testShouldFilterStaleRequest() {
        final var origShardCount = randomIntBetween(1, 5);
        final var newShardCount = origShardCount * 2;
        final var targetShard = randomIntBetween(origShardCount, newShardCount - 1);
        final var newSplit = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2).getSplit();
        final var sourceShard = newSplit.sourceShard(targetShard);

        // pre-split is no metadata, original shard count, or with metadata where target is CLONE through HANDOFF
        IndexReshardingMetadata reshardingMetadata = null;
        if (randomBoolean()) {
            reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2);
            if (randomBoolean()) {
                reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                    testShardId(targetShard),
                    IndexReshardingState.Split.TargetShardState.HANDOFF
                );
            }
        }
        final var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), origShardCount, 0))
            .reshardingMetadata(reshardingMetadata)
            .build();
        final var staleSummary = SplitShardCountSummary.forSearch(indexMetadata, sourceShard);

        // post-split is target at SPLIT or DONE, source at DONE, or no metadata but more shards
        IndexMetadata indexMetadataAfterSplit = null;
        if (randomBoolean()) {
            indexMetadataAfterSplit = IndexMetadata.builder("index")
                .settings(indexSettings(IndexVersion.current(), newShardCount, 0))
                .build();
        } else {
            reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(origShardCount, 2);
            reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                testShardId(targetShard),
                IndexReshardingState.Split.TargetShardState.HANDOFF
            );
            reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                testShardId(targetShard),
                IndexReshardingState.Split.TargetShardState.SPLIT
            );
            if (randomBoolean()) {
                reshardingMetadata = reshardingMetadata.transitionSplitTargetToNewState(
                    testShardId(targetShard),
                    IndexReshardingState.Split.TargetShardState.DONE
                );
                if (randomBoolean()) {
                    // source is only DONE when there are multiple source shards, otherwise metadata is immediately removed
                    if (origShardCount > 1) {
                        reshardingMetadata = reshardingMetadata.transitionSplitSourceToNewState(
                            testShardId(sourceShard),
                            IndexReshardingState.Split.SourceShardState.DONE
                        );
                    }
                }
            }
            indexMetadataAfterSplit = IndexMetadata.builder(indexMetadata).reshardingMetadata(reshardingMetadata).build();
        }

        assertFalse(ReshardSearchFilters.shouldFilter(staleSummary, indexMetadataAfterSplit, new ShardId(new Index("index", "_na_"), 0)));
    }

    private ShardId testShardId(int shardNumber) {
        return new ShardId(new Index("index", "_na_"), shardNumber);
    }
}
