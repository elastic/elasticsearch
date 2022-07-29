/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShardSplittingQueryTests extends ESTestCase {
    public void testSplitOnID() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards = randomIntBetween(2, 10);
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0)
            .build();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(metadata);
        int targetShardId = randomIntBetween(0, numShards - 1);
        boolean hasNested = randomBoolean();

        for (int j = 0; j < numDocs; j++) {
            writer.addDocuments(luceneDocs(indexRouting, hasNested, j, null));
        }
        writer.commit();
        writer.close();

        assertSplit(dir, metadata, targetShardId, hasNested);
        dir.close();
    }

    public void testSplitOnRouting() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards = randomIntBetween(2, 10);
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0)
            .build();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(metadata);
        boolean hasNested = randomBoolean();
        int targetShardId = randomIntBetween(0, numShards - 1);

        for (int j = 0; j < numDocs; j++) {
            writer.addDocuments(luceneDocs(indexRouting, hasNested, j, randomRealisticUnicodeOfCodepointLengthBetween(1, 5)));
        }
        writer.commit();
        writer.close();
        assertSplit(dir, metadata, targetShardId, hasNested);
        dir.close();
    }

    public void testSplitOnIdOrRouting() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards = randomIntBetween(2, 10);
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0)
            .build();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(metadata);
        boolean hasNested = randomBoolean();
        int targetShardId = randomIntBetween(0, numShards - 1);

        for (int j = 0; j < numDocs; j++) {
            writer.addDocuments(
                luceneDocs(indexRouting, hasNested, j, randomBoolean() ? null : randomRealisticUnicodeOfCodepointLengthBetween(1, 5))
            );
        }
        writer.commit();
        writer.close();
        assertSplit(dir, metadata, targetShardId, hasNested);
        dir.close();
    }

    public void testSplitOnRoutingPartitioned() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards = randomIntBetween(2, 10);
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .routingPartitionSize(randomIntBetween(1, 10))
            .numberOfReplicas(0)
            .build();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(metadata);
        boolean hasNested = randomBoolean();
        int targetShardId = randomIntBetween(0, numShards - 1);

        for (int j = 0; j < numDocs; j++) {
            writer.addDocuments(luceneDocs(indexRouting, hasNested, j, randomRealisticUnicodeOfCodepointLengthBetween(1, 5)));
        }
        writer.commit();
        writer.close();
        assertSplit(dir, metadata, targetShardId, hasNested);
        dir.close();
    }

    void assertSplit(Directory dir, IndexMetadata metadata, int targetShardId, boolean hasNested) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            final Weight splitWeight = searcher.createWeight(
                searcher.rewrite(new ShardSplittingQuery(metadata, targetShardId, hasNested)),
                ScoreMode.COMPLETE_NO_SCORES,
                1f
            );
            final List<LeafReaderContext> leaves = reader.leaves();
            for (final LeafReaderContext ctx : leaves) {
                Scorer scorer = splitWeight.scorer(ctx);
                DocIdSetIterator iterator = scorer.iterator();
                SortedNumericDocValues shard_id = ctx.reader().getSortedNumericDocValues("shard_id");
                int numExpected = 0;
                while (shard_id.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    if (targetShardId == shard_id.nextValue()) {
                        numExpected++;
                    }
                }
                if (numExpected == ctx.reader().maxDoc()) {
                    // all docs belong in this shard
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
                } else {
                    shard_id = ctx.reader().getSortedNumericDocValues("shard_id");
                    int doc;
                    int numActual = 0;
                    int lastDoc = 0;
                    while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        lastDoc = doc;
                        while (shard_id.nextDoc() < doc) {
                            long shardID = shard_id.nextValue();
                            assertEquals(shardID, targetShardId);
                            numActual++;
                        }
                        assertEquals(shard_id.docID(), doc);
                        long shardID = shard_id.nextValue();
                        BytesRef id = reader.document(doc).getBinaryValue("_id");
                        String actualId = Uid.decodeId(id.bytes, id.offset, id.length);
                        assertNotEquals(ctx.reader() + " docID: " + doc + " actualID: " + actualId, shardID, targetShardId);
                    }
                    if (lastDoc < ctx.reader().maxDoc()) {
                        // check the last docs in the segment and make sure they all have the right shard id
                        while (shard_id.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            long shardID = shard_id.nextValue();
                            assertEquals(shardID, targetShardId);
                            numActual++;
                        }
                    }

                    assertEquals(numExpected, numActual);
                }
            }
        }
    }

    private Iterable<Iterable<IndexableField>> luceneDocs(IndexRouting indexRouting, boolean nested, int id, @Nullable String routing) {
        if (nested == false) {
            return List.of(topLevel(indexRouting, id, routing));
        }
        int shardId = shardId(indexRouting, id, routing);
        List<Iterable<IndexableField>> docs = new ArrayList<>();
        int numNested = randomIntBetween(0, 10);
        for (int i = 0; i < numNested; i++) {
            docs.add(
                Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(id)), Field.Store.YES),
                    new StringField(NestedPathFieldMapper.NAME, "__nested", Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId)
                )
            );
        }
        docs.add(topLevel(indexRouting, id, routing));
        return docs;
    }

    private Iterable<IndexableField> topLevel(IndexRouting indexRouting, int id, @Nullable String routing) {
        LuceneDocument topLevel = new LuceneDocument();
        topLevel.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(id)), Field.Store.YES));
        topLevel.add(new SortedNumericDocValuesField("shard_id", shardId(indexRouting, id, routing)));
        if (routing != null) {
            topLevel.add(new StringField(RoutingFieldMapper.NAME, routing, Field.Store.YES));
        }
        SeqNoFieldMapper.SequenceIDFields.emptySeqID().addFields(topLevel);
        return topLevel;
    }

    private int shardId(IndexRouting indexRouting, int id, @Nullable String routing) {
        return indexRouting.getShard(Integer.toString(id), routing);
    }
}
