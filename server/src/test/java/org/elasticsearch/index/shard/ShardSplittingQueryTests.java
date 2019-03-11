/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
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
        int numShards =  randomIntBetween(2, 10);
        IndexMetaData metaData = IndexMetaData.builder("test")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0).build();
        int targetShardId = randomIntBetween(0, numShards-1);
        boolean hasNested = randomBoolean();
        for (int j = 0; j < numDocs; j++) {
            int shardId = OperationRouting.generateShardId(metaData, Integer.toString(j), null);
            if (hasNested) {
                List<Iterable<IndexableField>> docs = new ArrayList<>();
                int numNested = randomIntBetween(0, 10);
                for (int i = 0; i < numNested; i++) {
                    docs.add(Arrays.asList(
                        new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                        new StringField(TypeFieldMapper.NAME, "__nested", Field.Store.YES),
                        new SortedNumericDocValuesField("shard_id", shardId)
                    ));
                }
                docs.add(Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                ));
                writer.addDocuments(docs);
            } else {
                writer.addDocument(Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                ));
            }
        }
        writer.commit();
        writer.close();


        assertSplit(dir, metaData, targetShardId, hasNested);
        dir.close();
    }

    public void testSplitOnRouting() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards =  randomIntBetween(2, 10);
        IndexMetaData metaData = IndexMetaData.builder("test")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0).build();
        boolean hasNested = randomBoolean();
        int targetShardId = randomIntBetween(0, numShards-1);
        for (int j = 0; j < numDocs; j++) {
            String routing = randomRealisticUnicodeOfCodepointLengthBetween(1, 5);
            final int shardId = OperationRouting.generateShardId(metaData, null, routing);
            if (hasNested) {
                List<Iterable<IndexableField>> docs = new ArrayList<>();
                int numNested = randomIntBetween(0, 10);
                for (int i = 0; i < numNested; i++) {
                    docs.add(Arrays.asList(
                        new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                        new StringField(TypeFieldMapper.NAME, "__nested", Field.Store.YES),
                        new SortedNumericDocValuesField("shard_id", shardId)
                    ));
                }
                docs.add(Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new StringField(RoutingFieldMapper.NAME, routing, Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                ));
                writer.addDocuments(docs);
            } else {
                writer.addDocument(Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new StringField(RoutingFieldMapper.NAME, routing, Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                ));
            }
        }
        writer.commit();
        writer.close();
        assertSplit(dir, metaData, targetShardId, hasNested);
        dir.close();
    }

    public void testSplitOnIdOrRouting() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards = randomIntBetween(2, 10);
        IndexMetaData metaData = IndexMetaData.builder("test")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .numberOfReplicas(0).build();
        boolean hasNested = randomBoolean();
        int targetShardId = randomIntBetween(0, numShards-1);
        for (int j = 0; j < numDocs; j++) {
            Iterable<IndexableField> rootDoc;
            final int shardId;
            if (randomBoolean()) {
                String routing = randomRealisticUnicodeOfCodepointLengthBetween(1, 5);
                shardId = OperationRouting.generateShardId(metaData, null, routing);
                rootDoc = Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new StringField(RoutingFieldMapper.NAME, routing, Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                );
            } else {
                shardId = OperationRouting.generateShardId(metaData, Integer.toString(j), null);
                rootDoc = Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                );
            }

            if (hasNested) {
                List<Iterable<IndexableField>> docs = new ArrayList<>();
                int numNested = randomIntBetween(0, 10);
                for (int i = 0; i < numNested; i++) {
                    docs.add(Arrays.asList(
                        new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                        new StringField(TypeFieldMapper.NAME, "__nested", Field.Store.YES),
                        new SortedNumericDocValuesField("shard_id", shardId)
                    ));
                }
                docs.add(rootDoc);
                writer.addDocuments(docs);
            } else {
                writer.addDocument(rootDoc);
            }
        }
        writer.commit();
        writer.close();
        assertSplit(dir, metaData, targetShardId, hasNested);
        dir.close();
    }


    public void testSplitOnRoutingPartitioned() throws IOException {
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        Directory dir = newFSDirectory(createTempDir());
        final int numDocs = randomIntBetween(50, 100);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        int numShards =  randomIntBetween(2, 10);
        IndexMetaData metaData = IndexMetaData.builder("test")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numShards)
            .setRoutingNumShards(numShards * 1000000)
            .routingPartitionSize(randomIntBetween(1, 10))
            .numberOfReplicas(0).build();
        boolean hasNested = randomBoolean();
        int targetShardId = randomIntBetween(0, numShards-1);
        for (int j = 0; j < numDocs; j++) {
            String routing = randomRealisticUnicodeOfCodepointLengthBetween(1, 5);
            final int shardId = OperationRouting.generateShardId(metaData, Integer.toString(j), routing);

            if (hasNested) {
                List<Iterable<IndexableField>> docs = new ArrayList<>();
                int numNested = randomIntBetween(0, 10);
                for (int i = 0; i < numNested; i++) {
                    docs.add(Arrays.asList(
                        new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                        new StringField(TypeFieldMapper.NAME, "__nested", Field.Store.YES),
                        new SortedNumericDocValuesField("shard_id", shardId)
                    ));
                }
                docs.add(Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new StringField(RoutingFieldMapper.NAME, routing, Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                ));
                writer.addDocuments(docs);
            } else {
                writer.addDocument(Arrays.asList(
                    new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(j)), Field.Store.YES),
                    new StringField(RoutingFieldMapper.NAME, routing, Field.Store.YES),
                    new SortedNumericDocValuesField("shard_id", shardId),
                    sequenceIDFields.primaryTerm
                ));
            }
        }
        writer.commit();
        writer.close();
        assertSplit(dir, metaData, targetShardId, hasNested);
        dir.close();
    }




    void assertSplit(Directory dir, IndexMetaData metaData, int targetShardId, boolean hasNested) throws IOException {
        try (IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            final Weight splitWeight = searcher.createWeight(searcher.rewrite(new ShardSplittingQuery(metaData, targetShardId, hasNested)),
                ScoreMode.COMPLETE_NO_SCORES, 1f);
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
}
