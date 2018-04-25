/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class ShardChangesActionTests extends ESSingleNodeTestCase {

    public void testGetOperationsBetween() throws Exception {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.translog.generation_threshold_size", new ByteSizeValue(randomIntBetween(8, 64), ByteSizeUnit.KB))
                .build();
        final IndexService indexService = createIndex("index", settings);

        final int numWrites = randomIntBetween(2, 8192);
        for (int i = 0; i < numWrites; i++) {
            client().prepareIndex("index", "doc", Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        // A number of times, get operations within a range that exists:
        int iters = randomIntBetween(8, 32);
        IndexShard indexShard = indexService.getShard(0);
        for (int iter = 0; iter < iters; iter++) {
            int min = randomIntBetween(0, numWrites - 1);
            int max = randomIntBetween(min, numWrites - 1);

            final ShardChangesAction.Response r = ShardChangesAction.getOperationsBetween(indexShard, min, max, Long.MAX_VALUE);
            /*
             * We are not guaranteed that operations are returned to us in order they are in the translog (if our read crosses multiple
             * generations) so the best we can assert is that we see the expected operations.
             */
            final Set<Long> seenSeqNos = Arrays.stream(r.getOperations()).map(Translog.Operation::seqNo).collect(Collectors.toSet());
            final Set<Long> expectedSeqNos = LongStream.range(min, max + 1).boxed().collect(Collectors.toSet());
            assertThat(seenSeqNos, equalTo(expectedSeqNos));
        }

        // get operations for a range no operations exists:
        Exception e = expectThrows(IllegalStateException.class,
                () -> ShardChangesAction.getOperationsBetween(indexShard, numWrites, numWrites + 1, Long.MAX_VALUE));
        assertThat(e.getMessage(), equalTo("not all operations between min_seq_no [" + numWrites + "] and max_seq_no [" +
                (numWrites + 1) +"] found"));

        // get operations for a range some operations do not exist:
        e = expectThrows(IllegalStateException.class,
                () -> ShardChangesAction.getOperationsBetween(indexShard, numWrites  - 10, numWrites + 10, Long.MAX_VALUE));
        assertThat(e.getMessage(), equalTo("not all operations between min_seq_no [" + (numWrites - 10) + "] and max_seq_no [" +
                (numWrites + 10) +"] found"));
    }

    public void testGetOperationsBetweenWhenShardNotStarted() throws Exception {
        IndexShard indexShard = Mockito.mock(IndexShard.class);

        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 0, "_node_id", true, ShardRoutingState.INITIALIZING);
        Mockito.when(indexShard.routingEntry()).thenReturn(shardRouting);
        expectThrows(IndexShardNotStartedException.class, () -> ShardChangesAction.getOperationsBetween(indexShard, 0, 1, Long.MAX_VALUE));
    }

    public void testGetOperationsBetweenExceedByteLimit() throws Exception {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
        final IndexService indexService = createIndex("index", settings);

        final long numWrites = 32;
        for (int i = 0; i < numWrites; i++) {
            client().prepareIndex("index", "doc", Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        final IndexShard indexShard = indexService.getShard(0);
        final ShardChangesAction.Response r = ShardChangesAction.getOperationsBetween(indexShard, 0, numWrites - 1, 256);
        assertThat(r.getOperations().length, equalTo(12));
        assertThat(r.getOperations()[0].seqNo(), equalTo(0L));
        assertThat(r.getOperations()[1].seqNo(), equalTo(1L));
        assertThat(r.getOperations()[2].seqNo(), equalTo(2L));
        assertThat(r.getOperations()[3].seqNo(), equalTo(3L));
        assertThat(r.getOperations()[4].seqNo(), equalTo(4L));
        assertThat(r.getOperations()[5].seqNo(), equalTo(5L));
        assertThat(r.getOperations()[6].seqNo(), equalTo(6L));
        assertThat(r.getOperations()[7].seqNo(), equalTo(7L));
        assertThat(r.getOperations()[8].seqNo(), equalTo(8L));
        assertThat(r.getOperations()[9].seqNo(), equalTo(9L));
        assertThat(r.getOperations()[10].seqNo(), equalTo(10L));
        assertThat(r.getOperations()[11].seqNo(), equalTo(11L));
    }

    public void testReadOpsFromLuceneIndex() throws Exception {
        // MapperService is needed by FieldsVisitor.postProcess(...)
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        final MapperService mapperService = createIndex("index", settings, "_doc").mapperService();

        try (Directory directory = newDirectory()) {
            IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
            indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, indexWriterConfig)) {
                int numDocs = randomIntBetween(10, 100);
                for (int i = 0; i < numDocs; i++) {
                    SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                    seqID.seqNo.setLongValue(i);
                    seqID.seqNoDocValue.setLongValue(i);
                    seqID.primaryTerm.setLongValue(1);

                    Document document = new Document();
                    document.add(seqID.seqNo);
                    document.add(seqID.seqNoDocValue);
                    document.add(seqID.primaryTerm);
                    document.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1L));
                    document.add(new StoredField(SourceFieldMapper.NAME, new BytesRef("{}")));
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.YES));

                    iw.addDocument(document);
                    if (randomBoolean()) {
                        iw.deleteDocuments(new Term(IdFieldMapper.NAME, Integer.toString(i)));
                    }
                }
                try(DirectoryReader ir = DirectoryReader.open(iw.w)) {
                    List<Translog.Operation> ops =
                        ShardChangesAction.getOperationsBetween(0, numDocs - 1, Long.MAX_VALUE, ir, mapperService);
                    assertThat(ops.size(), equalTo(numDocs));
                }
            }

        }
    }

    public void testReadOpsFromLuceneIndex_missingFields() throws Exception {
        // MapperService is needed by FieldsVisitor.postProcess(...)
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        final MapperService mapperService = createIndex("index", settings, "_doc").mapperService();

        try (Directory directory = newDirectory()) {
            IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
            indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, indexWriterConfig)) {
                SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                seqID.seqNo.setLongValue(0);
                seqID.seqNoDocValue.setLongValue(0);
                seqID.primaryTerm.setLongValue(1);

                Document document = new Document();
                document.add(seqID.seqNo);
                document.add(seqID.seqNoDocValue);
                document.add(seqID.primaryTerm);
                document.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1L));
                document.add(new StoredField(IdFieldMapper.NAME, Uid.encodeId("0")));
                iw.addDocument(document);

                try(DirectoryReader ir = DirectoryReader.open(iw.w)) {
                    Exception e = expectThrows(IllegalArgumentException.class,
                        () -> ShardChangesAction.getOperationsBetween(0, 0, Long.MAX_VALUE, ir, mapperService));
                    assertThat(e.getMessage(), equalTo("no source found for document with id [0]"));
                }
            }
        }
    }

}
