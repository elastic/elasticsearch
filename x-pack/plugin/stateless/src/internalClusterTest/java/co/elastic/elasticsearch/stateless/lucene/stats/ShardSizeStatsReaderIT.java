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

package co.elastic.elasticsearch.stateless.lucene.stats;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DataStream.TIMESTAMP_FIELD_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class ShardSizeStatsReaderIT extends AbstractStatelessIntegTestCase {

    private static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(7);

    public void testShardSizeWithoutTimestampField() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        indexRandom(index, builder -> builder.setSource("value", randomIdentifier()));

        var shardId = resolveShardId(index);
        var shard = findSearchShard(shardId.getIndex(), shardId.id());
        assertBusy(() -> {
            var size = getShardSize(shard, randomNonNegativeLong());
            // shards with no timestamp are considered interactive
            assertThat(size.interactiveSizeInBytes(), equalTo(getShardSizeFromObjectStore(shard)));
            assertThat(size.nonInteractiveSizeInBytes(), equalTo(0L));
        });
    }

    public void testShardSizeWithNewDataStreamEntries() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var now = System.currentTimeMillis();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        indexRandom(
            index,
            builder -> builder.setSource(TIMESTAMP_FIELD_NAME, randomLongBetween(now - DEFAULT_BOOST_WINDOW.millis() + 1, now))
        );

        var shardId = resolveShardId(index);
        var shard = findSearchShard(shardId.getIndex(), shardId.id());
        assertBusy(() -> {
            var size = getShardSize(shard, now);
            // shards with no timestamp are considered interactive
            assertThat(size.interactiveSizeInBytes(), equalTo(getShardSizeFromObjectStore(shard)));
            assertThat(size.nonInteractiveSizeInBytes(), equalTo(0L));
        });
    }

    public void testShardSizeWithOldDataStreamEntries() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var now = System.currentTimeMillis();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        indexRandom(
            index,
            builder -> builder.setSource(TIMESTAMP_FIELD_NAME, randomLongBetween(0, now - DEFAULT_BOOST_WINDOW.millis() - 1))
        );

        var shardId = resolveShardId(index);
        var shard = findSearchShard(shardId.getIndex(), shardId.id());
        assertBusy(() -> {
            var size = getShardSize(shard, now);
            // shards with no timestamp are considered interactive
            assertThat(size.interactiveSizeInBytes(), equalTo(0L));
            assertThat(size.nonInteractiveSizeInBytes(), equalTo(getShardSizeFromObjectStore(shard)));
        });
    }

    private void indexRandom(String index, Consumer<IndexRequestBuilder> requestConfigurer) {
        var bulkRequest = client().prepareBulk();
        int count = randomIntBetween(1, 100);
        for (int i = 0; i < count; i++) {
            var indexRequestBuilder = client().prepareIndex(index);
            requestConfigurer.accept(indexRequestBuilder);
            bulkRequest.add(indexRequestBuilder);
        }
        assertNoFailures(bulkRequest.get());
        refresh(index);
    }

    private static ShardId resolveShardId(String index) {
        return new ShardId(internalCluster().getInstance(ClusterService.class).state().routingTable().index(index).getIndex(), 0);
    }

    private static ShardSize getShardSize(IndexShard shard, long currentTimeMillis) {
        return new ShardSizeStatsReader(() -> currentTimeMillis, null).getShardSize(shard, DEFAULT_BOOST_WINDOW);
    }

    private long getShardSizeFromObjectStore(IndexShard shard) throws IOException {
        ObjectStoreService objectStoreService = internalCluster().getAnyMasterNodeInstance(ObjectStoreService.class);
        BlobContainer blobContainerForCommit = objectStoreService.getBlobContainer(shard.shardId(), shard.getOperationPrimaryTerm());
        SegmentInfos segmentInfos = Lucene.readSegmentInfos(shard.store().directory());
        String commitFile = StatelessCompoundCommit.NAME + segmentInfos.getGeneration();
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
            blobContainerForCommit.listBlobs().get(commitFile).length()
        );
        long size = 0L;
        for (String localFile : segmentInfos.files(false)) {
            size += commit.commitFiles().get(localFile).fileLength();
        }
        return size;
    }
}
