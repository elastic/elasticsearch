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
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DataStream.TIMESTAMP_FIELD_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    public void testShardSizesWithAutodetectedMappingAndNewEntries() throws Exception {
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

    public void testShardSizesWithAutodetectedMappingAndOldEntries() throws Exception {
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

    public void testShardSizeWithDefinedMappingAndNewDataStreamEntries() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var now = System.currentTimeMillis();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1), createTimestampMapping(randomBoolean()));
        indexRandom(
            index,
            builder -> builder.setSource(TIMESTAMP_FIELD_NAME, getRandomTimestamp(now - DEFAULT_BOOST_WINDOW.millis() + 1, now))
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

    public void testShardSizeWithDefinedMappingAndOldDataStreamEntries() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var now = System.currentTimeMillis();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1), createTimestampMapping(randomBoolean()));
        indexRandom(
            index,
            builder -> builder.setSource(TIMESTAMP_FIELD_NAME, getRandomTimestamp(0, now - DEFAULT_BOOST_WINDOW.millis() - 1))
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

    public void testShardSizesWithClosedIndices() throws Exception {
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
        var expectedShardSize = getShardSizeFromObjectStore(shard);

        assertAcked(client().admin().indices().prepareClose(index));
        ensureGreen(index);

        var closedShard = findSearchShard(shardId.getIndex(), shardId.id());
        assertBusy(() -> {
            var shardSize = getShardSize(closedShard, now);
            assertThat(shardSize.interactiveSizeInBytes(), equalTo(0L));
            assertThat(shardSize.nonInteractiveSizeInBytes(), equalTo(expectedShardSize));
        });
    }

    private static Object getRandomTimestamp(long min, long max) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> randomLongBetween(min, max);
            case 1 -> {
                var instant = Instant.ofEpochMilli(randomLongBetween(min, max));
                yield DateFormatter.forPattern(FormatNames.STRICT_DATE_TIME.getName()).format(instant);
            }
            case 2 -> {
                var instant = Instant.ofEpochMilli(
                    randomLongBetween(
                        Instant.ofEpochMilli(min).truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS).toEpochMilli(),
                        Instant.ofEpochMilli(max).truncatedTo(ChronoUnit.DAYS).toEpochMilli()
                    )
                );
                yield DateFormatter.forPattern(FormatNames.STRICT_DATE.getName()).format(instant);
            }
            default -> throw new AssertionError("Unreachable");
        };
    }

    private void createIndex(String name, Settings.Builder settings, String mapping) {
        assertAcked(prepareCreate(name).setSettings(settings).setMapping(mapping));
    }

    private static String createTimestampMapping(boolean nano) {
        return Strings.format("""
            {
                "properties": {
                  "%s": {
                    "type":   "%s"
                  }
                }
            }
            """, TIMESTAMP_FIELD_NAME, nano ? "date_nanos" : "date");
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
        var discoveryNode = internalCluster().clusterService().state().nodes().get(shard.routingEntry().currentNodeId()).getName();
        var clusterService = internalCluster().getInstance(ClusterService.class, discoveryNode);
        var indicesService = internalCluster().getInstance(IndicesService.class, discoveryNode);
        return new ShardSizeStatsReader(clusterService, indicesService, () -> currentTimeMillis).getShardSize(shard, DEFAULT_BOOST_WINDOW);
    }

    private long getShardSizeFromObjectStore(IndexShard shard) throws IOException {
        ObjectStoreService objectStoreService = internalCluster().getAnyMasterNodeInstance(ObjectStoreService.class);
        BlobContainer blobContainerForCommit = objectStoreService.getBlobContainer(shard.shardId(), shard.getOperationPrimaryTerm());
        SegmentInfos segmentInfos = Lucene.readSegmentInfos(shard.store().directory());
        String commitFile = StatelessCompoundCommit.blobNameFromGeneration(segmentInfos.getGeneration());
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(operationPurpose, commitFile)),
            blobContainerForCommit.listBlobs(operationPurpose).get(commitFile).length()
        );
        long size = 0L;
        for (String localFile : segmentInfos.files(false)) {
            size += commit.commitFiles().get(localFile).fileLength();
        }
        return size;
    }
}
