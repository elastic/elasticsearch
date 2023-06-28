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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DataStream.TIMESTAMP_FIELD;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ShardSizeStatsReaderIT extends AbstractStatelessIntegTestCase {

    public void testShardSizeWithoutTimestampField() throws InterruptedException {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        indexRandom(index, builder -> builder.setSource("value", randomIdentifier()));

        var shardId = resolveShardId(index);
        var shard = findSearchShard(shardId.getIndex(), shardId.id());
        var size = getShardSize(shard, randomNonNegativeLong());
        // shards with no timestamp are considered interactive
        assertThat(size.interactiveSize(), greaterThan(0L));
        assertThat(size.nonInteractiveSize(), equalTo(0L));
    }

    public void testShardSizeWithNewDataStreamEntries() throws InterruptedException {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var now = System.currentTimeMillis();
        var interactiveAge = TimeValue.timeValueDays(7).millis();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        indexRandom(index, builder -> builder.setSource(TIMESTAMP_FIELD.getName(), randomLongBetween(now - interactiveAge + 1, now)));

        var shardId = resolveShardId(index);
        var shard = findSearchShard(shardId.getIndex(), shardId.id());
        var size = getShardSize(shard, now);
        // shards with no timestamp are considered interactive
        assertThat(size.interactiveSize(), greaterThan(0L));
        assertThat(size.nonInteractiveSize(), equalTo(0L));
    }

    public void testShardSizeWithOldDataStreamEntries() throws InterruptedException {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        var now = System.currentTimeMillis();
        var interactiveAge = TimeValue.timeValueDays(7).millis();

        var index = randomIdentifier();
        createIndex(index, indexSettings(1, 1).build());
        indexRandom(index, builder -> builder.setSource(TIMESTAMP_FIELD.getName(), randomLongBetween(0, now - interactiveAge - 1)));

        var shardId = resolveShardId(index);
        var shard = findSearchShard(shardId.getIndex(), shardId.id());
        var size = getShardSize(shard, now);
        // shards with no timestamp are considered interactive
        assertThat(size.interactiveSize(), equalTo(0L));
        assertThat(size.nonInteractiveSize(), greaterThan(0L));
    }

    private void indexRandom(String index, Consumer<IndexRequestBuilder> requestBuilder) throws InterruptedException {
        int count = randomIntBetween(1, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[count];
        for (int i = 0; i < count; i++) {
            requestBuilder.accept(builders[i] = client().prepareIndex(index));
        }
        indexRandom(true, Arrays.asList(builders));
    }

    private static ShardId resolveShardId(String index) {
        return new ShardId(internalCluster().getInstance(ClusterService.class).state().routingTable().index(index).getIndex(), 0);
    }

    private static ShardSize getShardSize(IndexShard shard, long currentTimeMillis) {
        return new ShardSizeStatsReader(createBuiltInClusterSettings(), () -> currentTimeMillis).getShardSize(shard);
    }
}
