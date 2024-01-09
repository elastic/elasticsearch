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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class ShardSizesPublisher {

    private final Client client;

    public ShardSizesPublisher(Client client) {
        this.client = client;
    }

    public void publishSearchShardDiskUsage(String nodeId, Map<ShardId, ShardSize> shardSizes, ActionListener<Void> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        var request = new PublishShardSizesRequest(nodeId, shardSizes);
        client.execute(TransportPublishShardSizes.INSTANCE, request, listener.map(unused -> null));
    }
}
