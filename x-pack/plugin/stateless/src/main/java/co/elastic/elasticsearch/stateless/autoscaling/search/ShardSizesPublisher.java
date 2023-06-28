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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ShardSizesPublisher {

    private final NodeClient client;
    private final Supplier<String> nodeIdSupplier;
    private final ThreadPool threadPool;

    private final AtomicLong seqNoSupplier = new AtomicLong();

    public ShardSizesPublisher(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this(client, () -> clusterService.state().nodes().getLocalNodeId(), threadPool);
    }

    public ShardSizesPublisher(Client client, Supplier<String> nodeIdSupplier, ThreadPool threadPool) {
        this.client = (NodeClient) client;
        this.nodeIdSupplier = nodeIdSupplier;
        this.threadPool = threadPool;
    }

    public void publishSearchShardDiskUsage(Map<ShardId, ShardSize> shardSizes, ActionListener<Void> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            client.execute(
                PublishShardSizesAction.INSTANCE,
                new PublishShardSizesRequest(nodeIdSupplier.get(), seqNoSupplier.incrementAndGet(), shardSizes),
                listener.map(unused -> null)
            );
        });
    }
}
