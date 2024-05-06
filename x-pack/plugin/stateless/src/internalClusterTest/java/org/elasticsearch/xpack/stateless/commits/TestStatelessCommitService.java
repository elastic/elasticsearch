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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.function.Supplier;

public class TestStatelessCommitService extends StatelessCommitService {

    private volatile Strategy strategy = new Strategy();

    public TestStatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        SharedBlobCacheWarmingService cacheWarmingService
    ) {
        super(settings, objectStoreService, clusterService, client, commitCleaner, cacheWarmingService);
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public ActionListener<Void> markRelocating(ShardId shardId, long minRelocatedGeneration, ActionListener<Void> listener) {
        return getStrategy().markRelocating(
            () -> super.markRelocating(shardId, minRelocatedGeneration, listener),
            shardId,
            minRelocatedGeneration,
            listener
        );
    }

    public static class Strategy {
        public ActionListener<Void> markRelocating(
            Supplier<ActionListener<Void>> originalSupplier,
            ShardId shardId,
            long minRelocatedGeneration,
            ActionListener<Void> listener
        ) {
            return originalSupplier.get();
        }
    }
}
