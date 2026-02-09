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

package org.elasticsearch.xpack.stateless.health;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Set;

public class StatelessShardsHealthPlugin extends Plugin implements HealthPlugin {

    private final SetOnce<ShardsAvailabilityHealthIndicatorService> shardHealthService = new SetOnce<>();

    public StatelessShardsHealthPlugin() {}

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.shardHealthService.set(
            new StatelessShardsAvailabilityHealthIndicatorService(
                services.clusterService(),
                services.allocationService(),
                services.systemIndices(),
                services.projectResolver()
            )
        );
        return Set.of(this.shardHealthService.get());
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return Set.of(this.shardHealthService.get());
    }
}
