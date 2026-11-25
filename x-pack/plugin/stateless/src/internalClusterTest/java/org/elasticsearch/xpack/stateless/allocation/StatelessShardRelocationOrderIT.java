/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

public class StatelessShardRelocationOrderIT extends AbstractStatelessPluginIntegTestCase {

    public void testStatelessPluginCustomizesShardRelocationOrder() {
        startMasterAndIndexNode();
        ensureStableCluster(1);
        var allocator = asInstanceOf(
            DesiredBalanceShardsAllocator.class,
            internalCluster().getCurrentMasterNodeInstance(ShardsAllocator.class)
        );
        var desiredBalanceReconciler = allocator.getReconciler();
        assertThat(desiredBalanceReconciler.getShardRelocationOrder().getClass(), equalTo(StatelessShardRelocationOrder.class));
    }
}
