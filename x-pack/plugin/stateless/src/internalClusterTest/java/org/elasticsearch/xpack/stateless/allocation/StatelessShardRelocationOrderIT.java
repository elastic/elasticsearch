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
