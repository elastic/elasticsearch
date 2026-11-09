/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.QueryScopeRegistry;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Set;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The registry's factory: no scope while spilling is off or without a store; a scope takes the settings of its start.
 */
public class SpillScopesTests extends ComputeTestCase {

    private static ClusterSettings clusterSettings() {
        return new ClusterSettings(
            Settings.EMPTY,
            Set.of(SpillSettings.ENABLED, SpillSettings.QUERY_BUDGET, SpillSettings.RESULT_BUDGET, SpillSettings.STORE)
        );
    }

    private MemorySpillStore store() {
        return new MemorySpillStore(MemorySpillStore.Completion.INLINE, null, 1 << 20, "memory.limit", random());
    }

    public void testOffOrWithoutAStoreMeansNoScope() {
        ClusterSettings clusterSettings = clusterSettings();
        SpillSettings.Holder holder = new SpillSettings.Holder(clusterSettings);
        BlockFactory node = blockFactory();
        Executor codec = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        assertNull("off", SpillScopes.factory(holder, store(), node, codec).create("q", ignored -> {}));
        clusterSettings.applySettings(Settings.builder().put(SpillSettings.ENABLED.getKey(), true).build());
        assertNull("no store", SpillScopes.factory(holder, null, node, codec).create("q", ignored -> {}));
    }

    public void testAScopeTakesTheSettingsOfItsStart() {
        ClusterSettings clusterSettings = clusterSettings();
        SpillSettings.Holder holder = new SpillSettings.Holder(clusterSettings);
        clusterSettings.applySettings(
            Settings.builder().put(SpillSettings.ENABLED.getKey(), true).put(SpillSettings.QUERY_BUDGET.getKey(), "25%").build()
        );
        BlockFactory node = blockFactory();
        Executor codec = command -> { throw new AssertionError("nothing serializes here"); };
        QueryScopeRegistry.ScopeFactory factory = SpillScopes.factory(holder, store(), node, codec);
        QueryScope scope = factory.create("q", ignored -> {});
        assertNotNull(scope);
        assertTrue(scope.spillEnabled());
        long base = node.breaker().getLimit() > 0 ? node.breaker().getLimit() : JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        assertThat(scope.budgetBytes(), equalTo(base / 4));
        assertThat(scope.codecExecutor(), sameInstance(codec));

        clusterSettings.applySettings(Settings.EMPTY);
        assertTrue("a running query keeps the settings of its start", scope.spillEnabled());
        assertNull("a query starting now gets none", factory.create("q2", ignored -> {}));

        QueryScope.Lease lease = scope.tryAcquire();
        assertNotNull(lease);
        lease.close();
        assertTrue(scope.isClosing());
    }
}
