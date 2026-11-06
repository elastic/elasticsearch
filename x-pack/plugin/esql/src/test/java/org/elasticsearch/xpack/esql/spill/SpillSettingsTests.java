/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * The kill switch and the budget: a snapshot is what a query keeps, the budget is a share of the breaker's limit.
 */
public class SpillSettingsTests extends ESTestCase {

    private static ClusterSettings clusterSettings() {
        return new ClusterSettings(Settings.EMPTY, Set.of(SpillSettings.ENABLED, SpillSettings.QUERY_BUDGET, SpillSettings.STORE));
    }

    public void testTheFlagGatesTheSettings() {
        assumeTrue("the flag is on in snapshot builds", SpillSettings.ESQL_SPILL_FEATURE_FLAG.isEnabled());
        assertThat(SpillSettings.settings(), containsInAnyOrder(SpillSettings.ENABLED, SpillSettings.QUERY_BUDGET, SpillSettings.STORE));
    }

    public void testTheHolderFollowsTheClusterSettingsAndASnapshotDoesNot() {
        ClusterSettings clusterSettings = clusterSettings();
        SpillSettings.Holder holder = new SpillSettings.Holder(clusterSettings);
        assertFalse(holder.get().enabled());
        assertThat(holder.get().queryBudget().getAsPercent(), equalTo(50.0));

        clusterSettings.applySettings(
            Settings.builder().put(SpillSettings.ENABLED.getKey(), true).put(SpillSettings.QUERY_BUDGET.getKey(), "25%").build()
        );
        SpillSettings snapshot = holder.get();
        assertTrue(snapshot.enabled());
        assertThat(snapshot.queryBudget().getAsPercent(), equalTo(25.0));

        clusterSettings.applySettings(Settings.EMPTY);
        assertFalse("the holder is back at the default", holder.get().enabled());
        assertTrue("the query that took the snapshot keeps it", snapshot.enabled());
    }

    public void testTheBudgetIsAShareOfTheBreakerLimitOrOfTheHeap() {
        SpillSettings settings = new SpillSettings(true, RatioValue.parseRatioValue("50%"));
        CircuitBreaker limited = new NoopCircuitBreaker("request") {
            @Override
            public long getLimit() {
                return ByteSizeValue.ofMb(100).getBytes();
            }
        };
        assertThat(settings.budgetBytes(limited), equalTo(ByteSizeValue.ofMb(50).getBytes()));
        long heap = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        assertThat(
            "a breaker without a limit falls back to the heap",
            settings.budgetBytes(new NoopCircuitBreaker("none")),
            equalTo(heap / 2)
        );
    }

    public void testTheBudgetIsAtMostTheWholeLimit() {
        expectThrows(
            Exception.class,
            () -> SpillSettings.QUERY_BUDGET.get(Settings.builder().put(SpillSettings.QUERY_BUDGET.getKey(), "150%").build())
        );
    }
}
