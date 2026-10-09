/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class CostBudgetTests extends ESTestCase {

    public void testThereIsNothingToSpendUntilLiveSearchesCostSomething() {
        CostBudget budget = new CostBudget(0.01, 1000);

        assertThat(budget.affordable(10, 5), equalTo(0));
        assertThat(budget.credit(), equalTo(0.0));
    }

    public void testCreditGrowsInProportionToTheCostOfLiveSearches() {
        CostBudget budget = new CostBudget(0.01, 1000);

        budget.earn(5000); // 5 seconds of live searching, 1% of it is 50 ms

        assertThat(budget.credit(), closeTo(50, 1e-9));
        assertThat("fits 5 searches of 10 ms", budget.affordable(10, 100), equalTo(5));
        assertThat("but no more than asked for", budget.affordable(10, 3), equalTo(3));
    }

    public void testExactSearchesSpendTheCredit() {
        CostBudget budget = new CostBudget(0.01, 1000);
        budget.earn(5000);

        budget.spend(30);

        assertThat(budget.credit(), closeTo(20, 1e-9));
        assertThat(budget.affordable(10, 100), equalTo(2));
    }

    public void testAQuietPeriodDoesNotStoreUpMoreThanTheCap() {
        CostBudget budget = new CostBudget(1.0, 100);

        budget.earn(1_000_000);

        assertThat(budget.credit(), equalTo(100.0));
    }

    public void testAnExpensiveSearchLeavesADebtThatIsPaidOffFirst() {
        CostBudget budget = new CostBudget(0.1, 100);
        budget.earn(100); // 10 ms
        budget.spend(50); // cost more than it was thought to

        assertThat(budget.credit(), closeTo(-40, 1e-9));
        assertThat("nothing is computed while there is a debt", budget.affordable(1, 10), equalTo(0));

        budget.earn(400); // 40 ms
        assertThat(budget.credit(), closeTo(0, 1e-9));
        budget.earn(100);
        assertThat(budget.affordable(10, 10), equalTo(1));
    }

    public void testTheDebtIsCapped() {
        CostBudget budget = new CostBudget(0.1, 100);

        budget.spend(10_000);

        assertThat(budget.credit(), equalTo(-100.0));
    }

    public void testWithoutARatioNothingIsEverEarned() {
        CostBudget budget = new CostBudget(0.0, 100);

        budget.earn(1_000_000);

        assertThat(budget.affordable(1, 10), equalTo(0));
    }

    public void testFollowsTheSettingWhenItChanges() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(QuerySamplingSettings.SAMPLING_COST_RATIO.getKey(), 0.1).build(),
            Set.of(QuerySamplingSettings.SAMPLING_COST_RATIO)
        );
        CostBudget budget = new CostBudget(0.0, 1000);

        budget.watch(clusterSettings);
        assertThat("what is set replaces what it was created with", budget.ratio(), equalTo(0.1));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.SAMPLING_COST_RATIO.getKey(), 0.4).build());
        assertThat(budget.ratio(), equalTo(0.4));
    }

    public void testTheRatioCanBeChanged() {
        CostBudget budget = new CostBudget(0.0, 1000);
        budget.earn(1000);
        assertThat(budget.credit(), equalTo(0.0));

        budget.ratio(0.5);
        budget.earn(1000);

        assertThat(budget.credit(), closeTo(500, 1e-9));
        assertThat(budget.ratio(), equalTo(0.5));
    }
}
