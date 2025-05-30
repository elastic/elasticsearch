/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class SparseVectorIndexOptionsUpgradeIT extends AbstractUpgradeTestCase {

    private final boolean testHasIndexOptions;
    private final boolean testIndexShouldPrune;
    private final boolean testQueryShouldNotPrune;
    private final boolean usePreviousIndexVersion;

    public SparseVectorIndexOptionsUpgradeIT(
        boolean setIndexOptions,
        boolean setIndexShouldPrune,
        boolean setQueryShouldNotPrune,
        boolean usePreviousIndexVersion
    ) {
        this.testHasIndexOptions = setIndexOptions;
        this.testIndexShouldPrune = setIndexShouldPrune;
        this.testQueryShouldNotPrune = setQueryShouldNotPrune;
        this.usePreviousIndexVersion = usePreviousIndexVersion;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        // create a matrix of all combinations
        // of our first three parameters
        for (int i = 0; i < 8; i++) {
            params.add(new Object[] { (i & 1) == 0, (i & 2) == 0, (i & 4) == 0, false });
        }
        // and add in overrides for the previous index versions
        params.add(new Object[] { false, false, false, true });
        params.add(new Object[] { false, false, true, true });
        return params;
    }

    public void testItPrunesTokensIfIndexOptions() {
        Assert.assertFalse(true);
    }

    public void testBehavioralAnalyticsDataRetention() throws Exception {
        Assert.assertFalse(true);
    }
}
