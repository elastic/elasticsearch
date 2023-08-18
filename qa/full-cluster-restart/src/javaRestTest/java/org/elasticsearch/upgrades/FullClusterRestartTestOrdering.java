/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;

import java.util.Comparator;

public class FullClusterRestartTestOrdering implements Comparator<TestMethodAndParams> {
    @Override
    public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
        return Integer.compare(getOrdinal(o1), getOrdinal(o2));
    }

    private int getOrdinal(TestMethodAndParams t) {
        return ((FullClusterRestartUpgradeStatus) t.getInstanceArguments().get(0)).ordinal();
    }
}
