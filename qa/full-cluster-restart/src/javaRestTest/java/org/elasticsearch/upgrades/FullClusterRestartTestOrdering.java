/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;

import org.elasticsearch.test.AnnotationTestOrdering;

import java.util.Comparator;

public class FullClusterRestartTestOrdering implements Comparator<TestMethodAndParams> {
    @Override
    public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
        final int result = Integer.compare(getOrdinal(o1), getOrdinal(o2));
        if (result == 0) {
            return Integer.compare(getOrderValue(o1), getOrderValue(o2));
        }
        return result;
    }

    private int getOrdinal(TestMethodAndParams t) {
        return ((FullClusterRestartUpgradeStatus) t.getInstanceArguments().get(0)).ordinal();
    }

    private int getOrderValue(TestMethodAndParams t) {
        return t.getTestMethod().isAnnotationPresent(AnnotationTestOrdering.Order.class)
            ? t.getTestMethod().getAnnotation(AnnotationTestOrdering.Order.class).value()
            : Integer.MAX_VALUE;
    }
}
