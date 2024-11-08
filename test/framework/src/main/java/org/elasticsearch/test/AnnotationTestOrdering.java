/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Comparator;

/**
 * Test case ordering to be used in conjunction with {@link com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering}. Tests are
 * ordered with respect to ordinals defined with {@link Order} annotations placed on individual test methods.
 */
public class AnnotationTestOrdering implements Comparator<TestMethodAndParams> {
    @Override
    public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
        return Integer.compare(getOrderValue(o1.getTestMethod()), getOrderValue(o2.getTestMethod()));
    }

    private int getOrderValue(Method method) {
        return method.isAnnotationPresent(Order.class) ? method.getAnnotation(Order.class).value() : Integer.MAX_VALUE;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Inherited
    public @interface Order {
        int value();
    }
}
