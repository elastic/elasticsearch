/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Comparator;

/**
 * Test case ordering to be used in conjunction with {@link com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering}. Tests are
 * ordered with respect to ordinals defined with {@link Order} annotations placed on individual test methods.
 */
public class AnnotationTestOrdering implements Comparator<TestMethodAndParams> {
    @Override
    public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
        return Integer.compare(
            o1.getTestMethod().getAnnotation(Order.class).value(),
            o2.getTestMethod().getAnnotation(Order.class).value()
        );
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Inherited
    public @interface Order {
        int value();
    }
}
