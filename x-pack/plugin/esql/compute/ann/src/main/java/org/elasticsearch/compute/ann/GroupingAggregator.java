/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a class that implements an aggregation function with grouping.
 * See {@link Aggregator} for more information.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface GroupingAggregator {

    IntermediateState[] value() default {};

    /**
     * Exceptions thrown by the `combine*(...)` methods to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};

    /**
     * When {@code false} (the default), the generated {@code prepareProcessRawInputPage}
     * will return {@code null} — opting out of the callback loop — when all values in an
     * input block are null, because there is nothing to aggregate.
     * <p>
     *     Set to {@code true} for aggregations like {@code FIRST} and {@code LAST} that
     *     need to process null values to correctly track position-based semantics.
     * </p>
     */
    boolean processNulls() default false;
}
