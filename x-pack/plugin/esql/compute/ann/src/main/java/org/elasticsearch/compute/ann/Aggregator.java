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
 * Trigger for generating {@code AggregatorFunction} implementations.
 * <p>
 *     The first thing the aggregator generation code does is find a static
 *     method names {@code init} or {@code initSingle}. That's the method that
 *     initializes an empty aggregation state. The method can either return
 *     a subclass of {@code AggregatorState} or it can return an {@code int},
 *     {@code long}, or {@code double} which will automatically be adapted into
 *     a small {@code AggregatorState} implementation that wraps a mutable reference
 *     to the primitive.
 * </p>
 * <p>
 *     Next the generation code finds a static method named {@code combine} which
 *     "combines" the state with a new value. The first parameter of this method
 *     must the state type detected in the previous section or be a primitive that
 *     lines up with one of the primitive state types from the previous section.
 *     This is called once per value to "combine" the value into the state.
 * </p>
 * <p>
 *     If the state type has a method called {@code seen} then the generated
 *     aggregation will call it at least once if it'll ever call {@code combine}.
 *     Think of this as a lower overhead way of detecting the cases where no values
 *     are ever collected.
 * </p>
 * <p>
 *     The generation code will also look for a method called {@code combineValueCount}
 *     which is called once per received block with a count of values. NOTE: We may
 *     not need this after we convert AVG into a composite operation.
 * </p>
 * <p>
 *     The generation code also looks for the optional methods {@code combineStates}
 *     and {@code evaluateFinal} which are used to combine intermediate states and
 *     produce the final output. If the first is missing then the generated code will
 *     call the {@code combine} method to combine intermediate states. If the second
 *     is missing the generated code will make a block containing the primitive from
 *     the state. If either of those don't have sensible interpretations then the code
 *     generation code will throw an error, aborting the compilation.
 * </p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Aggregator {
}
