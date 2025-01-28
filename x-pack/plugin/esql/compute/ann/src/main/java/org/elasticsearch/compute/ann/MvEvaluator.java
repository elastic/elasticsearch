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
 * Implement an evaluator for a function reducing multivalued fields into a
 * single valued field from a static {@code process} method.
 * <p>
 *     Annotated methods can have three "shapes":
 * </p>
 * <ul>
 *     <li>pairwise processing</li>
 *     <li>accumulator processing</li>
 *     <li>position at a time processing</li>
 * </ul>
 * <p>
 *     Pairwise processing is <strong>generally</strong> simpler and looks
 *     like {@code int process(int current, int next)}. Use it when the result
 *     is a primitive.
 * </p>
 * <p>
 *     Accumulator processing is a bit more complex and looks like
 *     {@code void process(State state, int v)} and it useful when you need to
 *     accumulate more data than fits in a primitive result. Think Kahan summation.
 * </p>
 * <p>
 *     Position at a time processing just hands the block, start index, and end index
 *     to the processor and is useful when none of the others fit. It looks like
 *     {@code long process(LongBlock block, int start, int end)} and is the most
 *     flexible, but the choice where the code generation does the least work for you.
 *     You should only use this if pairwise and state based processing aren't options.
 * </p>
 * <p>
 *     Pairwise and accumulator processing support a {@code finish = "finish_method"}
 *     parameter on the annotation which is used to, well, "finish" processing after
 *     all values have been received. Again, think reading the sum from the
 *     Kahan summation. Or doing the division for an "average" operation.
 *     This method is required for accumulator processing.
 * </p>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface MvEvaluator {
    /**
     * Extra part of the name of the evaluator. Use for disambiguating
     * when there are multiple ways to evaluate a function.
     */
    String extraName() default "";

    /**
     * Optional method called to convert state into result.
     */
    String finish() default "";

    /**
     * Optional method called to process single valued fields. If this
     * is missing then blocks containing only single valued fields will
     * be returned exactly as is. If this is present then single valued
     * fields will not call the process or finish function and instead
     * just call this function.
     */
    String single() default "";

    /**
     * Optional method called to process blocks whose values are sorted
     * in ascending order.
     */
    String ascending() default "";

    /**
     * Exceptions thrown by the process method to catch and convert
     * into a warning and turn into a null value.
     */
    Class<? extends Exception>[] warnExceptions() default {};
}
