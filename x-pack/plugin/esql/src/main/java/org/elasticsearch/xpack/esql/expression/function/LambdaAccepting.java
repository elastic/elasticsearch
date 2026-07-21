/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Lambda;

import java.util.List;

/**
 * Implemented by functions that accept a {@link Lambda} argument. During analysis the Analyzer
 * calls {@link #resolveLambdaParams} to obtain typed {@link Attribute}s for each lambda
 * parameter so they can be substituted into the lambda body.
 */
public interface LambdaAccepting {
    /**
     * Returns typed {@link Attribute}s for {@code lambda}'s parameters, in order, given the
     * function's already-resolved non-lambda arguments. When non-empty, the returned list must
     * have exactly the same size as {@link Lambda#parameters()}; the Analyzer rejects any other
     * size as an implementation bug.
     *
     * <p>Returns an empty list when the parameters cannot (or should not) be typed:
     * <ul>
     *   <li>the non-lambda arguments needed to determine parameter types are not yet resolved —
     *       the Analyzer will retry on the next iteration;</li>
     *   <li>the lambda has an arity the function does not support — the function must then report
     *       the user-facing error from its own {@code resolveType()}, which is where input
     *       validation belongs. The Analyzer never reports arity errors itself.</li>
     * </ul>
     */
    List<Attribute> resolveLambdaParams(Lambda lambda, List<Attribute> upstreamAttrs);
}
