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
     * function's already-resolved non-lambda arguments. The returned list must be parallel to
     * {@link Lambda#parameters()}.
     *
     * <p>Returns an empty list when the non-lambda arguments needed to determine parameter types
     * are not yet resolved — the Analyzer will retry on the next iteration.
     */
    List<Attribute> resolveLambdaParams(Lambda lambda, List<Attribute> upstreamAttrs);
}
