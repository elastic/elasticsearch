/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.stats.SearchStats;

/**
 * Interface signaling to the local logical plan optimizer that the declaring expression
 * has to be replaced by a different form.
 * Implement this on {@code Function}s when:
 * <ul>
 *     <li>The expression can be rewritten to another expression on data node, with the statistics available in SearchStats.
 *     Like {@code DateTrunc} and {@code Bucket} could be rewritten to {@code RoundTo} with the min/max values on the date field.
 *     </li>
 * </ul>
 */
public interface LocalSurrogateExpression {
    /**
     * Returns the expression to be replaced by or {@code null} if this cannot be replaced.
     */
    Expression surrogate(SearchStats searchStats);
}
