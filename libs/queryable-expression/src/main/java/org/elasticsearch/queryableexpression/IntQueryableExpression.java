/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import java.util.function.IntFunction;

/**
 * {@code int} flavored expression.
 */
public interface IntQueryableExpression extends QueryableExpression {
    /**
     * Transform this expression if it is a constant or return
     * {@link UnqueryableExpression} if it is not.
     */
    QueryableExpression mapConstant(IntFunction<QueryableExpression> map);
}
