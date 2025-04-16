/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;

public final class Validations {

    private Validations() {}

    /**
     * Validates if the given expression is foldable - if not returns a Failure.
     */
    public static Failure isFoldable(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        TypeResolution resolution = TypeResolutions.isFoldable(e, operationName, paramOrd);
        return resolution.unresolved() ? Failure.fail(e, resolution.message()) : null;
    }
}
