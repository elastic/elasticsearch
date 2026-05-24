/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;

/**
 * A {@code ScalarFunction} is a {@code Function} that takes values from some
 * operation and converts each to another value. An example would be
 * {@code ABS()}, which takes one value at a time, applies a function to the
 * value (abs) and returns a new value.
 */
public abstract class ScalarFunction extends Function {

    /**
     * Limit for the BytesRef return of functions.
     * <p>
     *     To be used when there's no CircuitBreaking, as an arbitrary measure to limit memory usage.
     * </p>
     */
    public static final long MAX_BYTES_REF_RESULT_SIZE = MB.toBytes(1);

    protected ScalarFunction(Source source) {
        super(source, emptyList());
    }

    protected ScalarFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }
}
