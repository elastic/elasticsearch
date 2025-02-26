/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public final class FunctionTestUtils {

    public static Literal l(Object value) {
        return new Literal(EMPTY, value, DataType.fromJava(value));
    }

    public static Literal l(Object value, DataType type) {
        return new Literal(EMPTY, value, type);
    }
}
