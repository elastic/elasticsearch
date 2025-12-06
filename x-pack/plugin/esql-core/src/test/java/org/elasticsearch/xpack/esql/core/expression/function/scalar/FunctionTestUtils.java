/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.function.scalar;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public final class FunctionTestUtils {

    public static Literal l(Object value) {
        return l(value, DataType.fromJava(value));
    }

    public static Literal l(Object value, DataType type) {
        if ((type == DataType.TEXT || type == DataType.KEYWORD) && value instanceof String) {
            value = BytesRefs.toBytesRef(value);
        }
        return new Literal(EMPTY, value, type);
    }
}
