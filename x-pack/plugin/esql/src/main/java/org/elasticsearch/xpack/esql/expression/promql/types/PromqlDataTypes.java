/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.types;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class PromqlDataTypes {

    public static final DataType INSTANT_VECTOR = DataType.OBJECT;
    public static final DataType RANGE_VECTOR = DataType.OBJECT;
    public static final DataType SCALAR = DataType.DOUBLE;
    public static final DataType STRING = DataType.KEYWORD;

    private PromqlDataTypes() {}

    public static DataType operationType(DataType l, DataType r) {
        if (l == r) {
            return l;
        }
        if (l == INSTANT_VECTOR || r == INSTANT_VECTOR) {
            return INSTANT_VECTOR;
        }
        if (l == RANGE_VECTOR || r == RANGE_VECTOR) {
            return INSTANT_VECTOR;
        }

        throw new QlIllegalArgumentException("Unable to determine operation type for [{}] and [{}]", l, r);
    }

    public static boolean isScalar(DataType dt) {
        return dt.isNumeric();
    }

    public static boolean isInstantVector(DataType dt) {
        // not yet implemented
        return false;
    }
}
