/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Common interface implemented by both {@link MultiTypeEsField} (legacy, keyed by index name) and
 * {@link CompactMultiTypeEsField} (newer, keyed by source data type) so that callers that only care about
 * the existence of a per-(index|type) conversion or about the unmapped-side conversion can treat the
 * two implementations uniformly.
 */
public sealed interface UnionTypeEsField permits MultiTypeEsField, CompactMultiTypeEsField {
    /**
     * Conversion expression to apply when the field is unmapped in the index, treating it as {@link DataType#KEYWORD}, or {@code null}
     * if there is no such conversion (i.e., unmapped indices should produce {@code null}).
     */
    @Nullable
    Expression getUnmappedConversionExpression();

    Collection<Expression> getConversionExpressions();

    static Resolution resolve(TypeConflictField field, Map<String, Expression> typesToConversionExpressions) {
        DataType resolvedDataType = DataType.UNSUPPORTED;
        Map<DataType, Expression> typeToExpr = new HashMap<>();
        for (String typeName : field.getTypesToIndices().keySet()) {
            Expression convertExpr = typesToConversionExpressions.get(typeName);
            if (resolvedDataType == DataType.UNSUPPORTED) {
                resolvedDataType = convertExpr.dataType();
            } else if (resolvedDataType != convertExpr.dataType()) {
                throw new IllegalArgumentException("Resolved data type mismatch: " + resolvedDataType + " != " + convertExpr.dataType());
            }
            typeToExpr.put(DataType.fromTypeName(typeName), convertExpr);
        }
        return new Resolution(resolvedDataType, typeToExpr);
    }

    record Resolution(DataType resolvedDataType, Map<DataType, Expression> typeToExpr) {}
}
