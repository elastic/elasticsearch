/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract sealed class UnionTypeEsField extends EsField permits MultiTypeEsField, CompactMultiTypeEsField {
    public UnionTypeEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, esDataType, properties, aggregatable, timeSeriesFieldType);
    }

    /**
     * Conversion expression to apply when the field is unmapped in the index (treating it as {@link DataType#KEYWORD}) or {@code null} if
     * there is no such conversion (i.e., unmapped indices should produce {@code null}).
     */
    @Nullable
    public abstract Expression getUnmappedConversionExpression();

    public abstract Collection<Expression> getConversionExpressions();

    /**
     * Wraps an existing union-type field's per-(index|type) conversions with another conversion expression on top, so the
     * composite expression first does the original cast then the additional cast.
     */
    public abstract UnionTypeEsField rewrapWithCast(Expression convertExpression);

    /**
     * Returns a copy of this field with the given unmapped conversion expression.
     */
    public abstract UnionTypeEsField withPotentiallyUnmappedExpression(Expression unmappedExpression);

    // Utility functions used by implementors.
    static <K> Map<K, Expression> replaceChildrenWithExpressionField(Map<K, Expression> map, Expression expression) {
        return Maps.transformValues(map, e -> expression.replaceChildren(List.of(((AbstractConvertFunction) e).field())));
    }

    record Resolution(DataType resolvedDataType, Map<DataType, Expression> typeToExpr) {}

    static Resolution resolve(TypeConflictedField field, Map<String, Expression> typesToConversionExpressions) {
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
}
