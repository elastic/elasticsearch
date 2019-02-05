/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.analysis.index.MappingException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Collections;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Super class of Aggregation functions on field types other than numeric, that need to be
 * translated into an ES {@link org.elasticsearch.search.aggregations.metrics.tophits.TopHits} aggregation.
 */
public abstract class TopHits extends AggregateFunction {

    TopHits(Source source, Expression field, Expression sortField) {
        super(source, field, sortField != null ? Collections.singletonList(sortField) : Collections.emptyList());
    }

    public Expression orderField() {
        return parameters().isEmpty() ? null : parameters().get(0);
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        if (field().foldable()) {
            return new TypeResolution(format(null, "First argument of [{}] must be a table column, found constant [{}]",
                functionName(),
                Expressions.name(field())));
        }
        try {
            ((FieldAttribute) field()).exactAttribute();
        } catch (MappingException ex) {
            return new TypeResolution(format(null, "[{}] cannot operate on first argument field of data type [{}]",
                functionName(), field().dataType().typeName));
        }

        if (orderField() != null) {
            if (orderField().foldable()) {
                return new TypeResolution(format(null, "Second argument of [{}] must be a table column, found constant [{}]",
                    functionName(),
                    Expressions.name(orderField())));
            }
            try {
                ((FieldAttribute) orderField()).exactAttribute();
            } catch (MappingException ex) {
                return new TypeResolution(format(null, "[{}] cannot operate on second argument field of data type [{}]",
                    functionName(), orderField().dataType().typeName));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
