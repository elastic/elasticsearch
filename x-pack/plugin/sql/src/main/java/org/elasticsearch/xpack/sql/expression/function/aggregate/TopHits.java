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

public abstract class TopHits extends AggregateFunction {

    TopHits(Source source, Expression field, Expression sortField) {
        super(source, field, sortField != null ? Collections.singletonList(sortField) : Collections.emptyList());
    }

    public Expression sortField() {
        return parameters().isEmpty() ? null : parameters().get(0);
    }

    public DataType sortFieldDataType() {
        return parameters().isEmpty() ? null : parameters().get(0).dataType();
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
                functionName(), field().dataType().esType));
        }

        if (sortField() != null) {
            if (sortField().foldable()) {
                return new TypeResolution(format(null, "Second argument of [{}] must be a table column, found constant [{}]",
                    functionName(),
                    Expressions.name(sortField())));
            }
            try {
                ((FieldAttribute) sortField()).exactAttribute();
            } catch (MappingException ex) {
                return new TypeResolution(format(null, "[{}] cannot operate on second argument field of data type [{}]",
                    functionName(), sortField().dataType().esType));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
