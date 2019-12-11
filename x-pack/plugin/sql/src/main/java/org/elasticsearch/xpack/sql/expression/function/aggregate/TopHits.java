/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.TypeResolutions;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Collections;

import static org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isNotFoldable;

/**
 * Super class of Aggregation functions on field types other than numeric, that need to be
 * translated into an ES {@link org.elasticsearch.search.aggregations.metrics.TopHits} aggregation.
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
        TypeResolution resolution = isNotFoldable(field(), sourceText(), ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = TypeResolutions.isExact(field(), sourceText(), ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (orderField() != null) {
            resolution = isNotFoldable(orderField(), sourceText(), ParamOrdinal.SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }

            resolution = TypeResolutions.isExact(orderField(), sourceText(), ParamOrdinal.SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
