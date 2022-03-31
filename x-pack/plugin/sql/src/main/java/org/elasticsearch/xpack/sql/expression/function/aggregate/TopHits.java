/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Collections;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNotFoldable;

/**
 * Super class of Aggregation functions on field types other than numeric, that need to be
 * translated into an ES {@link org.elasticsearch.search.aggregations.metrics.TopHits} aggregation.
 */
public abstract class TopHits extends AggregateFunction implements OptionalArgument {

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
        TypeResolution resolution = isNotFoldable(field(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = TypeResolutions.isExact(field(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (orderField() != null) {
            resolution = isNotFoldable(orderField(), sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }

            resolution = TypeResolutions.isExact(orderField(), sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
