/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * An {@link AggregateFunction} that processes exactly one field, e.g. {@code MAX(a)} or
 * {@code PERCENTILE(a, 90)} (the percentile is a configuration parameter, not a field).
 */
public abstract class UnaryAggregateFunction extends AggregateFunction {

    protected UnaryAggregateFunction(Source source, Expression field) {
        super(source, List.of(field), emptyList());
    }

    protected UnaryAggregateFunction(Source source, Expression field, List<? extends Expression> parameters) {
        super(source, List.of(field), parameters);
    }

    protected UnaryAggregateFunction(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        List<? extends Expression> parameters
    ) {
        super(source, List.of(field), filter, window, parameters);
    }

    protected UnaryAggregateFunction(StreamInput in) throws IOException {
        // Legacy serialization format for backwards compatibility
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Legacy serialization format for backwards compatibility
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(filter());
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window());
        }
        out.writeNamedWriteableCollection(CollectionUtils.combine(parameters()));
    }

    public final Expression field() {
        return fields().get(0);
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isExact(field(), sourceText(), DEFAULT);
    }

    public UnaryAggregateFunction withField(Expression newField) {
        if (newField == field()) {
            return this;
        }
        return (UnaryAggregateFunction) replaceChildren(CollectionUtils.combine(asList(newField, filter(), window()), parameters()));
    }
}
