/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * A type of {@code Function} that takes multiple values and extracts a single value out of them. For example, {@code AVG()}.
 */
public abstract class AggregateFunction extends Function {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            Avg.ENTRY,
            Count.ENTRY,
            CountDistinct.ENTRY,
            Max.ENTRY,
            Median.ENTRY,
            MedianAbsoluteDeviation.ENTRY,
            Min.ENTRY,
            Percentile.ENTRY,
            Rate.ENTRY,
            SpatialCentroid.ENTRY,
            Sum.ENTRY,
            Top.ENTRY,
            Values.ENTRY,
            // internal functions
            ToPartial.ENTRY,
            FromPartial.ENTRY,
            WeightedAvg.ENTRY
        );
    }

    private final Expression field;
    private final List<? extends Expression> parameters;

    protected AggregateFunction(Source source, Expression field) {
        this(source, field, emptyList());
    }

    protected AggregateFunction(Source source, Expression field, List<? extends Expression> parameters) {
        super(source, CollectionUtils.combine(singletonList(field), parameters));
        this.field = field;
        this.parameters = parameters;
    }

    protected AggregateFunction(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(field);
    }

    public Expression field() {
        return field;
    }

    public List<? extends Expression> parameters() {
        return parameters;
    }

    /**
     * Returns the input expressions used in aggregation.
     * Defaults to a list containing the only the input field.
     */
    public List<Expression> inputExpressions() {
        return List.of(field);
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isExact(field, sourceText(), DEFAULT);
    }

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            AggregateFunction other = (AggregateFunction) obj;
            return Objects.equals(other.field(), field()) && Objects.equals(other.parameters(), parameters());
        }
        return false;
    }
}
