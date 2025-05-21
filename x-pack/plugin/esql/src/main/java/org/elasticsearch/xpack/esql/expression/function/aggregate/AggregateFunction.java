/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * A type of {@code Function} that takes multiple values and extracts a single value out of them. For example, {@code AVG()}.
 */
public abstract class AggregateFunction extends Function implements PostAnalysisPlanVerificationAware {

    private final Expression field;
    private final List<? extends Expression> parameters;
    private final Expression filter;

    protected AggregateFunction(Source source, Expression field) {
        this(source, field, Literal.TRUE, emptyList());
    }

    protected AggregateFunction(Source source, Expression field, List<? extends Expression> parameters) {
        this(source, field, Literal.TRUE, parameters);
    }

    protected AggregateFunction(Source source, Expression field, Expression filter, List<? extends Expression> parameters) {
        super(source, CollectionUtils.combine(asList(field, filter), parameters));
        this.field = field;
        this.filter = filter;
        this.parameters = parameters;
    }

    protected AggregateFunction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readNamedWriteable(Expression.class) : Literal.TRUE,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? in.readNamedWriteableCollectionAsList(Expression.class)
                : emptyList()
        );
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeNamedWriteable(filter);
            out.writeNamedWriteableCollection(parameters);
        } else {
            deprecatedWriteParams(out);
        }
    }

    @Deprecated(since = "8.16", forRemoval = true)
    protected void deprecatedWriteParams(StreamOutput out) throws IOException {
        //
    }

    public Expression field() {
        return field;
    }

    public List<? extends Expression> parameters() {
        return parameters;
    }

    public boolean hasFilter() {
        return filter != null
            && (filter.foldable() == false || Boolean.TRUE.equals(filter.fold(FoldContext.small() /* TODO remove me */)) == false);
    }

    public Expression filter() {
        return filter;
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isExact(field, sourceText(), DEFAULT);
    }

    /**
     * Attach a filter to the aggregate function.
     */
    public abstract AggregateFunction withFilter(Expression filter);

    public AggregateFunction withParameters(List<? extends Expression> parameters) {
        if (parameters == this.parameters) {
            return this;
        }
        return (AggregateFunction) replaceChildren(CollectionUtils.combine(asList(field, filter), parameters));
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
            return Objects.equals(other.field(), field())
                && Objects.equals(other.filter(), filter())
                && Objects.equals(other.parameters(), parameters());
        }
        return false;
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (p, failures) -> {
            // `dedup` for now is not exposed as a command,
            // so allowing aggregate functions for dedup explicitly is just an internal implementation detail
            if ((p instanceof Aggregate) == false && (p instanceof Dedup) == false) {
                p.expressions().forEach(x -> x.forEachDown(AggregateFunction.class, af -> {
                    failures.add(fail(af, "aggregate function [{}] not allowed outside STATS command", af.sourceText()));
                }));
            }
        };
    }
}
