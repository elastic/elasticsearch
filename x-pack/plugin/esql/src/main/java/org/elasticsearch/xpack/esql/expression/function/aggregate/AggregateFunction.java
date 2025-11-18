/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * A type of {@code Function} that takes multiple values and extracts a single value out of them. For example, {@code AVG()}.
 * - Aggregate functions can have an optional filter and window, which default to {@code Literal.TRUE} and {@code NO_WINDOW}.
 * - The aggregation function should be composed as: source, field, filter, window, parameters.
 * Extra parameters should go to the parameters after the filter and window.
 */
public abstract class AggregateFunction extends Function implements PostAnalysisPlanVerificationAware {
    public static final Literal NO_WINDOW = Literal.timeDuration(Source.EMPTY, Duration.ZERO);
    public static final TransportVersion WINDOW_INTERVAL = TransportVersion.fromName("aggregation_window");

    private final Expression field;
    private final List<? extends Expression> parameters;
    private final Expression filter;
    private final Expression window;

    protected AggregateFunction(Source source, Expression field) {
        this(source, field, Literal.TRUE, NO_WINDOW, emptyList());
    }

    protected AggregateFunction(Source source, Expression field, List<? extends Expression> parameters) {
        this(source, field, Literal.TRUE, NO_WINDOW, parameters);
    }

    protected AggregateFunction(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        List<? extends Expression> parameters
    ) {
        super(source, CollectionUtils.combine(asList(field, filter, window), parameters));
        this.field = field;
        this.filter = filter;
        this.window = Objects.requireNonNull(window, "[window] must be specified; use NO_WINDOW instead");
        this.parameters = parameters;
    }

    protected AggregateFunction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    protected static Expression readWindow(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(WINDOW_INTERVAL)) {
            return in.readNamedWriteable(Expression.class);
        } else {
            return NO_WINDOW;
        }
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(filter);
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window);
        }
        out.writeNamedWriteableCollection(parameters);
    }

    public Expression field() {
        return field;
    }

    public List<? extends Expression> parameters() {
        return parameters;
    }

    public boolean hasFilter() {
        return filter != null
            && (filter.foldable() == false || (filter instanceof Literal literal && Boolean.TRUE.equals(literal.value()) == false));
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

    /**
     * Return the window associated with the aggregate function.
     */
    public Expression window() {
        return window;
    }

    /**
     * Whether the aggregate function has a window different than NO_WINDOW.
     */
    public boolean hasWindow() {
        if (window instanceof Literal lit && lit.value() instanceof Duration duration) {
            return duration.isZero() == false;
        }
        return true;
    }

    /**
     * Returns the set of input attributes required by this aggregate function, excluding those referenced by the filter.
     */
    public AttributeSet aggregateInputReferences(Supplier<List<Attribute>> inputAttributes) {
        if (hasFilter()) {
            return Expressions.references(CollectionUtils.combine(List.of(field), parameters));
        } else {
            return references();
        }
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
                && Objects.equals(other.window(), window())
                && Objects.equals(other.parameters(), parameters());
        }
        return false;
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (p, failures) -> {
            if ((p instanceof Aggregate) == false) {
                p.expressions().forEach(x -> x.forEachDown(AggregateFunction.class, af -> {
                    failures.add(fail(af, "aggregate function [{}] not allowed outside STATS command", af.sourceText()));
                }));
            }
        };
    }
}
