/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * A type of {@code Function} that takes multiple values and extracts a single value out of them. For example, {@code AVG()}.
 * - Aggregate functions can have an optional filter and window, which default to {@code Literal.TRUE} and {@code NO_WINDOW}.
 * - The aggregation function should be composed as: source, field, filter, window, parameters.
 * Extra parameters should go to the parameters after the filter and window.
 * <p>
 *     These function appear only in special places in the language that expect to take many inputs
 *     and produce one output per group key:
 * </p>
 * <ul>
 *     <li>{@code | STATS MAX(a)}</li>
 *     <li>{@code | STATS MAX(a) BY ...}</li>
 * </ul>
 * <p>
 *     They always process many input rows to produce their values. If they are built
 *     without a {@code BY} they produce a single value as output. If they are built
 *     with a {@code BY} they produce one value per group key as output.
 * </p>
 * <p>
 *     See {@link org.elasticsearch.compute.aggregation.AggregatorMode} for important
 *     information about their execution lifecycle.
 * </p>
 */
public abstract class AggregateFunction extends Function implements PostAnalysisPlanVerificationAware {
    public static final Literal NO_WINDOW = Literal.timeDuration(Source.EMPTY, Duration.ZERO);
    public static final TransportVersion WINDOW_INTERVAL = TransportVersion.fromName("aggregation_window");

    private final List<? extends Expression> fields;
    private final List<? extends Expression> parameters;
    private final Expression filter;
    private final Expression window;

    protected AggregateFunction(Source source, List<? extends Expression> fields) {
        this(source, fields, Literal.TRUE, NO_WINDOW, emptyList());
    }

    protected AggregateFunction(Source source, List<? extends Expression> fields, List<? extends Expression> parameters) {
        this(source, fields, Literal.TRUE, NO_WINDOW, parameters);
    }

    /**
     * @param fields     the per-row input fields processed by the aggregate function
     *                   (e.g. WEIGHTED_AVG's value and weight)
     * @param parameters the configuration constants of this aggregate, folded into the supplier
     *                   (e.g. TOP's limit and order)
     */
    protected AggregateFunction(
        Source source,
        List<? extends Expression> fields,
        Expression filter,
        Expression window,
        List<? extends Expression> parameters
    ) {
        super(source, buildChildren(fields, filter, window, parameters));
        this.fields = fields;
        this.filter = filter;
        this.window = Objects.requireNonNull(window, "[window] must be specified; use NO_WINDOW instead");
        this.parameters = parameters;
    }

    /**
     * The order of the children: fields, filter, window, parameters. Note this differs from the wire layout (see {@link #writeTo}),
     * which for backwards compatibility leads with a single field, followed by the parameters and the remaining fields.
     */
    private static List<Expression> buildChildren(
        List<? extends Expression> fields,
        Expression filter,
        Expression window,
        List<? extends Expression> parameters
    ) {
        return CollectionUtils.combine(CollectionUtils.combine(fields, asList(filter, window)), parameters);
    }

    protected static Expression readWindow(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(WINDOW_INTERVAL)) {
            return in.readNamedWriteable(Expression.class);
        } else {
            return NO_WINDOW;
        }
    }

    /**
     * The per-row fields processed by the aggregate function (e.g. {@code WEIGHTED_AVG}'s field and weight).
     * Configuration constants are not here; see {@link #parameters()}.
     */
    public List<? extends Expression> fields() {
        return fields;
    }

    /**
     * The configuration constants of this aggregate (e.g. {@code TOP}'s limit and order), folded into the
     * {@link org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier}.
     * Per-row input fields are not here; see {@link #fields()}.
     */
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

    /**
     * Attach a filter to the aggregate function.
     */
    public abstract AggregateFunction withFilter(Expression filter);

    public static Expression withFilter(Expression expression, Expression filter) {
        return expression.transformDown(AggregateFunction.class, af -> af.withFilter(filter));
    }

    public static List<? extends Expression> withFilter(List<? extends Expression> expression, Expression filter) {
        if (filter == null) {
            return expression;
        }
        return expression.stream().map(e -> withFilter(e, filter)).toList();
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
     * Returns the ordered list of input attributes required by this aggregate function, excluding those referenced by the filter.
     * The order must align with the input channels expected by the aggregator.
     */
    public List<Attribute> aggregateInputReferences(Supplier<List<Attribute>> inputAttributes) {
        List<Attribute> attributes = new ArrayList<>(fields.size());
        for (Expression field : fields) {
            attributes.addAll(field.references());
        }
        return attributes;
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
            return Objects.equals(other.fields(), fields())
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

    public AggregateFunction withFields(List<? extends Expression> newFields) {
        if (newFields == this.fields) {
            return this;
        }
        return (AggregateFunction) replaceChildren(buildChildren(newFields, filter, window, parameters));
    }

    public AggregateFunction withWindow(Expression newWindow) {
        if (newWindow == this.window) {
            return this;
        }
        return (AggregateFunction) replaceChildren(buildChildren(fields, filter, newWindow, parameters));
    }
}
