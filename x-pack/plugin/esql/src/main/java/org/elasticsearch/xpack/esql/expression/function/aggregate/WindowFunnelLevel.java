/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.WindowFunnelAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class WindowFunnelLevel extends AggregateFunction implements ToAggregator, PostOptimizationVerificationAware, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WindowFunnelLevel",
        WindowFunnelLevel::readFrom
    );

    public WindowFunnelLevel(
        Source source,
        Expression timestamp,
        Expression mask,
        Expression filter,
        Expression window,
        Expression maxspan,
        Expression steps
    ) {
        super(source, List.of(timestamp, mask), filter, window, List.of(maxspan, steps));
    }

    private static WindowFunnelLevel readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        List<Expression> fields = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new WindowFunnelLevel(source, fields.get(0), fields.get(1), filter, window, parameters.get(0), parameters.get(1));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression timestamp() {
        return fields().getFirst();
    }

    public Expression mask() {
        return fields().get(1);
    }

    public Expression maxspan() {
        return parameters().getFirst();
    }

    public Expression steps() {
        return parameters().get(1);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(timestamp(), dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), FIRST, "date", "date_nanos")
            .and(isType(mask(), dt -> dt == DataType.LONG, sourceText(), SECOND, "long"));
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        if (maxspan() instanceof Literal literal && literal.value() instanceof Duration) {
            return;
        }
        failures.add(fail(maxspan(), "maxspan of [{}] must be a constant time_duration, found [{}]", sourceText(), maxspan()));
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    protected NodeInfo<WindowFunnelLevel> info() {
        return NodeInfo.create(this, WindowFunnelLevel::new, timestamp(), mask(), filter(), window(), maxspan(), steps());
    }

    @Override
    public WindowFunnelLevel replaceChildren(List<Expression> newChildren) {
        return new WindowFunnelLevel(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4),
            newChildren.get(5)
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        Duration span = (Duration) ((Literal) maxspan()).value();
        int steps = (Integer) ((Literal) steps()).value();
        return new WindowFunnelAggregatorFunctionSupplier(spanInTimestampUnits(span), steps);
    }

    private long spanInTimestampUnits(Duration span) {
        try {
            return timestamp().dataType() == DataType.DATE_NANOS ? span.toNanos() : span.toMillis();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
