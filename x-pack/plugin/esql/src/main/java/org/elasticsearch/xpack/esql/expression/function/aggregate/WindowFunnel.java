/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.fromIndex;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isBoolean;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;

public class WindowFunnel extends AggregateFunction implements OnlySurrogateExpression, ConfigurationFunction {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(WindowFunnel.class)
        .binaryVariadicConfig(WindowFunnel::new)
        .name("window_funnel");

    public static final int MAX_CONDITIONS = 32;

    private final Configuration configuration;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.7.0") },
        returnType = "integer",
        preview = true,
        briefSummary = "Returns how many steps of an ordered sequence of conditions were reached within a time span.",
        description = """
            Searches for an ordered chain of events, where an event is a row that satisfies one of the conditions,
            and returns the number of conditions reached in order, starting with the first one.
            Every event of the chain must happen at most `maxspan` after the event that satisfied the first
            condition. Events don't need to be consecutive, rows that satisfy no condition are ignored,
            and a single row can satisfy several consecutive conditions.
            Returns `0` when no row satisfies the first condition.""",
        detailedDescription = """
            The function scans the events of each group in timestamp order. Every event that satisfies the first
            condition starts a new chain, and an event that satisfies condition `N` extends the chain that reached
            condition `N - 1` when it happens at most `maxspan` after that chain's first event.
            Events that share the same timestamp are processed together, as if they happened in condition order.

            Rows with a `null` timestamp are ignored. A multivalued timestamp makes the row an event at each of
            its values. A `null` condition counts as `false`, and a multivalued condition counts as `false`
            and emits a warning. The function keeps one entry for each distinct timestamp of each group that
            satisfies at least one condition, so its memory usage grows with the number of matching events.""",
        type = FunctionType.AGGREGATE,
        tsdbCompatible = false,
        examples = {
            @Example(file = "stats_window_funnel", tag = "window-funnel"),
            @Example(file = "stats_window_funnel", tag = "window-funnel-by-multiple") }
    )
    public WindowFunnel(
        Source source,
        @Param(
            name = "maxspan",
            type = { "time_duration" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "The maximum time between the event that satisfies the first condition and the last event of the chain."
        ) Expression maxspan,
        @Param(name = "timestamp", type = { "date", "date_nanos" }, description = "The timestamp of the event.") Expression timestamp,
        @Param(
            name = "conditions",
            type = { "boolean" },
            description = "Between one and 32 conditions, in the order the events must happen."
        ) List<Expression> conditions,
        Configuration configuration
    ) {
        this(source, timestamp, conditions, Literal.TRUE, NO_WINDOW, maxspan, configuration);
    }

    public WindowFunnel(
        Source source,
        Expression timestamp,
        List<Expression> conditions,
        Expression filter,
        Expression window,
        Expression maxspan,
        Configuration configuration
    ) {
        super(source, fields(timestamp, conditions), filter, window, List.of(maxspan));
        this.configuration = configuration;
    }

    private static List<Expression> fields(Expression timestamp, List<Expression> conditions) {
        List<Expression> fields = new ArrayList<>(conditions.size() + 1);
        fields.add(timestamp);
        fields.addAll(conditions);
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public Configuration configuration() {
        return configuration;
    }

    public Expression maxspan() {
        return parameters().getFirst();
    }

    public Expression timestamp() {
        return fields().getFirst();
    }

    public List<? extends Expression> conditions() {
        return fields().subList(1, fields().size());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isNotNull(maxspan(), sourceText(), FIRST).and(
            isType(maxspan(), dt -> dt == DataType.TIME_DURATION, sourceText(), FIRST, "time_duration")
        ).and(isType(timestamp(), dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), SECOND, "date", "date_nanos"));
        if (resolution.unresolved()) {
            return resolution;
        }
        List<? extends Expression> conditions = conditions();
        if (conditions.isEmpty()) {
            return new TypeResolution(format(null, "[{}] requires at least one condition", sourceText()));
        }
        if (conditions.size() > MAX_CONDITIONS) {
            return new TypeResolution(
                format(null, "[{}] supports at most [{}] conditions, found [{}]", sourceText(), MAX_CONDITIONS, conditions.size())
            );
        }
        for (int i = 0; i < conditions.size(); i++) {
            resolution = isBoolean(conditions.get(i), sourceText(), fromIndex(i + 2));
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        resolution = isFoldable(maxspan(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return resolveTypeMaxspan(maxspan(), sourceText(), forPreOptimizationValidation(maxspan()));
    }

    static TypeResolution resolveTypeMaxspan(Expression maxspan, String sourceText, TypeResolutionValidator validator) {
        if (maxspan instanceof Literal nullSpan && nullSpan.value() == null) {
            validator.invalid(
                new TypeResolution(format(null, "first argument of [{}] cannot be null, received [{}]", sourceText, maxspan.sourceText()))
            );
        } else if (maxspan instanceof Literal literal && literal.value() instanceof Duration duration) {
            if (duration.isNegative()) {
                validator.invalid(
                    new TypeResolution(
                        format(
                            null,
                            "first argument of [{}] must be a non-negative time_duration, found [{}]",
                            sourceText,
                            maxspan.sourceText()
                        )
                    )
                );
            }
        } else {
            validator.invalidIfPostValidation(
                fail(maxspan, "maxspan of [{}] must be a constant time_duration, found [{}]", sourceText, maxspan)
            );
        }
        return validator.getResolvedType();
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected NodeInfo<WindowFunnel> info() {
        return NodeInfo.create(
            this,
            WindowFunnel::new,
            timestamp(),
            List.<Expression>copyOf(conditions()),
            filter(),
            window(),
            maxspan(),
            configuration
        );
    }

    @Override
    public WindowFunnel replaceChildren(List<Expression> newChildren) {
        int conditionsEnd = newChildren.size() - 3;
        return new WindowFunnel(
            source(),
            newChildren.getFirst(),
            newChildren.subList(1, conditionsEnd),
            newChildren.get(conditionsEnd),
            newChildren.get(conditionsEnd + 1),
            newChildren.get(conditionsEnd + 2),
            configuration
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), configuration);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        return configuration.equals(((WindowFunnel) obj).configuration);
    }

    @Override
    public Expression surrogate() {
        Source s = source();
        List<? extends Expression> conditions = conditions();
        List<Expression> masks = new ArrayList<>(conditions.size());
        for (int i = 0; i < conditions.size(); i++) {
            masks.add(stepMask(s, conditions.get(i), i));
        }
        Expression mask = addAll(s, masks, 0, masks.size());
        WindowFunnelLevel level = new WindowFunnelLevel(
            s,
            timestamp(),
            mask,
            filter(),
            window(),
            maxspan(),
            Literal.integer(s, conditions.size())
        );
        return new Coalesce(s, level, List.of(Literal.integer(s, 0)));
    }

    private Expression addAll(Source s, List<Expression> values, int from, int to) {
        if (to - from == 1) {
            return values.get(from);
        }
        int middle = (from + to) >>> 1;
        return new Add(s, addAll(s, values, from, middle), addAll(s, values, middle, to), configuration);
    }

    private static Expression stepMask(Source s, Expression condition, int step) {
        return new Case(s, condition, List.of(Literal.fromLong(s, 1L << step), Literal.fromLong(s, 0L)));
    }
}
