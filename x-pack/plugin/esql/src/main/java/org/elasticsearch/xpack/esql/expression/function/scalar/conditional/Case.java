/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.ToMask;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

public final class Case extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Case", Case::new);

    record Condition(Expression condition, Expression value) {
        ConditionEvaluatorSupplier toEvaluator(ToEvaluator toEvaluator) {
            return new ConditionEvaluatorSupplier(condition.source(), toEvaluator.apply(condition), toEvaluator.apply(value));
        }
    }

    private final List<Condition> conditions;
    private final Expression elseValue;
    private DataType dataType;

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "text",
            "unsigned_long",
            "version" },
        description = """
            Accepts pairs of conditions and values. The function returns the value that
            belongs to the first condition that evaluates to `true`.

            If the number of arguments is odd, the last argument is the default value which
            is returned when no condition matches. If the number of arguments is even, and
            no condition matches, the function returns `null`.""",
        examples = {
            @Example(description = "Determine whether employees are monolingual, bilingual, or polyglot:", file = "docs", tag = "case"),
            @Example(
                description = "Calculate the total connection success rate based on log messages:",
                file = "conditional",
                tag = "docsCaseSuccessRate"
            ),
            @Example(
                description = "Calculate an hourly error rate as a percentage of the total number of log messages:",
                file = "conditional",
                tag = "docsCaseHourlyErrorRate"
            ) }
    )
    public Case(
        Source source,
        @Param(name = "condition", type = { "boolean" }, description = "A condition.") Expression first,
        @Param(
            name = "trueValue",
            type = {
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "The value that's returned when the corresponding condition is the first to evaluate to `true`. "
                + "The default value is returned when no condition matches."
        ) List<Expression> rest
    ) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
        int conditionCount = children().size() / 2;
        conditions = new ArrayList<>(conditionCount);
        for (int c = 0; c < conditionCount; c++) {
            conditions.add(new Condition(children().get(c * 2), children().get(c * 2 + 1)));
        }
        elseValue = elseValueIsExplicit() ? children().get(children().size() - 1) : new Literal(source, null, NULL);
    }

    private Case(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteableCollection(children().subList(1, children().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private boolean elseValueIsExplicit() {
        return children().size() % 2 == 1;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (children().size() < 2) {
            return new TypeResolution(format(null, "expected at least two arguments in [{}] but got {}", sourceText(), children().size()));
        }

        for (int c = 0; c < conditions.size(); c++) {
            Condition condition = conditions.get(c);

            TypeResolution resolution = TypeResolutions.isBoolean(
                condition.condition,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(c * 2)
            );
            if (resolution.unresolved()) {
                return resolution;
            }

            resolution = resolveValueType(condition.value, c * 2 + 1);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolveValueType(elseValue, conditions.size() * 2);
    }

    private TypeResolution resolveValueType(Expression value, int position) {
        if (dataType == null || dataType == NULL) {
            dataType = value.dataType();
            return TypeResolution.TYPE_RESOLVED;
        }
        return TypeResolutions.isType(
            value,
            t -> t == dataType,
            sourceText(),
            TypeResolutions.ParamOrdinal.fromIndex(position),
            dataType.typeName()
        );
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Case(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Case::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public boolean foldable() {
        for (Condition condition : conditions) {
            if (condition.condition.foldable() == false) {
                return false;
            }
            if (Boolean.TRUE.equals(condition.condition.fold())) {
                /*
                 * `fold` can make four things here:
                 * 1. `TRUE`
                 * 2. `FALSE`
                 * 3. null
                 * 4. A list with more than one `TRUE` or `FALSE` in it.
                 *
                 * In the first case, we're foldable if the condition is foldable.
                 * The multivalued field will make a warning, but eventually
                 * become null. And null will become false. So cases 2-4 are
                 * the same. In those cases we are foldable only if the *rest*
                 * of the condition is foldable.
                 */
                return condition.value.foldable();
            }
        }
        return elseValue.foldable();
    }

    /**
     * Fold the arms of {@code CASE} statements.
     * <ol>
     *     <li>
     *         Conditions that evaluate to {@code false} are removed so
     *         {@code EVAL c=CASE(false, foo, b, bar, bort)} becomes
     *         {@code EVAL c=CASE(b, bar, bort)}.
     *     </li>
     *     <li>
     *         Conditions that evaluate to {@code true} stop evaluation and
     *         return themselves so {@code EVAL c=CASE(true, foo, bar)} becomes
     *         {@code EVAL c=foo}.
     *     </li>
     * </ol>
     * And those two combine so {@code EVAL c=CASE(false, foo, b, bar, true, bort, el)} becomes
     * {@code EVAL c=CASE(b, bar, bort)}.
     */
    public Expression partiallyFold() {
        List<Expression> newChildren = new ArrayList<>(children().size());
        boolean modified = false;
        for (Condition condition : conditions) {
            if (condition.condition.foldable() == false) {
                newChildren.add(condition.condition);
                newChildren.add(condition.value);
                continue;
            }
            modified = true;
            if (Boolean.TRUE.equals(condition.condition.fold())) {
                /*
                 * `fold` can make four things here:
                 * 1. `TRUE`
                 * 2. `FALSE`
                 * 3. null
                 * 4. A list with more than one `TRUE` or `FALSE` in it.
                 *
                 * In the first case, we fold to the value of the condition.
                 * The multivalued field will make a warning, but eventually
                 * become null. And null will become false. So cases 2-4 are
                 * the same. In those cases we fold the entire condition
                 * away, returning just what ever's remaining in the CASE.
                 */
                newChildren.add(condition.value);
                return finishPartialFold(newChildren);
            }
        }
        if (modified == false) {
            return this;
        }
        if (elseValueIsExplicit()) {
            newChildren.add(elseValue);
        }
        return finishPartialFold(newChildren);
    }

    private Expression finishPartialFold(List<Expression> newChildren) {
        return switch (newChildren.size()) {
            case 0 -> new Literal(source(), null, dataType());
            case 1 -> newChildren.get(0);
            default -> replaceChildren(newChildren);
        };
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<ConditionEvaluatorSupplier> conditionsFactories = conditions.stream().map(c -> c.toEvaluator(toEvaluator)).toList();
        ExpressionEvaluator.Factory elseValueFactory = toEvaluator.apply(elseValue);
        ElementType resultType = PlannerUtils.toElementType(dataType());

        if (conditionsFactories.size() == 1
            && conditionsFactories.get(0).value.eagerEvalSafeInLazy()
            && elseValueFactory.eagerEvalSafeInLazy()) {
            return new CaseEagerEvaluatorFactory(resultType, conditionsFactories.get(0), elseValueFactory);
        }
        return new CaseLazyEvaluatorFactory(resultType, conditionsFactories, elseValueFactory);
    }

    record ConditionEvaluatorSupplier(Source conditionSource, ExpressionEvaluator.Factory condition, ExpressionEvaluator.Factory value)
        implements
            Function<DriverContext, ConditionEvaluator> {
        @Override
        public ConditionEvaluator apply(DriverContext driverContext) {
            return new ConditionEvaluator(
                /*
                 * We treat failures as null just like any other failure.
                 * It's just that we then *immediately* convert it to
                 * true or false using the tri-valued boolean logic stuff.
                 * And that makes it into false. This is, *exactly* what
                 * happens in PostgreSQL and MySQL and SQLite:
                 * > SELECT CASE WHEN null THEN 1 ELSE 2 END;
                 * 2
                 * Rather than go into depth about this in the warning message,
                 * we just say "false".
                 */
                Warnings.createWarningsTreatedAsFalse(
                    driverContext.warningsMode(),
                    conditionSource.source().getLineNumber(),
                    conditionSource.source().getColumnNumber(),
                    conditionSource.text()
                ),
                condition.get(driverContext),
                value.get(driverContext)
            );
        }

        @Override
        public String toString() {
            return "ConditionEvaluator[condition=" + condition + ", value=" + value + ']';
        }
    }

    record ConditionEvaluator(
        Warnings conditionWarnings,
        EvalOperator.ExpressionEvaluator condition,
        EvalOperator.ExpressionEvaluator value
    ) implements Releasable {
        @Override
        public void close() {
            Releasables.closeExpectNoException(condition, value);
        }

        @Override
        public String toString() {
            return "ConditionEvaluator[condition=" + condition + ", value=" + value + ']';
        }

        public void registerMultivalue() {
            conditionWarnings.registerException(new IllegalArgumentException("CASE expects a single-valued boolean"));
        }
    }

    private record CaseLazyEvaluatorFactory(
        ElementType resultType,
        List<ConditionEvaluatorSupplier> conditionsFactories,
        ExpressionEvaluator.Factory elseValueFactory
    ) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            List<ConditionEvaluator> conditions = new ArrayList<>(conditionsFactories.size());
            ExpressionEvaluator elseValue = null;
            try {
                for (ConditionEvaluatorSupplier cond : conditionsFactories) {
                    conditions.add(cond.apply(context));
                }
                elseValue = elseValueFactory.get(context);
                ExpressionEvaluator result = new CaseLazyEvaluator(context.blockFactory(), resultType, conditions, elseValue);
                conditions = null;
                elseValue = null;
                return result;
            } finally {
                Releasables.close(conditions == null ? () -> {} : Releasables.wrap(conditions), elseValue);
            }
        }

        @Override
        public String toString() {
            return "CaseLazyEvaluator[conditions=" + conditionsFactories + ", elseVal=" + elseValueFactory + ']';
        }
    }

    private record CaseLazyEvaluator(
        BlockFactory blockFactory,
        ElementType resultType,
        List<ConditionEvaluator> conditions,
        EvalOperator.ExpressionEvaluator elseVal
    ) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            /*
             * We have to evaluate lazily so any errors or warnings that would be
             * produced by the right hand side are avoided. And so if anything
             * on the right hand side is slow we skip it.
             *
             * And it'd be good if that lazy evaluation were fast. But this
             * implementation isn't. It's fairly simple - running position at
             * a time - but it's not at all fast.
             */
            int positionCount = page.getPositionCount();
            try (Block.Builder result = resultType.newBlockBuilder(positionCount, blockFactory)) {
                position: for (int p = 0; p < positionCount; p++) {
                    int[] positions = new int[] { p };
                    Page limited = new Page(
                        1,
                        IntStream.range(0, page.getBlockCount()).mapToObj(b -> page.getBlock(b).filter(positions)).toArray(Block[]::new)
                    );
                    try (Releasable ignored = limited::releaseBlocks) {
                        for (ConditionEvaluator condition : conditions) {
                            try (BooleanBlock b = (BooleanBlock) condition.condition.eval(limited)) {
                                if (b.isNull(0)) {
                                    continue;
                                }
                                if (b.getValueCount(0) > 1) {
                                    condition.registerMultivalue();
                                    continue;
                                }
                                if (false == b.getBoolean(b.getFirstValueIndex(0))) {
                                    continue;
                                }
                                try (Block values = condition.value.eval(limited)) {
                                    result.copyFrom(values, 0, 1);
                                    continue position;
                                }
                            }
                        }
                        try (Block values = elseVal.eval(limited)) {
                            result.copyFrom(values, 0, 1);
                        }
                    }
                }
                return result.build();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(() -> Releasables.close(conditions), elseVal);
        }

        @Override
        public String toString() {
            return "CaseLazyEvaluator[conditions=" + conditions + ", elseVal=" + elseVal + ']';
        }
    }

    private record CaseEagerEvaluatorFactory(
        ElementType resultType,
        ConditionEvaluatorSupplier conditionFactory,
        ExpressionEvaluator.Factory elseValueFactory
    ) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            ConditionEvaluator conditionEvaluator = conditionFactory.apply(context);
            ExpressionEvaluator elseValue = null;
            try {
                elseValue = elseValueFactory.get(context);
                ExpressionEvaluator result = new CaseEagerEvaluator(resultType, context.blockFactory(), conditionEvaluator, elseValue);
                conditionEvaluator = null;
                elseValue = null;
                return result;
            } finally {
                Releasables.close(conditionEvaluator, elseValue);
            }
        }

        @Override
        public String toString() {
            return "CaseEagerEvaluator[conditions=[" + conditionFactory + "], elseVal=" + elseValueFactory + ']';
        }
    }

    private record CaseEagerEvaluator(
        ElementType resultType,
        BlockFactory blockFactory,
        ConditionEvaluator condition,
        EvalOperator.ExpressionEvaluator elseVal
    ) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            try (BooleanBlock lhsOrRhsBlock = (BooleanBlock) condition.condition.eval(page); ToMask lhsOrRhs = lhsOrRhsBlock.toMask()) {
                if (lhsOrRhs.hadMultivaluedFields()) {
                    condition.registerMultivalue();
                }
                if (lhsOrRhs.mask().isConstant()) {
                    if (lhsOrRhs.mask().getBoolean(0)) {
                        return condition.value.eval(page);
                    } else {
                        return elseVal.eval(page);
                    }
                }
                try (
                    Block lhs = condition.value.eval(page);
                    Block rhs = elseVal.eval(page);
                    Block.Builder builder = resultType.newBlockBuilder(lhs.getTotalValueCount(), blockFactory)
                ) {
                    for (int p = 0; p < lhs.getPositionCount(); p++) {
                        if (lhsOrRhs.mask().getBoolean(p)) {
                            builder.copyFrom(lhs, p, p + 1);
                        } else {
                            builder.copyFrom(rhs, p, p + 1);
                        }
                    }
                    return builder.build();
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(condition, elseVal);
        }

        @Override
        public String toString() {
            return "CaseEagerEvaluator[conditions=[" + condition + "], elseVal=" + elseVal + ']';
        }
    }
}
