/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.ToMask;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

public final class Case extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Case", Case::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Case.class)
        .unaryVariadic(Case::new)
        // A one value list condition is single valued, so it picks a branch like a plain boolean.
        // A multivalued one warns even when the CASE is only partially folded, and reports the
        // same message the other functions use.
        .capabilities(
            "flattened",
            "single_value_list_condition",
            "partial_fold_multivalue_warning",
            "standard_multivalue_message",
            "multivalue_warning_names_function"
        )
        .name("case");

    private static final String MULTIVALUE_CONDITION_MESSAGE = "single-value function encountered multi-value";

    record Condition(Expression condition, Expression value) {
        /**
         * @param caseSource the source of the enclosing {@code CASE}, which multivalue warnings
         *                   are reported against so that they name the function rather than one
         *                   of its arguments
         */
        ConditionEvaluatorSupplier toEvaluator(ToEvaluator toEvaluator, Source caseSource) {
            return new ConditionEvaluatorSupplier(caseSource, toEvaluator.apply(condition), toEvaluator.apply(value));
        }
    }

    private final List<Condition> conditions;
    private final Expression elseValue;
    private DataType dataType;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = {
            "aggregate_metric_double",
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "date_range",
            "dense_vector",
            "double",
            "double_range",
            "flattened",
            "geo_point",
            "geo_shape",
            "geohash",
            "geotile",
            "geohex",
            "histogram",
            "integer",
            "ip",
            "keyword",
            "long",
            "tdigest",
            "unsigned_long",
            "version",
            "exponential_histogram" },
        // Identity-return overloads omitted: the return type follows the first non-null value
        // branch, so a fixed $N return reference cannot express it.
        briefSummary = "Returns the value for the first condition that evaluates to true.",
        description = """
            Accepts pairs of conditions and values. The function returns the value that
            belongs to the first condition that evaluates to `true`. Both the conditions
            and the returned values can be any expression, including column references.

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
            ),
            @Example(
                description = "Extract error messages and count distinct ones using a column expression:",
                file = "conditional",
                tag = "docsCaseColumnExpression"
            ) }
    )
    public Case(
        Source source,
        @Param(name = "condition", type = { "boolean" }, description = "A condition.") Expression first,
        @Param(
            name = "trueValue",
            type = {
                "aggregate_metric_double",
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "date_range",
                "dense_vector",
                "double",
                "double_range",
                "flattened",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "histogram",
                "integer",
                "ip",
                "keyword",
                "long",
                "tdigest",
                "text",
                "unsigned_long",
                "version",
                "exponential_histogram" },
            description = "The expression or value that’s returned when the corresponding condition is the first to evaluate to `true`. "
                + "Can be a column reference or any other expression. The default value is returned when no condition matches."
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
            boolean originalWasNull = dataType == NULL;
            dataType = value.dataType().noText();
            return TypeResolutions.isType(
                value,
                t -> true,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(position),
                originalWasNull ? NULL.typeName() : "any type"
            );
        }
        return TypeResolutions.isType(
            value,
            t -> t.noText() == dataType,
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
        // Nested CASE values are walked here rather than recursed into, so a deep
        // CASE(true, CASE(true, ...), ...) cannot overflow the stack.
        Deque<Case> nested = null;
        Case current = this;
        while (current != null) {
            Expression takenValue = null;
            for (Condition condition : current.conditions) {
                if (condition.condition.foldable() == false) {
                    return false;
                }
                /* Given the current condition is foldable,
                    if we have already folded the condition into a Literal
                        If True, Case is foldable if the value is foldable
                        If False, Case is foldable if the rest of the conditions are foldable
                    Otherwise
                        if the value is foldable and the rest of the conditions are foldable, Case is foldable
                 */
                if (condition.condition instanceof Literal literal) {
                    if (isTrue(literal.value())) {
                        // The condition is literally TRUE, so only the matching value needs to be foldable.
                        takenValue = condition.value;
                        break;
                    } else {
                        continue;
                    }
                }
                if (condition.value instanceof Case c) {
                    nested = defer(nested, c);
                } else if (condition.value.foldable() == false) {
                    return false;
                }
            }
            Expression last = takenValue == null ? current.elseValue : takenValue;
            if (last instanceof Case c) {
                nested = defer(nested, c);
            } else if (last.foldable() == false) {
                return false;
            }
            current = nested == null || nested.isEmpty() ? null : nested.pop();
        }
        return true;
    }

    private static Deque<Case> defer(Deque<Case> nested, Case c) {
        if (nested == null) {
            nested = new ArrayDeque<>();
        }
        nested.push(c);
        return nested;
    }

    @Override
    public Object fold(FoldContext ctx) {
        // Walk nested CASE along the taken branch so CASE(true, CASE(true, ...), ...)
        // cannot overflow the stack.
        Expression remaining = this;
        while (remaining instanceof Case current) {
            remaining = takenBranch(ctx, current);
        }
        return remaining.fold(ctx);
    }

    /**
     * {@link PlannerUtils#toElementType} rejects these types, so they can't go in a
     * {@link Block} and there is no evaluator to fold them with.
     */
    private static boolean hasNoEvaluator(Case c) {
        DataType type = c.dataType();
        return type == DataType.DATE_PERIOD || type == DataType.TIME_DURATION;
    }

    /**
     * Is this the {@code true} the evaluator sees? {@code CaseLazyEvaluator#eval} reads the value
     * out of the Block, so a one value list is single valued and counts, while more than one is
     * multivalued and is treated as false.
     */
    private static boolean isTrue(Object value) {
        if (value instanceof List<?> values) {
            return values.size() == 1 && Boolean.TRUE.equals(values.getFirst());
        }
        return Boolean.TRUE.equals(value);
    }

    private static Expression takenBranch(FoldContext ctx, Case current) {
        for (Condition condition : current.conditions) {
            Object folded = condition.condition.fold(ctx);
            warnIfMultivaluedCondition(current, folded);
            if (isTrue(folded)) {
                return condition.value;
            }
        }
        return current.elseValue;
    }

    /**
     * The two warnings {@link Warnings#registerException} raises for a multivalued condition.
     * Planning drops such a condition without building an evaluator, in {@link #takenBranch} and
     * in {@link #partiallyFold}, so the warning the evaluator would have raised has to come from
     * here instead. Types with no evaluator have never warned and still don't.
     * <p>
     *     There is no {@link DriverContext} to collect these, so they go straight to the response
     *     headers, like {@code SpatialGridFunction#foldWarningConsumer}. Keep the text in step
     *     with {@link Warnings}, including the 20 from its {@code MAX_ADDED_WARNINGS}, or a
     *     planned CASE warns differently from an evaluated one.
     * </p>
     */
    private static void warnIfMultivaluedCondition(Case c, Object folded) {
        if (folded instanceof List<?> values && values.size() > 1 && hasNoEvaluator(c) == false) {
            Source source = c.source();
            String location = source.viewName() == null
                ? format("Line {}:{}: ", source.lineNumber(), source.columnNumber())
                : format("Line {}:{} (in view [{}]): ", source.lineNumber(), source.columnNumber(), source.viewName());
            HeaderWarning.addWarning(
                "{}evaluation of [{}] failed, treating result as false. Only first {} failures recorded.",
                location,
                source.text(),
                20
            );
            HeaderWarning.addWarning(location + IllegalArgumentException.class.getName() + ": " + MULTIVALUE_CONDITION_MESSAGE);
        }
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
    public Expression partiallyFold(FoldContext ctx) {
        // TODO don’t throw away the results of any `fold`. That might mean looking for literal TRUE on the conditions.
        List<Expression> newChildren = new ArrayList<>(children().size());
        boolean modified = false;
        for (Condition condition : conditions) {
            if (condition.condition.foldable() == false) {
                newChildren.add(condition.condition);
                newChildren.add(condition.value);
                continue;
            }
            modified = true;
            Object folded = condition.condition.fold(ctx);
            warnIfMultivaluedCondition(this, folded);
            if (isTrue(folded)) {
                /*
                 * `fold` can make four things here:
                 * 1. `TRUE`, or a one element list holding it, which is single valued
                 * 2. `FALSE`
                 * 3. null
                 * 4. A list with more than one `TRUE` or `FALSE` in it.
                 *
                 * In the first case, we fold to the value of the condition.
                 * The multivalued field will make a warning, but eventually
                 * become null. And null will become false. So cases 2-4 are
                 * the same. In those cases we fold the entire condition
                 * away, returning just what ever’s remaining in the CASE.
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
        Expression result = innerFinishPartialFold(newChildren);
        if (result.dataType().noText().equals(dataType()) == false) {
            throw new IllegalStateException("partiallyFold produced type [" + result.dataType() + "] but expected [" + dataType() + "]");
        }
        return result;
    }

    private Expression innerFinishPartialFold(List<Expression> newChildren) {
        return switch (newChildren.size()) {
            // CASE(false, a) -> NULL
            case 0 -> new Literal(source(), null, dataType());
            // CASE(false, a, b) -> b, casting a NULL arm to dataType() so callers see KEYWORD, not NULL.
            case 1 -> {
                Expression child = newChildren.getFirst();
                if (child.dataType() == NULL && dataType() != NULL) {
                    yield new Literal(child.source(), null, dataType());
                }
                yield child;
            }
            // CASE(false, a, b, c, d) -> CASE(b, c, d)
            default -> replaceChildren(newChildren);
        };
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<ConditionEvaluatorSupplier> conditionsFactories = conditions.stream().map(c -> c.toEvaluator(toEvaluator, source())).toList();
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
                 * It’s just that we then *immediately* convert it to
                 * true or false using the tri-valued boolean logic stuff.
                 * And that makes it into false. This is, *exactly* what
                 * happens in PostgreSQL and MySQL and SQLite:
                 * > SELECT CASE WHEN null THEN 1 ELSE 2 END;
                 * 2
                 * Rather than go into depth about this in the warning message,
                 * we just say "false".
                 */
                driverContext.createWarningsTreatedAsFalse(conditionSource),
                condition.get(driverContext),
                condition.eagerEvalSafeInLazy(),
                value.get(driverContext),
                value.eagerEvalSafeInLazy()
            );
        }

        @Override
        public String toString() {
            return "ConditionEvaluator[condition=" + condition + ", value=" + value + ']';
        }
    }

    /**
     * A single {@code condition, value} arm of a {@code CASE}.
     * <p>
     *     {@code conditionEagerEvalSafe} and {@code valueEagerEvalSafe} carry
     *     {@link ExpressionEvaluator.Factory#eagerEvalSafeInLazy()} over to evaluation time. When
     *     {@code true} the {@link CaseLazyEvaluator} runs the child over the whole {@link Page} and
     *     just reads the positions it needs, instead of first filtering the page down to those positions.
     * </p>
     */
    record ConditionEvaluator(
        Warnings conditionWarnings,
        ExpressionEvaluator condition,
        boolean conditionEagerEvalSafe,
        ExpressionEvaluator value,
        boolean valueEagerEvalSafe
    ) implements Releasable {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConditionEvaluator.class);

        @Override
        public void close() {
            Releasables.closeExpectNoException(condition, value);
        }

        @Override
        public String toString() {
            return "ConditionEvaluator[condition=" + condition + ", value=" + value + ']';
        }

        public void registerMultivalue() {
            conditionWarnings.registerException(IllegalArgumentException.class, MULTIVALUE_CONDITION_MESSAGE);
        }

        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + condition.baseRamBytesUsed() + value.baseRamBytesUsed();
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
                ExpressionEvaluator result = new CaseLazyEvaluator(
                    context.blockFactory(),
                    resultType,
                    conditions,
                    elseValue,
                    elseValueFactory.eagerEvalSafeInLazy()
                );
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

    /**
     * Evaluates {@code CASE} lazily, one <strong>arm</strong> at a time rather than one position at a time.
     * <p>
     *     An arm is one {@code condition, value} pair of the {@code CASE}, the branch that is taken when
     *     that condition is the first one to be {@code true}. The trailing else value is the final arm,
     *     taken when no condition matched. So {@code CASE(a, x, b, y, z)} has three arms: {@code a -> x},
     *     {@code b -> y} and the else arm {@code z}.
     * </p>
     * <p>
     *     Laziness is required for correctness: a condition may only be evaluated for the positions
     *     where all previous conditions were not {@code true}, and a value may only be evaluated for
     *     the positions where its condition is the first {@code true} one. Otherwise we’d emit
     *     warnings (or do expensive work) for positions whose result never uses that arm.
     * </p>
     * <p>
     *     We keep an ascending array of the positions that are still unresolved. For each arm we
     *     evaluate the condition either on the whole page, when every position is still unresolved
     *     or when the condition is {@link ExpressionEvaluator.Factory#eagerEvalSafeInLazy() safe to
     *     evaluate eagerly}, or on the page {@link Page#filter filtered} down to the unresolved
     *     positions. The matching positions are then removed from the unresolved set and the arm’s
     *     value is evaluated the same way, either on the whole page or on the page filtered to just
     *     those positions. Finally the per-arm result blocks are scattered back into a single block
     *     in page order. Because filtering preserves order, the index of a position inside a filtered
     *     arm block is just the number of earlier positions that landed in the same arm.
     * </p>
     * <p>
     *     This costs at most one {@link Page#filter} per arm rather than one per position, and no
     *     filtering at all for eager-safe children like literals and field loads.
     * </p>
     */
    private static final class CaseLazyEvaluator implements ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CaseLazyEvaluator.class);

        private final BlockFactory blockFactory;
        private final ElementType resultType;
        private final List<ConditionEvaluator> conditions;
        private final ExpressionEvaluator elseVal;
        private final boolean elseEagerEvalSafe;

        /*
         * Per-position scratch space, allocated lazily and grown to the largest page seen so
         * we don't allocate four fresh arrays for every page. An evaluator is only ever used
         * by a single driver at a time and eval is not re-entrant, so reusing these is safe.
         * Their contents are never read before being written within an eval call.
         */
        private int[] armOf = new int[0];
        private int[] remaining = new int[0];
        private int[] matched = new int[0];
        private int[] nextRemaining = new int[0];

        CaseLazyEvaluator(
            BlockFactory blockFactory,
            ElementType resultType,
            List<ConditionEvaluator> conditions,
            ExpressionEvaluator elseVal,
            boolean elseEagerEvalSafe
        ) {
            this.blockFactory = blockFactory;
            this.resultType = resultType;
            this.conditions = conditions;
            this.elseVal = elseVal;
            this.elseEagerEvalSafe = elseEagerEvalSafe;
        }

        @Override
        public Block eval(Page page) {
            int positionCount = page.getPositionCount();
            int armCount = conditions.size() + 1;
            /*
             * arms[i] holds the block produced by the value of condition i, arms[armCount - 1]
             * the block produced by the else value. Arms that no position selected stay null.
             * armEvaluatedOnFullPage[i] is true when arms[i] was evaluated on the whole page,
             * so position p lives at index p in the block. Otherwise the block only contains
             * the positions that selected the arm, in ascending order.
             */
            Block[] arms = new Block[armCount];
            boolean[] armEvaluatedOnFullPage = new boolean[armCount];
            if (armOf.length < positionCount) {
                armOf = new int[positionCount];
                remaining = new int[positionCount];
                matched = new int[positionCount];
                nextRemaining = new int[positionCount];
            }
            int[] armOf = this.armOf;
            int[] remaining = this.remaining;
            int[] matched = this.matched;
            int[] nextRemaining = this.nextRemaining;
            for (int p = 0; p < positionCount; p++) {
                remaining[p] = p;
            }
            int remainingCount = positionCount;
            try {
                for (int arm = 0; arm < conditions.size() && remainingCount > 0; arm++) {
                    ConditionEvaluator condition = conditions.get(arm);
                    boolean fullPage = remainingCount == positionCount || condition.conditionEagerEvalSafe();
                    // TODO filter only the channels the child reads; filtering every block copies columns the arm never looks at
                    Page conditionPage = fullPage ? page : page.filter(false, remaining, 0, remainingCount);
                    int matchedCount = 0;
                    int nextRemainingCount = 0;
                    boolean sawMultivalue = false;
                    try (BooleanBlock b = (BooleanBlock) condition.condition.eval(conditionPage)) {
                        BooleanVector v = b.asVector();
                        if (v != null) {
                            // Fast path: no nulls or multivalues to check for.
                            for (int j = 0; j < remainingCount; j++) {
                                int p = remaining[j];
                                if (v.getBoolean(fullPage ? p : j)) {
                                    matched[matchedCount++] = p;
                                } else {
                                    nextRemaining[nextRemainingCount++] = p;
                                }
                            }
                        } else {
                            for (int j = 0; j < remainingCount; j++) {
                                int p = remaining[j];
                                int idx = fullPage ? p : j;
                                boolean selected;
                                if (b.isNull(idx)) {
                                    selected = false;
                                } else if (b.getValueCount(idx) > 1) {
                                    sawMultivalue = true;
                                    selected = false;
                                } else {
                                    selected = b.getBoolean(b.getFirstValueIndex(idx));
                                }
                                if (selected) {
                                    matched[matchedCount++] = p;
                                } else {
                                    nextRemaining[nextRemainingCount++] = p;
                                }
                            }
                        }
                    } finally {
                        if (conditionPage != page) {
                            conditionPage.releaseBlocks();
                        }
                    }
                    if (sawMultivalue) {
                        condition.registerMultivalue();
                    }
                    if (matchedCount > 0) {
                        boolean valueFullPage = matchedCount == positionCount || condition.valueEagerEvalSafe();
                        arms[arm] = evalArm(page, condition.value, valueFullPage, matched, matchedCount);
                        armEvaluatedOnFullPage[arm] = valueFullPage;
                        for (int j = 0; j < matchedCount; j++) {
                            armOf[matched[j]] = arm;
                        }
                    }
                    int[] tmp = remaining;
                    remaining = nextRemaining;
                    nextRemaining = tmp;
                    remainingCount = nextRemainingCount;
                }
                if (remainingCount > 0) {
                    int arm = armCount - 1;
                    boolean elseFullPage = remainingCount == positionCount || elseEagerEvalSafe;
                    arms[arm] = evalArm(page, elseVal, elseFullPage, remaining, remainingCount);
                    armEvaluatedOnFullPage[arm] = elseFullPage;
                    for (int j = 0; j < remainingCount; j++) {
                        armOf[remaining[j]] = arm;
                    }
                }

                /*
                 * If a single arm was selected then it must have been selected by every position
                 * and, because nothing was resolved before it, evaluated on the whole page. So
                 * we can hand its block back directly without copying.
                 */
                int onlyArm = -1;
                int selectedArms = 0;
                for (int arm = 0; arm < armCount; arm++) {
                    if (arms[arm] != null) {
                        onlyArm = arm;
                        selectedArms++;
                    }
                }
                if (selectedArms == 1 && armEvaluatedOnFullPage[onlyArm]) {
                    Block result = arms[onlyArm];
                    arms[onlyArm] = null;
                    return result;
                }

                /*
                 * Scatter the arm blocks back into page order, copying each run of consecutive
                 * positions that picked the same arm with a single copyFrom. cursor[arm] tracks how
                 * far we've read into an arm block that was evaluated on a filtered page.
                 */
                int[] cursor = new int[armCount];
                try (Block.Builder result = resultType.newBlockBuilder(positionCount, blockFactory)) {
                    int p = 0;
                    while (p < positionCount) {
                        int arm = armOf[p];
                        int end = p + 1;
                        while (end < positionCount && armOf[end] == arm) {
                            end++;
                        }
                        int length = end - p;
                        if (armEvaluatedOnFullPage[arm]) {
                            result.copyFrom(arms[arm], p, end);
                        } else {
                            result.copyFrom(arms[arm], cursor[arm], cursor[arm] + length);
                            cursor[arm] += length;
                        }
                        p = end;
                    }
                    return result.build();
                }
            } finally {
                Releasables.closeExpectNoException(arms);
            }
        }

        /**
         * Evaluate an arm’s value for the {@code selectedCount} positions in {@code selected}. When
         * {@code fullPage} is set the value is evaluated on the whole page, which the caller does for
         * eager-safe values and for values that every position selected. Otherwise it is evaluated on
         * the page filtered down to just the selected positions so that the value never sees, and never
         * warns about, positions that another arm resolved.
         */
        private static Block evalArm(Page page, ExpressionEvaluator value, boolean fullPage, int[] selected, int selectedCount) {
            Block result;
            if (fullPage) {
                result = value.eval(page);
            } else {
                // TODO filter only the channels the child reads; filtering every block copies columns the arm never looks at
                Page valuePage = page.filter(false, selected, 0, selectedCount);
                try {
                    result = value.eval(valuePage);
                } finally {
                    valuePage.releaseBlocks();
                }
            }
            assert result.getPositionCount() == (fullPage ? page.getPositionCount() : selectedCount)
                : "arm produced ["
                    + result.getPositionCount()
                    + "] positions, expected ["
                    + (fullPage ? page.getPositionCount() : selectedCount)
                    + "]";
            return result;
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            for (ConditionEvaluator condition : conditions) {
                baseRamBytesUsed += condition.baseRamBytesUsed();
            }
            baseRamBytesUsed += elseVal.baseRamBytesUsed();
            return baseRamBytesUsed;
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
        ExpressionEvaluator elseVal
    ) implements ExpressionEvaluator {

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CaseEagerEvaluator.class);

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
                            // TODO Copy the per-type specialization that COALESCE has.
                            // There’s also a slowdown because copying from a block checks to see if there are any nulls and that’s slow.
                            // Vectors do not, so this still shows as fairly fast. But not as fast as the per-type unrolling.
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
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + condition.baseRamBytesUsed() + elseVal.baseRamBytesUsed();
        }

        @Override
        public String toString() {
            return "CaseEagerEvaluator[conditions=[" + condition + "], elseVal=" + elseVal + ']';
        }
    }
}
