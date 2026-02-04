/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStatsFilteredOrNullAggWithEval;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.junit.AssumptionViolatedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Base class for aggregation tests.
 */
public abstract class AbstractAggregationTestCase extends AbstractFunctionTestCase {
    /**
     * Converts a list of aggregation test cases into a list of parameter suppliers.
     * Also, adds a default set of extra test cases.
     * <p>
     *     Use if possible, as this method may get updated with new checks in the future.
     * </p>
     */
    protected static Iterable<Object[]> parameterSuppliersFromTypedDataWithDefaultChecks(
        List<TestCaseSupplier> suppliers,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(withNoRowsExpectingNull(randomizeBytesRefsOffset(suppliers)), positionalErrorMessageSupplier)
        );
    }

    protected static Iterable<Object[]> parameterSuppliersFromTypedDataWithDefaultChecks(List<TestCaseSupplier> suppliers) {
        return parameterSuppliersFromTypedData(withNoRowsExpectingNull(randomizeBytesRefsOffset(suppliers)));
    }

    /**
     * Adds a test case with no rows, expecting null, to the list of suppliers.
     */
    protected static List<TestCaseSupplier> withNoRowsExpectingNull(List<TestCaseSupplier> suppliers) {
        List<TestCaseSupplier> newSuppliers = new ArrayList<>(suppliers);
        Set<List<DataType>> uniqueSignatures = new HashSet<>();

        for (TestCaseSupplier original : suppliers) {
            if (uniqueSignatures.add(original.types())) {
                newSuppliers.add(new TestCaseSupplier(original.name() + " with no rows", original.types(), () -> {
                    var testCase = original.get();

                    if (testCase.getData().stream().noneMatch(TestCaseSupplier.TypedData::isMultiRow)) {
                        // Fail if no multi-row data, at least until a real case is found
                        fail("No multi-row data found in test case: " + testCase);
                    }

                    var newData = testCase.getData().stream().map(td -> td.isMultiRow() ? td.withData(List.of()) : td).toList();

                    return new TestCaseSupplier.TestCase(
                        testCase.getSource(),
                        testCase.getConfiguration(),
                        newData,
                        testCase.evaluatorToString(),
                        testCase.expectedType(),
                        nullValue(),
                        null,
                        null,
                        testCase.getExpectedTypeError(),
                        null,
                        null,
                        null,
                        testCase.canBuildEvaluator()
                    );
                }));
            }
        }

        return newSuppliers;
    }

    /**
     * Returns true if the aggregation gives the same result given the same inputs. False otherwise.
     */
    protected boolean isDeterministic() {
        return true;
    }

    public void testAggregate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        executeExpression(expression, this::aggregateSingleMode);
    }

    public void testAggregateToString() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        assertAggregatorToString(expression, false);
    }

    public void testGroupingAggregate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        executeExpression(expression, this::aggregateGroupingSingleMode);
    }

    public void testGroupingAggregateToString() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        assertAggregatorToString(expression, true);
    }

    public void testAggregateIntermediate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        executeExpression(expression, this::aggregateWithIntermediates);
    }

    public void testFold() {
        Expression expression = buildLiteralExpression(testCase);

        executeExpression(expression, (agg, pages) -> { throw new AssumptionViolatedException("Aggregate not foldable"); });
    }

    public void testSurrogateHasFilter() {
        Expression expression = randomFrom(
            buildLiteralExpression(testCase),
            buildDeepCopyOfFieldExpression(testCase),
            buildFieldExpression(testCase)
        );

        assumeTrue("expression should have no type errors", expression.typeResolved().resolved());

        if (expression instanceof AggregateFunction && expression instanceof SurrogateExpression) {
            var filter = ((AggregateFunction) expression).filter();

            var surrogate = ((SurrogateExpression) expression).surrogate();

            if (surrogate != null) {
                surrogate.forEachDown(AggregateFunction.class, child -> {
                    var surrogateFilter = child.filter();
                    assertEquals(filter, surrogateFilter);
                });
            }
        }
    }

    private Object aggregateSingleMode(Expression expression, List<Page> pages) {
        Object result;
        try (var aggregator = aggregator(expression, initialInputChannels(), AggregatorMode.SINGLE)) {
            for (Page inputPage : pages) {
                if (inputPage.getPositionCount() > 0) {
                    try (
                        BooleanVector noMasking = driverContext().blockFactory()
                            .newConstantBooleanVector(true, inputPage.getPositionCount())
                    ) {
                        aggregator.processPage(inputPage, noMasking);
                    }
                }
            }

            result = extractResultFromAggregator(aggregator, PlannerUtils.toElementType(expression.dataType()));
        }

        return result;
    }

    private Object aggregateGroupingSingleMode(Expression expression, List<Page> pages) {
        List<Object> results;
        assumeFalse(
            "Grouping aggregations must receive data to check results",
            pages.isEmpty() || pages.getFirst().getPositionCount() == 0
        );

        try (var aggregator = groupingAggregator(expression, initialInputChannels(), AggregatorMode.SINGLE)) {
            var groupCount = randomIntBetween(1, 1000);
            for (Page inputPage : pages) {
                if (inputPage.getPositionCount() > 0) {
                    processPageGrouping(aggregator, inputPage, groupCount);
                }
            }

            results = extractResultsFromAggregator(aggregator, PlannerUtils.toElementType(expression.dataType()), groupCount);
        }

        if (isDeterministic()) {
            assertThat("All groups must have the same result", results.stream().distinct().count(), is(1L));
        }

        assert results.isEmpty() == false;
        return results.getFirst();
    }

    private Object aggregateWithIntermediates(Expression expression, List<Page> pages) {
        int intermediateBlockOffset = randomIntBetween(0, 10);
        Block[] intermediateBlocks;
        int intermediateStates;

        // Input rows to intermediate states
        try (var aggregator = aggregator(expression, initialInputChannels(), AggregatorMode.INITIAL)) {
            intermediateStates = aggregator.evaluateBlockCount();

            int intermediateBlockExtraSize = randomIntBetween(0, 10);
            intermediateBlocks = new Block[intermediateBlockOffset + intermediateStates + intermediateBlockExtraSize];

            for (Page inputPage : pages) {
                if (inputPage.getPositionCount() > 0) {
                    try (
                        BooleanVector noMasking = driverContext().blockFactory()
                            .newConstantBooleanVector(true, inputPage.getPositionCount())
                    ) {
                        aggregator.processPage(inputPage, noMasking);
                    }
                }
            }

            aggregator.evaluate(intermediateBlocks, intermediateBlockOffset, driverContext());

            int positionCount = intermediateBlocks[intermediateBlockOffset].getPositionCount();

            // Fill offset and extra blocks with nulls
            for (int i = 0; i < intermediateBlockOffset; i++) {
                intermediateBlocks[i] = driverContext().blockFactory().newConstantNullBlock(positionCount);
            }
            for (int i = intermediateBlockOffset + intermediateStates; i < intermediateBlocks.length; i++) {
                intermediateBlocks[i] = driverContext().blockFactory().newConstantNullBlock(positionCount);
            }
        }

        Object result;
        // Intermediate states to final result
        try (
            var aggregator = aggregator(
                expression,
                intermediaryInputChannels(intermediateStates, intermediateBlockOffset),
                AggregatorMode.FINAL
            )
        ) {
            Page inputPage = new Page(intermediateBlocks);
            try (BooleanVector noMasking = driverContext().blockFactory().newConstantBooleanVector(true, inputPage.getPositionCount())) {
                if (inputPage.getPositionCount() > 0) {
                    aggregator.processPage(inputPage, noMasking);
                }
            } finally {
                inputPage.releaseBlocks();
            }

            result = extractResultFromAggregator(aggregator, PlannerUtils.toElementType(expression.dataType()));
        }

        return result;
    }

    private Block executeLeafEvaluableExpression(Expression expression) {
        try (
            BlockUtils.BuilderWrapper wrapper = BlockUtils.wrapperFor(
                driverContext().blockFactory(),
                PlannerUtils.toElementType(expression.dataType()),
                0
            )
        ) {
            List<Object> results = new ArrayList<>();
            try (EvalOperator.ExpressionEvaluator evaluator = evaluator(expression).get(driverContext())) {
                // TODO: This should look at the layout to place the correct blocks in the correct places
                for (Page inputPage : rows(testCase.getMultiRowFields())) {
                    try (Block block = evaluator.eval(inputPage)) {
                        assertThat(block.getPositionCount(), is(inputPage.getPositionCount()));
                        BlockTestUtils.readInto(results, block);

                        for (int i = 0; i < block.getPositionCount(); i++) {
                            wrapper.accept(BlockTestUtils.valuesAtPosition(block, i, false));
                        }
                    } finally {
                        inputPage.releaseBlocks();
                    }
                }
            }
            return wrapper.builder().build();
        }
    }

    /**
     * Executes an aggregation.
     * <p>
     *     Surrogates may return something like `DIV(SUM(MV_MIN(x)), COUNT(x))`. This has 3 layers:
     * </p>
     * <ol>
     *     <li>The optional outer evaluable function, working individually on every input row (DIV)</li>
     *     <li>The actual aggs, consuming all the input rows (SUM, COUNT)</li>
     *     <li>The optional inner evaluable function, working on a single agg output row (MV_MIN)</li>
     * </ol>
     * <p>
     *     We resolve them from inside-out, following these steps:
     * </p>
     * <ol>
     *     <li>Resolve the expression: Surrogates, null folding...</li>
     *     <li>Evaluate the aggs children, which must be non-agg functions</li>
     *     <li>Evaluate the aggs, calling {@code executeAggregator} for each of them</li>
     *     <li>Fold the final function, if any</li>
     * </ol>
     * <p>
     *     Depending on the presence or absence of those surrogate structures, we'll just short-circuit wherever required.
     * </p>
     * @param originalExpression the expression to execute. Expected to be an aggregation
     * @param executeAggregator a function that will handle the execution of an aggregation, given the page with the inputs.
     *                          It allows us to test the execution with different modes (single, grouping, intermediate...)
     */
    private void executeExpression(Expression originalExpression, BiFunction<Expression, List<Page>, Object> executeAggregator) {
        Expression expression = resolveExpression(originalExpression);
        // Not expected to be evaluated (E.g. type error already checked)
        if (expression == null) {
            return;
        }

        boolean isExecutableAgg = expression instanceof AggregateFunction
            && expression.children()
                .stream()
                .allMatch(child -> child instanceof FieldAttribute || child instanceof DeepCopy || child instanceof Literal);

        // If there was no surrogate, or it didn't add evaluable functions, short-circuit for the happy-path
        if (isExecutableAgg) {
            var result = executeAggregator.apply(expression, rows(testCase.getMultiRowFields()));
            assertTestCaseResultAndWarnings(result);
            return;
        }

        // Maps with every evaluation result, which will help us simulating the Layout and pages of each step
        Map<Expression, Block> blocksByField = new HashMap<>();
        Map<Expression, Literal> literalsByField = new HashMap<>();
        try {
            // Populate maps with the test input data
            int dataIndex = 0;
            for (int i = 0; i < originalExpression.children().size(); i++) {
                Expression field = originalExpression.children().get(i);

                if (field instanceof Literal) {
                    continue;
                }

                var data = testCase.getData().get(dataIndex++);

                if (data.isMultiRow()) {
                    blocksByField.put(
                        field,
                        BlockTestUtils.asBlock(
                            driverContext().blockFactory(),
                            PlannerUtils.toElementType(field.dataType()),
                            data.multiRowData()
                        )
                    );
                } else {
                    literalsByField.put(field, data.asLiteral());
                }
            }

            // Resolve agg children and store their results in the literal/blocks maps
            Expression expressionWithResolvedAggChildren = expression.transformUp(AggregateFunction.class, agg -> {
                var newChildren = new ArrayList<Expression>(agg.children().size());
                for (Expression child : agg.children()) {
                    if (child.foldable()) {
                        var literal = Literal.of(child, child.fold(FoldContext.small()));
                        literalsByField.put(child, literal);
                        newChildren.add(literal);
                    } else {
                        if (blocksByField.containsKey(child) == false) {
                            Block result = executeLeafEvaluableExpression(child);
                            blocksByField.put(child, result);
                        }
                        newChildren.add(child);
                    }
                }

                return agg.replaceChildren(newChildren);
            });

            Function<Expression, Object> resolveAgg = (agg) -> {
                // Holder used to release blocks generated in this section that aren't part of the blocksByField map
                Holder<Block> releasableBlock = new Holder<>();
                try (Releasable releasable = () -> {
                    if (releasableBlock.get() != null) {
                        releasableBlock.get().close();
                    }
                }) {
                    Block[] blocks = agg.children()
                        .stream()
                        .filter(child -> child instanceof Literal == false)
                        .map(blocksByField::get)
                        .toArray(Block[]::new);
                    if (blocks.length == 0) {
                        var justPositionsBlock = driverContext().blockFactory()
                            .newConstantNullBlock(testCase.getMultiRowFields().getFirst().multiRowData().size());
                        releasableBlock.setOnce(justPositionsBlock);
                        // Handle cases like COUNT(*), where there are no input fields
                        blocks = new Block[] { justPositionsBlock };
                    }
                    // Generate the page based on the resolved blocks
                    Page inputPage = new Page(blocks);
                    return executeAggregator.apply(agg, List.of(inputPage));
                }
            };

            // No top-level evaluators, short-circuit
            if (expressionWithResolvedAggChildren instanceof AggregateFunction) {
                var result = resolveAgg.apply(expressionWithResolvedAggChildren);
                assertTestCaseResultAndWarnings(result);
                return;
            }

            // Resolve nested aggs
            Expression expressionWithResolvedAggs = expressionWithResolvedAggChildren.transformUp(e -> {
                var newExpressionChildren = new ArrayList<Expression>(e.children().size());
                boolean changed = false;
                // Iterate agg children only
                for (Expression agg : e.children()) {
                    if (agg instanceof AggregateFunction == false) {
                        newExpressionChildren.add(agg);
                        continue;
                    }

                    var resultLiteral = Literal.of(agg, resolveAgg.apply(agg));

                    literalsByField.put(agg, resultLiteral);
                    newExpressionChildren.add(resultLiteral);
                    changed = true;
                }
                return changed ? e.replaceChildren(newExpressionChildren) : e;
            });

            // Resolve final evaluation
            Object result = Foldables.valueOf(FoldContext.small(), expressionWithResolvedAggs);
            assertTestCaseResultAndWarnings(result);
        } finally {
            Releasables.close(blocksByField.values());
        }
    }

    /**
     * Asserts the evaluator toString of the given expression.
     */
    private void assertAggregatorToString(Expression originalExpression, boolean grouping) {
        Expression expression = resolveExpression(originalExpression);
        if (expression == null) {
            return;
        }

        assumeTrue(
            "Surrogate expression with non-trivial children cannot be evaluated",
            expression.children()
                .stream()
                .allMatch(child -> child instanceof FieldAttribute || child instanceof DeepCopy || child instanceof Literal)
        );

        if (expression instanceof AggregateFunction == false) {
            // Not an aggregator
            return;
        }

        assertThat(expression, instanceOf(ToAggregator.class));
        logger.info("Result type: " + expression.dataType());

        try (
            var aggregator = grouping
                ? groupingAggregator(expression, initialInputChannels(), AggregatorMode.SINGLE)
                : aggregator(expression, initialInputChannels(), AggregatorMode.SINGLE)
        ) {
            assertAggregatorToString(aggregator);
        }
    }

    /**
     * Resolves the expression and checks type resolution and expected type.
     * @return the resolved, ready-to-execute expression, or null if it wasn't resolved
     */
    private Expression resolveExpression(Expression expression) {
        String valuesString = testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","));
        if (valuesString.length() > 200) {
            valuesString = valuesString.substring(0, 200) + "...";
        }
        logger.info("Test Values: " + valuesString);
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return null;
        }
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));
        expression = resolveSurrogates(expression);

        // Fold nulls
        expression = expression.transformUp(e -> new FoldNull().rule(e, unboundLogicalOptimizerContext()));
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));

        // Replace null aggs
        expression = expression.transformUp(AggregateFunction.class, agg -> {
            if (ReplaceStatsFilteredOrNullAggWithEval.shouldReplace(agg)) {
                return Literal.of(agg, ReplaceStatsFilteredOrNullAggWithEval.mapNullToValue(agg));
            }
            return agg;
        });
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));

        Expression.TypeResolution resolution = expression.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }

        return expression;
    }

    private Object extractResultFromAggregator(Aggregator aggregator, ElementType expectedElementType) {
        var blocksArraySize = randomIntBetween(1, 10);
        var resultBlockIndex = randomIntBetween(0, blocksArraySize - 1);
        var blocks = new Block[blocksArraySize];
        try {
            aggregator.evaluate(blocks, resultBlockIndex, driverContext());

            var block = blocks[resultBlockIndex];

            // For null blocks, the element type is NULL, so if the provided matcher matches, the type works too
            assertThat(block.elementType(), is(oneOf(expectedElementType, ElementType.NULL)));

            return toJavaObject(blocks[resultBlockIndex], 0);
        } finally {
            Releasables.close(blocks);
        }
    }

    /**
     * Returns a list of results, one for each group from 0 to {@code groupCount}
     */
    private List<Object> extractResultsFromAggregator(GroupingAggregator aggregator, ElementType expectedElementType, int groupCount) {
        var blocksArraySize = randomIntBetween(1, 10);
        var resultBlockIndex = randomIntBetween(0, blocksArraySize - 1);
        var blocks = new Block[blocksArraySize];
        try (var groups = driverContext().blockFactory().newIntRangeVector(0, groupCount)) {
            aggregator.evaluate(blocks, resultBlockIndex, groups, new GroupingAggregatorEvaluationContext(driverContext()));

            var block = blocks[resultBlockIndex];

            // For null blocks, the element type is NULL, so if the provided matcher matches, the type works too
            assertThat(block.elementType(), is(oneOf(expectedElementType, ElementType.NULL)));

            return IntStream.range(0, groupCount).mapToObj(position -> toJavaObject(blocks[resultBlockIndex], position)).toList();
        } finally {
            Releasables.close(blocks);
        }
    }

    private List<Integer> initialInputChannels() {
        // TODO: Randomize channels. If surrogated, channels may change
        return IntStream.range(0, testCase.getMultiRowFields().size()).boxed().toList();
    }

    private List<Integer> intermediaryInputChannels(int intermediaryStates, int offset) {
        return IntStream.range(offset, offset + intermediaryStates).boxed().toList();
    }

    /**
     * Resolves surrogates of aggregations until a non-surrogate expression is found.
     * <p>
     *     No-op if expecting errors, as surrogates depend on correct types
     * </p>
     */
    private Expression resolveSurrogates(Expression expression) {
        if (testCase.getExpectedTypeError() != null) {
            return expression;
        }

        // Run agg surrogates twice
        // This simulates the double aggs surrogation in LogicalPlanOptimizer
        for (int i = 0; i < 2; i++) {
            expression = expression.transformUp(AggregateFunction.class, agg -> {
                if (agg instanceof SurrogateExpression se) {
                    var surrogate = se.surrogate();
                    if (surrogate != null) {
                        return surrogate;
                    }
                }
                return agg;
            });
        }

        expression = SubstituteSurrogateExpressions.rule(expression);

        return expression;
    }

    private Aggregator aggregator(Expression expression, List<Integer> inputChannels, AggregatorMode mode) {
        AggregatorFunctionSupplier aggregatorFunctionSupplier = ((ToAggregator) expression).supplier();

        return new Aggregator(aggregatorFunctionSupplier.aggregator(driverContext(), inputChannels), mode);
    }

    private GroupingAggregator groupingAggregator(Expression expression, List<Integer> inputChannels, AggregatorMode mode) {
        AggregatorFunctionSupplier aggregatorFunctionSupplier = ((ToAggregator) expression).supplier();

        return new GroupingAggregator(aggregatorFunctionSupplier.groupingAggregator(driverContext(), inputChannels), mode);
    }

    /**
     * Make a groupsId block with all the groups in the range for each row.
     */
    private IntBlock makeGroupsVector(int groupStart, int groupEnd, int rowCount) {
        try (var groupsBuilder = driverContext().blockFactory().newIntBlockBuilder(rowCount)) {
            for (var i = 0; i < rowCount; i++) {
                groupsBuilder.beginPositionEntry();
                for (int groupId = groupStart; groupId < groupEnd; groupId++) {
                    groupsBuilder.appendInt(groupId);
                }
                groupsBuilder.endPositionEntry();
            }

            return groupsBuilder.build();
        }
    }

    /**
     * Process the page with the aggregator. Adds all the values in all the groups in the range [0, {@code groupCount}).
     * <p>
     *   This method splits the data and groups in chunks, to test the aggregator capabilities.
     * </p>
     */
    private void processPageGrouping(GroupingAggregator aggregator, Page inputPage, int groupCount) {
        var groupSliceSize = 1;
        var allValuesNull = IntStream.range(0, inputPage.getBlockCount())
            .<Block>mapToObj(inputPage::getBlock)
            .anyMatch(Block::areAllValuesNull);
        // Add data to chunks of groups
        for (int currentGroupOffset = 0; currentGroupOffset < groupCount;) {
            int groupSliceRemainingSize = Math.min(groupSliceSize, groupCount - currentGroupOffset);
            var seenGroupIds = new SeenGroupIds.Range(0, allValuesNull ? 0 : currentGroupOffset + groupSliceRemainingSize);
            try (GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessPage(seenGroupIds, inputPage)) {
                var positionCount = inputPage.getPositionCount();
                var dataSliceSize = 1;
                // Divide data in chunks
                for (int currentDataOffset = 0; currentDataOffset < positionCount;) {
                    int dataSliceRemainingSize = Math.min(dataSliceSize, positionCount - currentDataOffset);
                    try (
                        var groups = makeGroupsVector(
                            currentGroupOffset,
                            currentGroupOffset + groupSliceRemainingSize,
                            dataSliceRemainingSize
                        )
                    ) {
                        addInput.add(currentDataOffset, groups);
                    }

                    currentDataOffset += dataSliceSize;
                    if (positionCount > currentDataOffset) {
                        dataSliceSize = randomIntBetween(1, Math.min(100, positionCount - currentDataOffset));
                    }
                }
            }

            currentGroupOffset += groupSliceSize;
            if (groupCount > currentGroupOffset) {
                groupSliceSize = randomIntBetween(1, Math.min(100, groupCount - currentGroupOffset));
            }
        }
    }

    private void assertAggregatorToString(Object aggregator) {
        String expectedStart = switch (aggregator) {
            case Aggregator a -> "Aggregator[aggregatorFunction=";
            case GroupingAggregator a -> "GroupingAggregator[aggregatorFunction=";
            default -> throw new UnsupportedOperationException("can't check toString for [" + aggregator.getClass() + "]");
        };
        String channels = initialInputChannels().stream().map(Object::toString).collect(Collectors.joining(", "));
        String expectedEnd = switch (aggregator) {
            case Aggregator a -> "AggregatorFunction[channels=[" + channels + "]], mode=SINGLE]";
            case GroupingAggregator a -> "GroupingAggregatorFunction[channels=[" + channels + "]], mode=SINGLE]";
            default -> throw new UnsupportedOperationException("can't check toString for [" + aggregator.getClass() + "]");
        };

        String toString = aggregator.toString();
        assertThat(toString, startsWith(expectedStart));
        assertThat(toString, endsWith(expectedEnd));
        assertThat(toString.substring(expectedStart.length(), toString.length() - expectedEnd.length()), testCase.evaluatorToString());
    }

    protected static String standardAggregatorName(String prefix, DataType type) {
        String typeName = switch (type) {
            case BOOLEAN -> "Boolean";
            case CARTESIAN_POINT -> "CartesianPoint";
            case CARTESIAN_SHAPE -> "CartesianShape";
            case GEO_POINT -> "GeoPoint";
            case GEO_SHAPE -> "GeoShape";
            case KEYWORD, TEXT, VERSION, TSID_DATA_TYPE -> "BytesRef";
            case DOUBLE, COUNTER_DOUBLE -> "Double";
            case INTEGER, COUNTER_INTEGER -> "Int";
            case IP -> "Ip";
            case DATETIME, DATE_NANOS, LONG, COUNTER_LONG, UNSIGNED_LONG, GEOHASH, GEOTILE, GEOHEX -> "Long";
            case AGGREGATE_METRIC_DOUBLE -> "AggregateMetricDouble";
            case DATE_RANGE -> "LongRange";
            case EXPONENTIAL_HISTOGRAM -> "ExponentialHistogram";
            case NULL -> "Null";
            case TDIGEST -> "TDigest";
            default -> throw new UnsupportedOperationException("name for [" + type + "]");
        };
        return prefix + typeName;
    }

    protected static String standardAggregatorNameAllBytesTheSame(String prefix, DataType type) {
        return standardAggregatorName(prefix, switch (type) {
            case CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_POINT, GEO_SHAPE, IP -> DataType.KEYWORD;
            default -> type;
        });
    }
}
