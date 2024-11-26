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
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.FoldNull;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

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
        boolean entirelyNullPreservesType,
        PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        return parameterSuppliersFromTypedData(
            errorsForCasesWithoutExamples(
                withNoRowsExpectingNull(anyNullIsNull(entirelyNullPreservesType, randomizeBytesRefsOffset(suppliers))),
                positionalErrorMessageSupplier
            )
        );
    }

    // TODO: Remove and migrate everything to the method with all the parameters
    /**
     * @deprecated Use {@link #parameterSuppliersFromTypedDataWithDefaultChecks(List, boolean, PositionalErrorMessageSupplier)} instead.
     * This method doesn't add all the default checks.
     */
    @Deprecated
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
                        newData,
                        testCase.evaluatorToString(),
                        testCase.expectedType(),
                        nullValue(),
                        null,
                        null,
                        testCase.getExpectedTypeError(),
                        null,
                        null,
                        null
                    );
                }));
            }
        }

        return newSuppliers;
    }

    public void testAggregate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        resolveExpression(expression, this::aggregateSingleMode, this::evaluate);
    }

    public void testGroupingAggregate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        resolveExpression(expression, this::aggregateGroupingSingleMode, this::evaluate);
    }

    public void testAggregateIntermediate() {
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);

        resolveExpression(expression, this::aggregateWithIntermediates, this::evaluate);
    }

    public void testFold() {
        Expression expression = buildLiteralExpression(testCase);

        resolveExpression(expression, aggregatorFunctionSupplier -> {
            // An aggregation cannot be folded.
            // It's not an error either as not all aggregations are foldable.
        }, this::evaluate);
    }

    private void aggregateSingleMode(Expression expression) {
        Object result;
        try (var aggregator = aggregator(expression, initialInputChannels(), AggregatorMode.SINGLE)) {
            for (Page inputPage : rows(testCase.getMultiRowFields())) {
                try (
                    BooleanVector noMasking = driverContext().blockFactory().newConstantBooleanVector(true, inputPage.getPositionCount())
                ) {
                    aggregator.processPage(inputPage, noMasking);
                } finally {
                    inputPage.releaseBlocks();
                }
            }

            result = extractResultFromAggregator(aggregator, PlannerUtils.toElementType(testCase.expectedType()));
        }

        assertTestCaseResultAndWarnings(result);
    }

    private void aggregateGroupingSingleMode(Expression expression) {
        var pages = rows(testCase.getMultiRowFields());
        List<Object> results;
        try {
            assumeFalse("Grouping aggregations must receive data to check results", pages.isEmpty());

            try (var aggregator = groupingAggregator(expression, initialInputChannels(), AggregatorMode.SINGLE)) {
                var groupCount = randomIntBetween(1, 1000);
                for (Page inputPage : pages) {
                    processPageGrouping(aggregator, inputPage, groupCount);
                }

                results = extractResultsFromAggregator(aggregator, PlannerUtils.toElementType(testCase.expectedType()), groupCount);
            }
        } finally {
            for (var page : pages) {
                page.releaseBlocks();
            }
        }

        for (var result : results) {
            assertThat(result, not(equalTo(Double.NaN)));
            assert testCase.getMatcher().matches(Double.POSITIVE_INFINITY) == false;
            assertThat(result, not(equalTo(Double.POSITIVE_INFINITY)));
            assert testCase.getMatcher().matches(Double.NEGATIVE_INFINITY) == false;
            assertThat(result, not(equalTo(Double.NEGATIVE_INFINITY)));
            assertThat(result, testCase.getMatcher());
            if (testCase.getExpectedWarnings() != null) {
                assertWarnings(testCase.getExpectedWarnings());
            }
        }
    }

    private void aggregateWithIntermediates(Expression expression) {
        int intermediateBlockOffset = randomIntBetween(0, 10);
        Block[] intermediateBlocks;
        int intermediateStates;

        // Input rows to intermediate states
        try (var aggregator = aggregator(expression, initialInputChannels(), AggregatorMode.INITIAL)) {
            intermediateStates = aggregator.evaluateBlockCount();

            int intermediateBlockExtraSize = randomIntBetween(0, 10);
            intermediateBlocks = new Block[intermediateBlockOffset + intermediateStates + intermediateBlockExtraSize];

            for (Page inputPage : rows(testCase.getMultiRowFields())) {
                try (
                    BooleanVector noMasking = driverContext().blockFactory().newConstantBooleanVector(true, inputPage.getPositionCount())
                ) {
                    aggregator.processPage(inputPage, noMasking);
                } finally {
                    inputPage.releaseBlocks();
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

            result = extractResultFromAggregator(aggregator, PlannerUtils.toElementType(testCase.expectedType()));
        }

        assertTestCaseResultAndWarnings(result);
    }

    private void evaluate(Expression evaluableExpression) {
        assertTrue(evaluableExpression.foldable());

        if (testCase.foldingExceptionClass() != null) {
            Throwable t = expectThrows(testCase.foldingExceptionClass(), evaluableExpression::fold);
            assertThat(t.getMessage(), equalTo(testCase.foldingExceptionMessage()));
            return;
        }

        Object result = evaluableExpression.fold();
        // Decode unsigned longs into BigIntegers
        if (testCase.expectedType() == DataType.UNSIGNED_LONG && result != null) {
            result = NumericUtils.unsignedLongAsBigInteger((Long) result);
        }
        assertTestCaseResultAndWarnings(result);
    }

    private void resolveExpression(Expression expression, Consumer<Expression> onAggregator, Consumer<Expression> onEvaluableExpression) {
        logger.info(
            "Test Values: " + testCase.getData().stream().map(TestCaseSupplier.TypedData::toString).collect(Collectors.joining(","))
        );
        if (testCase.getExpectedTypeError() != null) {
            assertTypeResolutionFailure(expression);
            return;
        }
        expression = resolveSurrogates(expression);

        // As expressions may be composed of multiple functions, we need to fold nulls bottom-up
        expression = expression.transformUp(e -> new FoldNull().rule(e));
        assertThat(expression.dataType(), equalTo(testCase.expectedType()));

        Expression.TypeResolution resolution = expression.typeResolved();
        if (resolution.unresolved()) {
            throw new AssertionError("expected resolved " + resolution.message());
        }

        assumeTrue(
            "Surrogate expression with non-trivial children cannot be evaluated",
            expression.children()
                .stream()
                .allMatch(child -> child instanceof FieldAttribute || child instanceof DeepCopy || child instanceof Literal)
        );

        if (expression instanceof AggregateFunction == false) {
            onEvaluableExpression.accept(expression);
            return;
        }

        assertThat(expression, instanceOf(ToAggregator.class));
        logger.info("Result type: " + expression.dataType());

        onAggregator.accept(expression);
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
        try (var groups = IntVector.range(0, groupCount, driverContext().blockFactory())) {
            aggregator.evaluate(blocks, resultBlockIndex, groups, driverContext());

            var block = blocks[resultBlockIndex];

            // For null blocks, the element type is NULL, so if the provided matcher matches, the type works too
            assertThat(block.elementType(), is(oneOf(expectedElementType, ElementType.NULL)));

            return IntStream.range(resultBlockIndex, groupCount)
                .mapToObj(position -> toJavaObject(blocks[resultBlockIndex], position))
                .toList();
        } finally {
            Releasables.close(blocks);
        }
    }

    private List<Integer> initialInputChannels() {
        // TODO: Randomize channels
        // TODO: If surrogated, channels may change
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

        for (int i = 0;; i++) {
            assertThat("Potential infinite loop detected in surrogates", i, lessThan(10));

            if (expression instanceof SurrogateExpression == false) {
                break;
            }

            var surrogate = ((SurrogateExpression) expression).surrogate();

            if (surrogate == null) {
                break;
            }

            expression = surrogate;
        }

        return expression;
    }

    private Aggregator aggregator(Expression expression, List<Integer> inputChannels, AggregatorMode mode) {
        AggregatorFunctionSupplier aggregatorFunctionSupplier = ((ToAggregator) expression).supplier(inputChannels);

        return new Aggregator(aggregatorFunctionSupplier.aggregator(driverContext()), mode);
    }

    private GroupingAggregator groupingAggregator(Expression expression, List<Integer> inputChannels, AggregatorMode mode) {
        AggregatorFunctionSupplier aggregatorFunctionSupplier = ((ToAggregator) expression).supplier(inputChannels);

        return new GroupingAggregator(aggregatorFunctionSupplier.groupingAggregator(driverContext()), mode);
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
}
