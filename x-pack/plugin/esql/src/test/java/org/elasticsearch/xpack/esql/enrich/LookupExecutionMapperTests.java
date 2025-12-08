/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupMergeDropExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class LookupExecutionMapperTests extends ESTestCase {
    private BlockFactory blockFactory;
    private BigArrays bigArrays;
    private List<Releasable> releasables;
    private Directory testDirectory;
    private DirectoryReader testReader;
    private MapperService mapperService;

    @Before
    public void setup() throws IOException {
        blockFactory = TestBlockFactory.getNonBreakingInstance();
        bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking();
        releasables = new ArrayList<>();

        // Create a minimal index for testing
        testDirectory = newDirectory();
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), testDirectory)) {
            writer.commit();
        }
        testReader = DirectoryReader.open(testDirectory);
        MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
        };
        String mapping = "{\n  \"doc\": { \"properties\": { \"field1\": { \"type\": \"keyword\" } } }\n}";
        mapperService = mapperHelper.createMapperService(mapping);
        releasables.add(() -> {
            try {
                org.elasticsearch.core.IOUtils.close(testReader, mapperService, testDirectory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @After
    public void cleanup() {
        Releasables.close(releasables);
        blockFactory = null;
        bigArrays = null;
    }

    /**
     * Creates a physical plan matching the structure from createLookupPhysicalPlan():
     * OutputExec -> LookupMergeDropExec -> (FieldExtractExec?) -> ParameterizedQueryExec
     */
    private PhysicalPlan createLookupPhysicalPlan(List<NamedExpression> extractFields) {
        // Create doc attribute
        FieldAttribute docAttribute = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            EsQueryExec.DOC_ID_FIELD.getName(),
            EsQueryExec.DOC_ID_FIELD
        );
        List<Attribute> sourceOutput = List.of(docAttribute);

        // Create ParameterizedQueryExec
        LookupEnrichQueryGenerator queryList = mock(LookupEnrichQueryGenerator.class);
        ParameterizedQueryExec source = new ParameterizedQueryExec(Source.EMPTY, sourceOutput, queryList);

        PhysicalPlan plan = source;

        // Add FieldExtractExec if we have extract fields
        if (extractFields.isEmpty() == false) {
            List<Attribute> extractAttributes = new ArrayList<>();
            for (NamedExpression extractField : extractFields) {
                extractAttributes.add(extractField.toAttribute());
            }
            plan = new FieldExtractExec(Source.EMPTY, plan, extractAttributes, MappedFieldType.FieldExtractPreference.NONE);
        }

        // Add LookupMergeDropExec
        final ElementType[] mergingTypes = new ElementType[extractFields.size()];
        for (int i = 0; i < extractFields.size(); i++) {
            mergingTypes[i] = org.elasticsearch.xpack.esql.planner.PlannerUtils.toElementType(extractFields.get(i).dataType());
        }
        final int[] mergingChannels = IntStream.range(0, extractFields.size()).map(i -> i + 2).toArray();
        plan = new LookupMergeDropExec(Source.EMPTY, plan, extractFields, mergingTypes, mergingChannels);

        // Add OutputExec
        plan = new OutputExec(Source.EMPTY, plan, page -> {});

        return plan;
    }

    public void testEnrichLookupNoMerge() throws IOException {
        // Test enrich lookup with mergePages=false and no extract fields (lookup join case)
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            false
        );

        BytesRefBlock inputBlock = blockFactory.newBytesRefBlockBuilder(2)
            .appendBytesRef(new BytesRef("a"))
            .appendBytesRef(new BytesRef("b"))
            .build();
        Page inputPage = new Page(inputBlock);

        // No extract fields - just lookup join
        List<NamedExpression> extractFields = Collections.emptyList();
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ProjectOperator -> OutputOperator
        verifyCompletePlan(queryPlan, List.of(EnrichQuerySourceOperator.class, ProjectOperator.class, OutputOperator.class));

        // Verify EnrichQuerySourceOperator
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);

        // Verify ProjectOperator
        ProjectOperator projectOp = (ProjectOperator) queryPlan.operators().get(0);
        verifyProjectOperator(projectOp, extractFields.size());

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    public void testEnrichLookupDictionaryOptimization() throws IOException {
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            true
        );

        // Create a BytesRefBlock with ordinals for dictionary optimization
        BytesRefVector dictionary = blockFactory.newBytesRefVectorBuilder(2)
            .appendBytesRef(new BytesRef("a"))
            .appendBytesRef(new BytesRef("b"))
            .build();
        IntBlock ordinals = blockFactory.newIntBlockBuilder(3).appendInt(0).appendInt(1).appendInt(0).build();
        OrdinalBytesRefBlock ordinalBlock = new OrdinalBytesRefBlock(ordinals, dictionary);
        Page inputPage = new Page(ordinalBlock);

        List<NamedExpression> extractFields = List.of(
            createFieldAttribute("field1", DataType.KEYWORD),
            createFieldAttribute("field2", DataType.INTEGER)
        );
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> MergePositionsOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, MergePositionsOperator.class, OutputOperator.class)
        );

        // Verify EnrichQuerySourceOperator with dictionary optimization
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);
        verifyDictionaryOptimization(sourceOp, 2);

        // Verify ValuesSourceReaderOperator
        ValuesSourceReaderOperator valuesOp = verifyValuesSourceReaderOperator(queryPlan, 0);

        // Verify MergePositionsOperator with dictionary ordinals
        MergePositionsOperator mergeOp = (MergePositionsOperator) queryPlan.operators().get(1);
        verifyMergePositionsOperator(mergeOp, 3, false); // 3 positions, not range block

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    public void testEnrichLookupRangeBlockOptimization() throws IOException {
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            true
        );

        BytesRefBlock inputBlock = blockFactory.newBytesRefBlockBuilder(3)
            .appendBytesRef(new BytesRef("a"))
            .appendBytesRef(new BytesRef("b"))
            .appendBytesRef(new BytesRef("c"))
            .build();
        Page inputPage = new Page(inputBlock);

        List<NamedExpression> extractFields = List.of(
            createFieldAttribute("field1", DataType.KEYWORD),
            createFieldAttribute("field2", DataType.INTEGER),
            createFieldAttribute("field3", DataType.LONG)
        );
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> MergePositionsOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, MergePositionsOperator.class, OutputOperator.class)
        );

        // Verify EnrichQuerySourceOperator with range block optimization (no optimization on input page)
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);
        verifyNoPageOptimization(sourceOp, inputBlock);

        // Verify ValuesSourceReaderOperator
        ValuesSourceReaderOperator valuesOp = verifyValuesSourceReaderOperator(queryPlan, 0);

        // Verify MergePositionsOperator with range block
        MergePositionsOperator mergeOp = (MergePositionsOperator) queryPlan.operators().get(1);
        verifyMergePositionsOperator(mergeOp, 3, true); // 3 positions, range block

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    public void testLookupJoinNoExtraFields() throws IOException {
        // Test lookup join where we don't need to get extra fields (no extract fields, no merge)
        // This is the simplest lookup join case - just joining without extracting additional fields
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            false
        );

        BytesRefBlock inputBlock = blockFactory.newBytesRefBlockBuilder(3)
            .appendBytesRef(new BytesRef("key1"))
            .appendBytesRef(new BytesRef("key2"))
            .appendBytesRef(new BytesRef("key3"))
            .build();
        Page inputPage = new Page(inputBlock);

        // No extract fields - pure lookup join without extra field extraction
        List<NamedExpression> extractFields = Collections.emptyList();
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ProjectOperator -> OutputOperator
        // No ValuesSourceReaderOperator (no extract fields), no MergePositionsOperator (no merge)
        verifyCompletePlan(queryPlan, List.of(EnrichQuerySourceOperator.class, ProjectOperator.class, OutputOperator.class));

        // Verify EnrichQuerySourceOperator
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);

        // Verify ProjectOperator - should just drop doc block (projection = [1] since no extract fields)
        ProjectOperator projectOp = (ProjectOperator) queryPlan.operators().get(0);
        verifyProjectOperator(projectOp, 0); // 0 extract fields

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    public void testEnrichLookupNoMergeWithExtractFields() throws IOException {
        // Test enrich lookup with mergePages=false but with extract fields (uses ProjectOperator, not MergePositionsOperator)
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            false
        );

        BytesRefBlock inputBlock = blockFactory.newBytesRefBlockBuilder(2)
            .appendBytesRef(new BytesRef("a"))
            .appendBytesRef(new BytesRef("b"))
            .build();
        Page inputPage = new Page(inputBlock);

        List<NamedExpression> extractFields = List.of(createFieldAttribute("field1", DataType.KEYWORD));
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> ProjectOperator -> OutputOperator
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, ProjectOperator.class, OutputOperator.class)
        );

        // Verify EnrichQuerySourceOperator
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);

        // Verify ValuesSourceReaderOperator
        ValuesSourceReaderOperator valuesOp = verifyValuesSourceReaderOperator(queryPlan, 0);

        // Verify ProjectOperator
        ProjectOperator projectOp = (ProjectOperator) queryPlan.operators().get(1);
        verifyProjectOperator(projectOp, extractFields.size());

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    public void testEnrichLookupMergeWithEmptyExtractFields() throws IOException {
        // Test enrich lookup with mergePages=true but empty extract fields
        // This is an edge case: when mergingChannels is empty, we should use dropDocBlockOperator instead of MergePositionsOperator
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            true
        );

        BytesRefBlock inputBlock = blockFactory.newBytesRefBlockBuilder(2)
            .appendBytesRef(new BytesRef("a"))
            .appendBytesRef(new BytesRef("b"))
            .build();
        Page inputPage = new Page(inputBlock);

        // Empty extract fields - but mergePages=true
        List<NamedExpression> extractFields = Collections.emptyList();
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ProjectOperator -> OutputOperator
        // Even though mergePages=true, empty mergingChannels means we use dropDocBlockOperator (ProjectOperator) instead of
        // MergePositionsOperator
        verifyCompletePlan(queryPlan, List.of(EnrichQuerySourceOperator.class, ProjectOperator.class, OutputOperator.class));

        // Verify EnrichQuerySourceOperator
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);

        // Verify ProjectOperator - should drop doc block (projection = [1] since no extract fields)
        ProjectOperator projectOp = (ProjectOperator) queryPlan.operators().get(0);
        verifyProjectOperator(projectOp, 0); // 0 extract fields

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    public void testEnrichLookupWithEmptyInputPage() throws IOException {
        // Test enrich lookup with empty input page (0 positions) - edge case
        // This verifies that the mapper handles empty input correctly, especially for range block creation
        LookupExecutionMapper mapper = new LookupExecutionMapper(
            blockFactory,
            bigArrays,
            new LocalCircuitBreaker.SizeSettings(Settings.EMPTY),
            true
        );

        // Create empty input page (0 positions)
        BytesRefBlock inputBlock = blockFactory.newBytesRefBlockBuilder(0).build();
        Page inputPage = new Page(inputBlock);

        List<NamedExpression> extractFields = List.of(createFieldAttribute("field1", DataType.KEYWORD));
        PhysicalPlan plan = createLookupPhysicalPlan(extractFields);

        AbstractLookupService.TransportRequest request = createMockRequest(inputPage, extractFields);
        AbstractLookupService.LookupShardContext shardContext = createMockShardContext();

        AbstractLookupService.LookupQueryPlan queryPlan = mapper.map(request, plan, shardContext, releasables);
        MatcherAssert.assertThat(queryPlan, notNullValue());

        // Verify complete plan structure
        // Expected: EnrichQuerySourceOperator -> ValuesSourceReaderOperator -> MergePositionsOperator -> OutputOperator
        // Even with empty input, the plan structure should be correct
        verifyCompletePlan(
            queryPlan,
            List.of(EnrichQuerySourceOperator.class, ValuesSourceReaderOperator.class, MergePositionsOperator.class, OutputOperator.class)
        );

        // Verify EnrichQuerySourceOperator
        EnrichQuerySourceOperator sourceOp = verifyEnrichQuerySourceOperator(queryPlan);
        verifyNoPageOptimization(sourceOp, inputBlock);

        // Verify ValuesSourceReaderOperator
        ValuesSourceReaderOperator valuesOp = verifyValuesSourceReaderOperator(queryPlan, 0);

        // Verify MergePositionsOperator with empty range block (0 positions)
        MergePositionsOperator mergeOp = (MergePositionsOperator) queryPlan.operators().get(1);
        verifyMergePositionsOperator(mergeOp, 0, true); // 0 positions, range block

        // Verify OutputOperator
        verifyOutputOperator(queryPlan);
    }

    // Verification helper methods

    /**
     * Verifies the complete plan structure with all operators.
     * @param queryPlan The query plan to verify
     * @param expectedOperators List of expected operator types in order: [source, intermediate1, intermediate2, ..., output]
     */
    private void verifyCompletePlan(AbstractLookupService.LookupQueryPlan queryPlan, List<Class<? extends Operator>> expectedOperators) {
        if (expectedOperators.size() < 2) {
            throw new IllegalArgumentException("Expected operators list must have at least 2 elements (source and output)");
        }

        // Verify source operator (first in list)
        MatcherAssert.assertThat("Source operator type mismatch", queryPlan.queryOperator(), instanceOf(expectedOperators.get(0)));

        // Verify intermediate operators (middle of list)
        List<Operator> actualOperators = queryPlan.operators();
        int expectedIntermediateCount = expectedOperators.size() - 2; // Exclude source and output
        MatcherAssert.assertThat("Intermediate operators count mismatch", actualOperators.size(), is(expectedIntermediateCount));

        for (int i = 0; i < expectedIntermediateCount; i++) {
            Class<? extends Operator> expectedType = expectedOperators.get(i + 1); // +1 to skip source
            MatcherAssert.assertThat(
                "Operator at index " + i + " should be " + expectedType.getSimpleName(),
                actualOperators.get(i),
                instanceOf(expectedType)
            );
        }

        // Verify output operator (last in list)
        MatcherAssert.assertThat(
            "Output operator type mismatch",
            queryPlan.outputOperator(),
            instanceOf(expectedOperators.get(expectedOperators.size() - 1))
        );
    }

    /**
     * Verifies EnrichQuerySourceOperator and returns it.
     */
    private EnrichQuerySourceOperator verifyEnrichQuerySourceOperator(AbstractLookupService.LookupQueryPlan queryPlan) {
        EnrichQuerySourceOperator sourceOp = queryPlan.queryOperator();
        MatcherAssert.assertThat(sourceOp, notNullValue());
        MatcherAssert.assertThat(sourceOp, instanceOf(EnrichQuerySourceOperator.class));
        Page inputPage = sourceOp.getInputPage();
        MatcherAssert.assertThat(inputPage, notNullValue());
        return sourceOp;
    }

    /**
     * Verifies ValuesSourceReaderOperator at the specified index.
     */
    private ValuesSourceReaderOperator verifyValuesSourceReaderOperator(AbstractLookupService.LookupQueryPlan queryPlan, int index) {
        Operator operator = queryPlan.operators().get(index);
        MatcherAssert.assertThat(operator, instanceOf(ValuesSourceReaderOperator.class));
        return (ValuesSourceReaderOperator) operator;
    }

    /**
     * Verifies ProjectOperator projection.
     */
    private void verifyProjectOperator(ProjectOperator projectOp, int extractFieldsCount) {
        MatcherAssert.assertThat(projectOp, notNullValue());
        int[] projection = projectOp.getProjection();
        MatcherAssert.assertThat("Projection should have extractFields.size() + 1 elements", projection.length, is(extractFieldsCount + 1));
        // Verify projection starts at 1 (skipping doc block at index 0) and contains sequential values
        for (int i = 0; i < projection.length; i++) {
            MatcherAssert.assertThat("Projection should contain sequential values starting from 1", projection[i], is(i + 1));
        }
    }

    /**
     * Verifies MergePositionsOperator with range block or dictionary ordinals.
     */
    private void verifyMergePositionsOperator(MergePositionsOperator mergeOp, int expectedPositionCount, boolean isRangeBlock) {
        MatcherAssert.assertThat(mergeOp, notNullValue());
        IntBlock selectedPositions = mergeOp.getSelectedPositions();
        MatcherAssert.assertThat(selectedPositions, notNullValue());
        MatcherAssert.assertThat(
            "Selected positions should have expected position count",
            selectedPositions.getPositionCount(),
            is(expectedPositionCount)
        );

        if (isRangeBlock) {
            // Verify it's a range block (vector with sequential values 0, 1, 2, ...)
            IntVector selectedPositionsVector = selectedPositions.asVector();
            MatcherAssert.assertThat("Selected positions should be a vector (range block)", selectedPositionsVector, notNullValue());
            for (int i = 0; i < expectedPositionCount; i++) {
                MatcherAssert.assertThat("Range block position " + i + " should be " + i, selectedPositionsVector.getInt(i), is(i));
            }
        } else {
            // Dictionary ordinals case - just verify it exists and has correct count
            // The actual values come from the dictionary ordinals
        }
    }

    /**
     * Verifies dictionary optimization was applied to the input page.
     */
    private void verifyDictionaryOptimization(EnrichQuerySourceOperator sourceOp, int expectedDictionarySize) {
        Page optimizedPage = sourceOp.getInputPage();
        Block optimizedBlock = optimizedPage.getBlock(0);
        MatcherAssert.assertThat(
            "Optimized block should be a BytesRefBlock (dictionary block)",
            optimizedBlock,
            instanceOf(BytesRefBlock.class)
        );
        MatcherAssert.assertThat(
            "Dictionary block should have dictionary size positions",
            optimizedBlock.getPositionCount(),
            is(expectedDictionarySize)
        );

        // Verify the dictionary values are present
        BytesRefBlock dictBlock = (BytesRefBlock) optimizedBlock;
        BytesRef value1 = new BytesRef();
        BytesRef value2 = new BytesRef();
        dictBlock.getBytesRef(0, value1);
        dictBlock.getBytesRef(1, value2);
        // The dictionary should contain "a" and "b" (order may vary)
        MatcherAssert.assertThat(
            "Dictionary block should contain expected values",
            (value1.utf8ToString().equals("a") && value2.utf8ToString().equals("b"))
                || (value1.utf8ToString().equals("b") && value2.utf8ToString().equals("a")),
            is(true)
        );
    }

    /**
     * Verifies that no page optimization was applied (input page is original).
     */
    private void verifyNoPageOptimization(EnrichQuerySourceOperator sourceOp, Block expectedInputBlock) {
        Page sourceInputPage = sourceOp.getInputPage();
        MatcherAssert.assertThat(
            "Input page should be the original page when no optimization is used",
            sourceInputPage.getBlock(0),
            is(expectedInputBlock)
        );
    }

    /**
     * Verifies OutputOperator.
     */
    private void verifyOutputOperator(AbstractLookupService.LookupQueryPlan queryPlan) {
        OutputOperator outputOp = queryPlan.outputOperator();
        MatcherAssert.assertThat(outputOp, notNullValue());
        MatcherAssert.assertThat(outputOp, instanceOf(OutputOperator.class));
    }

    // Helper methods

    private AbstractLookupService.TransportRequest createMockRequest(Page inputPage, List<NamedExpression> extractFields) {
        return new LookupFromIndexService.TransportRequest(
            "test-session",
            new ShardId("test", "n/a", 0),
            "test-index",
            inputPage,
            null,
            extractFields,
            Collections.emptyList(),
            Source.EMPTY,
            null,
            null
        );
    }

    private AbstractLookupService.LookupShardContext createMockShardContext() throws IOException {
        MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
        };
        SearchExecutionContext executionCtx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(testReader));
        EsPhysicalOperationProviders.DefaultShardContext shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            executionCtx,
            AliasFilter.EMPTY
        );
        return new AbstractLookupService.LookupShardContext(
            shardContext,
            executionCtx,
            () -> {} // No-op releasable
        );
    }

    private FieldAttribute createFieldAttribute(String name, DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, type, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
