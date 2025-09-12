/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AsyncOperatorTestCase;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Mockito.mock;

public class LookupFromIndexOperatorTests extends AsyncOperatorTestCase {
    private static final int LOOKUP_SIZE = 1000;
    private static final int LESS_THAN_VALUE = -40;
    private final ThreadPool threadPool = threadPool();
    private final Directory lookupIndexDirectory = newDirectory();
    private final List<Releasable> releasables = new ArrayList<>();
    private int numberOfJoinColumns; // we only allow 1 or 2 columns due to simpleInput() implementation
    private EsqlBinaryComparison.BinaryComparisonOperation operation;

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> operations = new ArrayList<>();
        operations.add(new Object[] { null });
        for (EsqlBinaryComparison.BinaryComparisonOperation operation : EsqlBinaryComparison.BinaryComparisonOperation.values()) {
            operations.add(new Object[] { operation });
        }
        return operations;
    }

    public LookupFromIndexOperatorTests(EsqlBinaryComparison.BinaryComparisonOperation operation) {
        super();
        this.operation = operation;
    }

    @Before
    public void buildLookupIndex() throws IOException {
        numberOfJoinColumns = 1 + randomInt(1); // 1 or 2 join columns
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), lookupIndexDirectory)) {
            String suffix = (operation == null) ? "" : ("_" + "right");
            for (int i = 0; i < LOOKUP_SIZE; i++) {
                List<IndexableField> fields = new ArrayList<>();
                fields.add(new LongField("match0" + suffix, i, Field.Store.NO));
                if (numberOfJoinColumns == 2) {
                    fields.add(new LongField("match1" + suffix, i + 1, Field.Store.NO));
                }
                fields.add(new KeywordFieldMapper.KeywordField("lkwd", new BytesRef("l" + i), KeywordFieldMapper.Defaults.FIELD_TYPE));
                fields.add(new IntField("lint", -i, Field.Store.NO));
                writer.addDocument(fields);
            }
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        if (numberOfJoinColumns == 1) {
            return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> l % LOOKUP_SIZE));
        } else if (numberOfJoinColumns == 2) {
            return new TupleLongLongBlockSourceOperator(
                blockFactory,
                LongStream.range(0, size).mapToObj(l -> Tuple.tuple(l % LOOKUP_SIZE, l % LOOKUP_SIZE + 1))
            );
        } else {
            throw new IllegalStateException("numberOfJoinColumns must be 1 or 2, got: " + numberOfJoinColumns);
        }
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        if (operation == EsqlBinaryComparison.BinaryComparisonOperation.NEQ) {
            // if not equal there are too many matches and we get OOMEs when validating the output
            return;
        }
        /*
         * We've configured there to be just a single result per input so the total
         * row count is the same. But lookup cuts into pages of length 256 so the
         * result is going to have more pages usually.
         */
        int filterStart = Math.abs(LESS_THAN_VALUE) + 1; // i > 40 for LESS_THAN_VALUE = -40
        List<Long> expectedInputValues = new ArrayList<>();
        List<Long> expectedMatchedIndices = new ArrayList<>();
        List<Boolean> expectedNulls = new ArrayList<>();
        if (operation == null || operation == EsqlBinaryComparison.BinaryComparisonOperation.EQ) {
            for (Page page : input) {
                LongVector left0Vector = (LongVector) page.getBlock(0).asVector();
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    long left0 = left0Vector.getLong(pos);
                    if (left0 > Math.abs(LESS_THAN_VALUE)) {
                        expectedInputValues.add(left0);
                        expectedMatchedIndices.add(left0);
                        expectedNulls.add(false);
                    } else {
                        expectedInputValues.add(left0);
                        expectedMatchedIndices.add(null);
                        expectedNulls.add(true);
                    }
                }
            }
        } else {
            for (Page page : input) {
                LongVector left0Vector = (LongVector) page.getBlock(0).asVector();
                LongVector left1Vector = null;
                if (numberOfJoinColumns == 2) {
                    left1Vector = (LongVector) page.getBlock(1).asVector();
                }
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    long left0 = left0Vector.getLong(pos);
                    long left1 = (numberOfJoinColumns == 2) ? left1Vector.getLong(pos) : 0;
                    boolean hasMatch = false;
                    for (int i = filterStart; i < LOOKUP_SIZE; i++) {
                        boolean match = true;
                        if (numberOfJoinColumns >= 1) {
                            match &= compare(left0, i, operation);
                        }
                        if (numberOfJoinColumns == 2) {
                            match &= compare(left1, i + 1, operation);
                        }
                        if (match) {
                            expectedInputValues.add(left0);
                            expectedMatchedIndices.add((long) i);
                            expectedNulls.add(false);
                            hasMatch = true;
                        }
                    }
                    if (hasMatch == false) {
                        // No matches for this input, expect a null output row
                        expectedInputValues.add(left0);
                        expectedMatchedIndices.add(null);
                        expectedNulls.add(true);
                    }
                }
            }
        }
        // Now check actual output matches expected
        int outputIdx = 0;
        for (Page r : results) {
            assertThat(r.getBlockCount(), equalTo(numberOfJoinColumns + 2));
            LongVector match = r.<LongBlock>getBlock(0).asVector();
            BytesRefBlock lkwdBlock = r.getBlock(numberOfJoinColumns);
            IntBlock lintBlock = r.getBlock(numberOfJoinColumns + 1);
            for (int p = 0; p < r.getPositionCount(); p++, outputIdx++) {
                Long expectedMatched = expectedMatchedIndices.get(outputIdx);
                boolean expectNull = expectedNulls.get(outputIdx);
                if (expectNull) {
                    assertTrue("at " + p, lkwdBlock.isNull(p));
                    assertTrue("at " + p, lintBlock.isNull(p));
                } else {
                    assertFalse("at " + p, lkwdBlock.isNull(p));
                    assertFalse("at " + p, lintBlock.isNull(p));
                    assertThat(
                        lkwdBlock.getBytesRef(lkwdBlock.getFirstValueIndex(p), new BytesRef()).utf8ToString(),
                        equalTo("l" + expectedMatched)
                    );
                    assertThat(lintBlock.getInt(lintBlock.getFirstValueIndex(p)), equalTo((int) -expectedMatched.longValue()));
                }
            }
        }
    }

    private boolean compare(long left, long right, EsqlBinaryComparison.BinaryComparisonOperation op) {
        // Use the operator's fold method for comparison
        Literal leftLiteral = new Literal(Source.EMPTY, left, DataType.LONG);
        Literal rightLiteral = new Literal(Source.EMPTY, right, DataType.LONG);
        EsqlBinaryComparison operatorInstance = op.buildNewInstance(Source.EMPTY, leftLiteral, rightLiteral);
        Object result = operatorInstance.fold(FoldContext.small());
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        throw new IllegalArgumentException("Operator fold did not return a boolean");
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        String sessionId = "test";
        CancellableTask parentTask = new CancellableTask(0, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of());
        int maxOutstandingRequests = 1;
        DataType inputDataType = DataType.LONG;
        String lookupIndex = "idx";
        List<NamedExpression> loadFields = List.of(
            new ReferenceAttribute(Source.EMPTY, "lkwd", DataType.KEYWORD),
            new ReferenceAttribute(Source.EMPTY, "lint", DataType.INTEGER)
        );

        List<MatchConfig> matchFields = new ArrayList<>();
        String suffix = (operation == null) ? "" : ("_left");
        for (int i = 0; i < numberOfJoinColumns; i++) {
            String matchField = "match" + i + suffix;
            matchFields.add(new MatchConfig(matchField, i, inputDataType));
        }
        Expression joinOnExpression = null;
        if (operation != null) {
            List<Expression> conditions = new ArrayList<>();
            for (int i = 0; i < numberOfJoinColumns; i++) {
                String matchFieldLeft = "match" + i + "_left";
                String matchFieldRight = "match" + i + "_right";
                FieldAttribute left = new FieldAttribute(
                    Source.EMPTY,
                    matchFieldLeft,
                    new EsField(matchFieldLeft, inputDataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                );
                FieldAttribute right = new FieldAttribute(
                    Source.EMPTY,
                    matchFieldRight,
                    new EsField(matchFieldRight.replace("left", "right"), inputDataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                );
                conditions.add(operation.buildNewInstance(Source.EMPTY, left, right));
            }
            joinOnExpression = Predicates.combineAnd(conditions);
        }

        return new LookupFromIndexOperator.Factory(
            matchFields,
            sessionId,
            parentTask,
            maxOutstandingRequests,
            this::lookupService,
            lookupIndex,
            lookupIndex,
            loadFields,
            Source.EMPTY,
            buildLessThanFilter(LESS_THAN_VALUE),
            joinOnExpression
        );
    }

    private FragmentExec buildLessThanFilter(int value) {
        FieldAttribute filterAttribute = new FieldAttribute(
            Source.EMPTY,
            "lint",
            new EsField("lint", DataType.INTEGER, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Expression lessThan = new LessThan(Source.EMPTY, filterAttribute, new Literal(Source.EMPTY, value, DataType.INTEGER));
        EsRelation esRelation = new EsRelation(Source.EMPTY, "test", IndexMode.LOOKUP, Map.of(), List.of());
        Filter filter = new Filter(Source.EMPTY, esRelation, lessThan);
        return new FragmentExec(filter);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    public void testSimpleDescription() {
        Operator.OperatorFactory factory = simple();
        String description = factory.describe();
        assertThat(description, expectedDescriptionOfSimple());
        try (Operator op = factory.get(driverContext())) {
            // we use a special pattern here because the description can contain new lines for the right_pre_join_plan
            String pattern = "^\\w*\\[[\\s\\S]*\\]$";
            assertThat(description, matchesPattern(pattern));
        }
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        StringBuilder sb = new StringBuilder();
        String suffix = (operation == null) ? "" : ("_left");
        sb.append("LookupOperator\\[index=idx load_fields=\\[lkwd\\{r}#\\d+, lint\\{r}#\\d+] ");
        for (int i = 0; i < numberOfJoinColumns; i++) {
            // match_field=match<i>_left (index first, then suffix)
            sb.append("input_type=LONG match_field=match").append(i).append(suffix).append(" inputChannel=").append(i).append(" ");
        }
        // Accept either the legacy physical plan rendering (FilterExec/EsQueryExec) or the new FragmentExec rendering
        sb.append("right_pre_join_plan=(?:");
        // Legacy pattern
        sb.append("FilterExec\\[lint\\{f}#\\d+ < ")
            .append(LESS_THAN_VALUE)
            .append(
                "\\[INTEGER]]\\n\\\\_EsQueryExec\\[test], indexMode\\[lookup],\\s*(?:query\\[\\]|\\[\\])?,?\\s*"
                    + "limit\\[\\],?\\s*sort\\[(?:\\[\\])?\\]\\s*estimatedRowSize\\[null\\]\\s*queryBuilderAndTags \\[(?:\\[\\]\\])\\]"
            );
        sb.append("|");
        // New FragmentExec pattern - match the actual output format
        sb.append("FragmentExec\\[filter=null, estimatedRowSize=\\d+, reducer=\\[\\], fragment=\\[<>\\n")
            .append("Filter\\[lint\\{f}#\\d+ < ")
            .append(LESS_THAN_VALUE)
            .append("\\[INTEGER]]\\n")
            .append("\\\\_EsRelation\\[test]\\[LOOKUP]\\[\\]<>\\]\\]");
        sb.append(")");
        // Accept join_on_expression=null or a valid join predicate
        sb.append(" join_on_expression=(null|match\\d+left [=!<>]+ match\\d+right( AND match\\d+left [=!<>]+ match\\d+right)*|)\\]");
        return matchesPattern(sb.toString());
    }

    private LookupFromIndexService lookupService(DriverContext mainContext) {
        boolean beCranky = mainContext.bigArrays().breakerService() instanceof CrankyCircuitBreakerService;
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node", "node");
        var builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            localNode,
            Settings.builder()
                // Reserve 0 bytes in the sub-driver so we are more likely to hit the cranky breaker in it.
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .build(),
            new ClusterSettings(Settings.EMPTY, builtInClusterSettings)
        );
        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
        releasables.add(clusterService::stop);
        final var projectId = randomProjectIdOrDefault();
        ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.state(projectId, "idx", 1, 1));
        if (beCranky) {
            logger.info("building a cranky lookup");
        }
        DriverContext ctx = beCranky ? crankyDriverContext() : driverContext();
        BigArrays bigArrays = ctx.bigArrays();
        BlockFactory blockFactory = ctx.blockFactory();
        return new LookupFromIndexService(
            clusterService,
            indicesService,
            lookupShardContextFactory(),
            transportService(clusterService),
            indexNameExpressionResolver,
            bigArrays,
            blockFactory,
            TestProjectResolvers.singleProject(projectId)
        );
    }

    private ThreadPool threadPool() {
        return new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
                1,
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    private TransportService transportService(ClusterService clusterService) {
        MockTransport mockTransport = new MockTransport();
        releasables.add(mockTransport);
        TransportService transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            clusterService.getClusterSettings(),
            Set.of()
        );
        releasables.add(transportService);
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }

    private AbstractLookupService.LookupShardContextFactory lookupShardContextFactory() {
        return shardId -> {
            MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
            };
            String suffix = (operation == null) ? "" : ("_right");
            StringBuilder props = new StringBuilder();
            props.append(String.format("\"match0%s\": { \"type\": \"long\" }", suffix));
            if (numberOfJoinColumns == 2) {
                props.append(String.format(", \"match1%s\": { \"type\": \"long\" }", suffix));
            }
            props.append(", \"lkwd\": { \"type\": \"keyword\" }, \"lint\": { \"type\": \"integer\" }");
            String mapping = String.format("{\n  \"doc\": { \"properties\": { %s } }\n}", props.toString());
            MapperService mapperService = mapperHelper.createMapperService(mapping);
            DirectoryReader reader = DirectoryReader.open(lookupIndexDirectory);
            SearchExecutionContext executionCtx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            var ctx = new EsPhysicalOperationProviders.DefaultShardContext(0, new NoOpReleasable(), executionCtx, AliasFilter.EMPTY);
            return new AbstractLookupService.LookupShardContext(ctx, executionCtx, () -> {
                try {
                    IOUtils.close(reader, mapperService);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        };
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(lookupIndexDirectory);
    }

    @After
    public void release() {
        Releasables.close(Releasables.wrap(releasables.reversed()), () -> terminate(threadPool));
    }

    @Override
    protected MapMatcher extendStatusMatcher(MapMatcher mapMatcher, List<Page> input, List<Page> output) {
        var totalInputRows = input.stream().mapToInt(Page::getPositionCount).sum();
        var totalOutputRows = output.stream().mapToInt(Page::getPositionCount).sum();

        return mapMatcher.entry("total_rows", totalInputRows).entry("pages_emitted", output.size()).entry("rows_emitted", totalOutputRows);
    }
}
