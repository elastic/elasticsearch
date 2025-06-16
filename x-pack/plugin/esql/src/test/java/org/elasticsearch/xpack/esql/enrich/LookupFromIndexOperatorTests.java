/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Mockito.mock;

public class LookupFromIndexOperatorTests extends OperatorTestCase {
    private static final int LOOKUP_SIZE = 1000;
    private final ThreadPool threadPool = threadPool();
    private final Directory lookupIndexDirectory = newDirectory();
    private final List<Releasable> releasables = new ArrayList<>();

    @Before
    public void buildLookupIndex() throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), lookupIndexDirectory)) {
            for (int i = 0; i < LOOKUP_SIZE; i++) {
                writer.addDocument(
                    List.of(
                        new LongField("match", i, Field.Store.NO),
                        new KeywordFieldMapper.KeywordField("lkwd", new BytesRef("l" + i), KeywordFieldMapper.Defaults.FIELD_TYPE),
                        new IntField("lint", -i, Field.Store.NO)
                    )
                );
            }
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> l % LOOKUP_SIZE));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        /*
         * We've configured there to be just a single result per input so the total
         * row count is the same. But lookup cuts into pages of length 256 so the
         * result is going to have more pages usually.
         */
        int count = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(count, equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
        for (Page r : results) {
            assertThat(r.getBlockCount(), equalTo(3));
            LongVector match = r.<LongBlock>getBlock(0).asVector();
            BytesRefVector lkwd = r.<BytesRefBlock>getBlock(1).asVector();
            IntVector lint = r.<IntBlock>getBlock(2).asVector();
            for (int p = 0; p < r.getPositionCount(); p++) {
                long m = match.getLong(p);
                assertThat(lkwd.getBytesRef(p, new BytesRef()).utf8ToString(), equalTo("l" + m));
                assertThat(lint.getInt(p), equalTo((int) -m));
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple() {
        String sessionId = "test";
        CancellableTask parentTask = new CancellableTask(0, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of());
        int maxOutstandingRequests = 1;
        int inputChannel = 0;
        DataType inputDataType = DataType.LONG;
        String lookupIndex = "idx";
        String matchField = "match";
        List<NamedExpression> loadFields = List.of(
            new ReferenceAttribute(Source.EMPTY, "lkwd", DataType.KEYWORD),
            new ReferenceAttribute(Source.EMPTY, "lint", DataType.INTEGER)
        );
        return new LookupFromIndexOperator.Factory(
            sessionId,
            parentTask,
            maxOutstandingRequests,
            inputChannel,
            this::lookupService,
            inputDataType,
            lookupIndex,
            lookupIndex,
            matchField,
            loadFields,
            Source.EMPTY
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesPattern(
            "LookupOperator\\[index=idx input_type=LONG match_field=match load_fields=\\[lkwd\\{r}#\\d+, lint\\{r}#\\d+] inputChannel=0]"
        );
    }

    private LookupFromIndexService lookupService(DriverContext mainContext) {
        boolean beCranky = mainContext.bigArrays().breakerService() instanceof CrankyCircuitBreakerService;
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node", "node");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            localNode,
            Settings.builder()
                // Reserve 0 bytes in the sub-driver so we are more likely to hit the cranky breaker in it.
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, ByteSizeValue.ofKb(0))
                .build(),
            ClusterSettings.createBuiltInClusterSettings()
        );
        IndicesService indicesService = mock(IndicesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
        releasables.add(clusterService::stop);
        ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.state("idx", 1, 1));
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
            blockFactory
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
            MapperService mapperService = mapperHelper.createMapperService("""
                {
                    "doc": { "properties": {
                        "match": { "type": "long" },
                        "lkwd": { "type": "keyword" },
                        "lint": { "type": "integer" }
                    }}
                }""");
            DirectoryReader reader = DirectoryReader.open(lookupIndexDirectory);
            SearchExecutionContext executionCtx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            EsPhysicalOperationProviders.DefaultShardContext ctx = new EsPhysicalOperationProviders.DefaultShardContext(
                0,
                executionCtx,
                AliasFilter.EMPTY
            );
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
        Collections.reverse(releasables);
        Releasables.close(Releasables.wrap(releasables), () -> terminate(threadPool));
    }

    @Override
    public void testOperatorStatus() {
        assumeFalse("not yet standardized", true);
    }
}
