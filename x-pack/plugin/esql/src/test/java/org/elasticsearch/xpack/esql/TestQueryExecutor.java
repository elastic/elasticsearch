/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.Type;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestLocalPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.TestPhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.TestPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.DisabledSearchStats;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;

/**
 * Executes a query without creating a full ES instance. To account for this, uses specialized {@link TestPhysicalOperationProviders} and
 * a{@link TestPhysicalPlanOptimizer}.
 *
 * When we add more operators, optimization rules to the logical or physical plan optimizers, there may be the need to change the operators
 * in TestPhysicalOperationProviders or adjust TestPhysicalPlanOptimizer. For example, the TestPhysicalPlanOptimizer is skipping any
 * rules that push operations to ES itself (a Limit for example). The TestPhysicalOperationProviders is a bit more complicated than that:
 * it’s creating its own Source physical operator, aggregation operator (just a tiny bit of it) and field extract operator.
 */
public class TestQueryExecutor {
    private static final Logger LOGGER = LogManager.getLogger(TestQueryExecutor.class);
    private final EsqlConfiguration configuration = EsqlTestUtils.configuration(
        new QueryPragmas(Settings.builder().put("page_size", randomPageSize()).build())
    );
    private final FunctionRegistry functionRegistry = new EsqlFunctionRegistry();
    private final Mapper mapper = new Mapper(functionRegistry);
    private final PhysicalPlanOptimizer physicalPlanOptimizer = new TestPhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
    private final ThreadPool threadPool;
    BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();

    public TestQueryExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    private int randomPageSize() {
        if (ESTestCase.randomBoolean()) {
            return ESTestCase.between(1, 16);
        } else {
            return ESTestCase.between(1, 16 * 1024);
        }
    }

    private PhysicalPlan physicalPlan(LogicalPlan parsed, IndexResolution indexResolution, EnrichResolution enrichResolution) {
        var analyzer = new Analyzer(new AnalyzerContext(configuration, functionRegistry, indexResolution, enrichResolution), TEST_VERIFIER);
        var analyzed = analyzer.analyze(parsed);
        var logicalOptimized = new LogicalPlanOptimizer(new LogicalOptimizerContext(configuration)).optimize(analyzed);
        var physicalPlan = mapper.map(logicalOptimized);
        var optimizedPlan = EstimatesRowSize.estimateRowSize(0, physicalPlanOptimizer.optimize(physicalPlan));
        opportunisticallyAssertPlanSerialization(physicalPlan, optimizedPlan); // comment out to disable serialization
        return optimizedPlan;
    }

    public ActualResults executePlan(
        LogicalPlan parsedQuery,
        AbstractPhysicalOperationProviders testOperationProviders,
        IndexResolution indexResolution,
        EnrichResolution enrichResolution,
        boolean useEsPhysicalOptimizations
    ) {
        String sessionId = "csv-test";
        ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(
            ESTestCase.between(1, 64),
            threadPool.executor(ESQL_THREAD_POOL_NAME)
        );
        ExchangeSinkHandler exchangeSink = new ExchangeSinkHandler(ESTestCase.between(1, 64), threadPool::relativeTimeInMillis);

        BlockFactory blockFactory = new BlockFactory(
            bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST),
            bigArrays,
            ByteSizeValue.ofBytes(ESTestCase.randomLongBetween(1, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes() * 2))
        );
        LocalExecutionPlanner executionPlanner = new LocalExecutionPlanner(
            sessionId,
            new CancellableTask(1, "transport", "esql", null, TaskId.EMPTY_TASK_ID, Map.of()),
            bigArrays,
            blockFactory,
            randomNodeSettings(),
            configuration,
            exchangeSource,
            exchangeSink,
            Mockito.mock(EnrichLookupService.class),
            testOperationProviders
        );
        //
        // Keep in sync with ComputeService#execute
        //
        PhysicalPlan physicalPlan = physicalPlan(parsedQuery, indexResolution, enrichResolution);
        Tuple<PhysicalPlan, PhysicalPlan> coordinatorAndDataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(
            physicalPlan,
            configuration
        );
        PhysicalPlan coordinatorPlan = coordinatorAndDataNodePlan.v1();
        PhysicalPlan dataNodePlan = coordinatorAndDataNodePlan.v2();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Coordinator plan\n" + coordinatorPlan);
            LOGGER.trace("DataNode plan\n" + dataNodePlan);
        }

        List<String> columnNames = Expressions.names(coordinatorPlan.output());
        List<String> dataTypes = new ArrayList<>(columnNames.size());
        List<Type> columnTypes = coordinatorPlan.output()
            .stream()
            .peek(o -> dataTypes.add(EsqlDataTypes.outputType(o.dataType())))
            .map(o -> Type.asType(o.dataType().name()))
            .toList();

        List<Driver> drivers = new ArrayList<>();
        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        // replace fragment inside the coordinator plan
        try {
            LocalExecutionPlan coordinatorNodeExecutionPlan = executionPlanner.plan(new OutputExec(coordinatorPlan, collectedPages::add));
            drivers.addAll(coordinatorNodeExecutionPlan.createDrivers(sessionId));
            if (dataNodePlan != null) {
                var searchStats = new DisabledSearchStats();
                var logicalTestOptimizer = new LocalLogicalPlanOptimizer(new LocalLogicalOptimizerContext(configuration, searchStats));
                var localPhysicalOptimizerCtx = new LocalPhysicalOptimizerContext(configuration, searchStats);
                var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(localPhysicalOptimizerCtx, useEsPhysicalOptimizations);

                var csvDataNodePhysicalPlan = PlannerUtils.localPlan(dataNodePlan, logicalTestOptimizer, physicalTestOptimizer);
                exchangeSource.addRemoteSink(exchangeSink::fetchPageAsync, ESTestCase.randomIntBetween(1, 3));
                LocalExecutionPlan dataNodeExecutionPlan = executionPlanner.plan(csvDataNodePhysicalPlan);
                drivers.addAll(dataNodeExecutionPlan.createDrivers(sessionId));
                Randomness.shuffle(drivers);
            }
            // Execute the driver
            DriverRunner runner = new DriverRunner(threadPool.getThreadContext()) {
                @Override
                protected void start(Driver driver, ActionListener<Void> driverListener) {
                    Driver.start(
                        threadPool.getThreadContext(),
                        threadPool.executor(ESQL_THREAD_POOL_NAME),
                        driver,
                        ESTestCase.between(1, 1000),
                        driverListener
                    );
                }
            };
            PlainActionFuture<ActualResults> future = new PlainActionFuture<>();
            runner.runToCompletion(drivers, ActionListener.releaseAfter(future, () -> Releasables.close(drivers)).map(ignore -> {
                var responseHeaders = threadPool.getThreadContext().getResponseHeaders();
                return new ActualResults(columnNames, columnTypes, dataTypes, collectedPages, responseHeaders);
            }));
            return future.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            Releasables.close(() -> Releasables.close(drivers));
        }
    }

    public ActualResults executePlan(
        LogicalPlan parsedQuery,
        AbstractPhysicalOperationProviders testOperationProviders,
        IndexResolution indexResolution,
        EnrichResolution enrichResolution
    ) {
        return executePlan(parsedQuery, testOperationProviders, indexResolution, enrichResolution, false);
    }

    public CircuitBreakerService breakerService() {
        return bigArrays.breakerService();
    }

    private Settings randomNodeSettings() {
        Settings.Builder builder = Settings.builder();
        if (ESTestCase.randomBoolean()) {
            builder.put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, ByteSizeValue.ofBytes(ESTestCase.randomIntBetween(0, 4096)));
            builder.put(
                BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING,
                ByteSizeValue.ofBytes(ESTestCase.randomIntBetween(0, 16 * 1024))
            );
        }
        return builder.build();
    }

    // Asserts that the serialization and deserialization of the plan creates an equivalent plan.
    private static void opportunisticallyAssertPlanSerialization(PhysicalPlan... plans) {
        for (var plan : plans) {
            var tmp = plan;
            do {
                if (tmp instanceof LocalSourceExec) {
                    return; // skip plans with localSourceExec
                }
            } while (tmp.children().isEmpty() == false && (tmp = tmp.children().get(0)) != null);

            SerializationTestUtils.assertSerialization(plan);
        }
    }
}
