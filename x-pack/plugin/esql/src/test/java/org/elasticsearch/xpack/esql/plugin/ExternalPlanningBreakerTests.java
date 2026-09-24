/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase-2 planning reservations: local discovery charges resolved file lists, fragment discovery charges the
 * relation file count, and a warm skip charges nothing. Release returns the same bytes to the breaker.
 */
public class ExternalPlanningBreakerTests extends ESTestCase {

    private static final long PHASE2_BYTES_PER_FILE = 1160L;

    public void testDiscoverSplitsChargesResolvedFilesBeforeDiscovery() {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        ExternalSourceExec exec = relation(resolvedFiles(2), Map.of()).toPhysicalExec();

        service.discoverSplits(exec, configuration(), info, () -> false);

        assertEquals(1, discoveries.get());
        assertEquals(baseline + 2 * PHASE2_BYTES_PER_FILE, breaker.getUsed());
        assertEquals(2 * PHASE2_BYTES_PER_FILE, info.planningBytes().get());
    }

    public void testDiscoverSplitsTripsBeforeDiscoveryAndLeavesLedgerAtZero() {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1b");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        ExternalSourceExec exec = relation(resolvedFiles(2), Map.of()).toPhysicalExec();

        CircuitBreakingException broke = expectThrows(
            CircuitBreakingException.class,
            () -> service.discoverSplits(exec, configuration(), info, () -> false)
        );

        assertThat(broke.getMessage(), containsString(EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL));
        assertEquals(0, discoveries.get());
        assertEquals(0L, info.planningBytes().get());
        assertEquals(baseline, breaker.getUsed());
    }

    public void testFragmentWorkChargesRelationFileCount() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        FragmentExec fragment = new FragmentExec(relation(resolvedFiles(3), Map.of()));
        assertFalse(ComputeService.canSkipSplitDiscovery(fragment, service.formatReaderRegistry()));
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(fragment, configuration(), info, () -> false, done);

        done.actionGet(30, TimeUnit.SECONDS);
        assertEquals(1, discoveries.get());
        assertEquals(baseline + 3 * PHASE2_BYTES_PER_FILE, breaker.getUsed());
        assertEquals(3 * PHASE2_BYTES_PER_FILE, info.planningBytes().get());
    }

    public void testSkipDiscoveryDoesNotCharge() throws Exception {
        AtomicInteger discoveries = new AtomicInteger();
        CircuitBreaker breaker = requestBreaker("1gb");
        long baseline = breaker.getUsed();
        ComputeService service = service(breaker, discoveries);
        EsqlExecutionInfo info = executionInfo();
        Map<String, Object> stats = new HashMap<>();
        stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 99_000L);
        ExternalRelation external = relation(resolvedFiles(4), stats);
        Alias countAlias = alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        FragmentExec fragment = new FragmentExec(new Aggregate(Source.EMPTY, external, List.of(), List.of(countAlias)));
        assertTrue(ComputeService.canSkipSplitDiscovery(fragment, service.formatReaderRegistry()));
        PlainActionFuture<ComputeService.CollectedSplits> done = new PlainActionFuture<>();

        service.startPhase2OrSkip(fragment, configuration(), info, () -> false, done);

        done.actionGet(30, TimeUnit.SECONDS);
        assertEquals(0, discoveries.get());
        assertEquals(baseline, breaker.getUsed());
        assertEquals(0L, info.planningBytes().get());
    }

    public void testReleaseReturnsSuccessAndFailureToBaseline() {
        CircuitBreaker breaker = requestBreaker("1mb");
        long baseline = breaker.getUsed();

        EsqlExecutionInfo success = executionInfo();
        success.planningBytes().addAndGet(400);
        breaker.addEstimateBytesAndMaybeBreak(400, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
        TransportEsqlQueryAction.releaseExternalPlanningBytes(success, breaker);
        assertEquals(baseline, breaker.getUsed());
        assertEquals(0L, success.planningBytes().get());

        EsqlExecutionInfo failure = executionInfo();
        failure.planningBytes().addAndGet(250);
        breaker.addEstimateBytesAndMaybeBreak(250, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
        TransportEsqlQueryAction.releaseExternalPlanningBytes(failure, breaker);
        assertEquals(baseline, breaker.getUsed());
        assertEquals(0L, failure.planningBytes().get());

        TransportEsqlQueryAction.releaseExternalPlanningBytes(failure, breaker);
        assertEquals(baseline, breaker.getUsed());
    }

    private static ComputeService service(CircuitBreaker breaker, AtomicInteger discoveries) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(
            mock(org.elasticsearch.threadpool.Scheduler.Cancellable.class)
        );

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.add(EsqlPlugin.GROK_WATCHDOG_MAX_EXECUTION_TIME);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, registered);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        ExchangeService exchangeService = new ExchangeService(Settings.EMPTY, threadPool, ThreadPool.Names.SEARCH, blockFactory);
        FormatReaderRegistry readers = parquetRegistry();
        SplitProvider splitter = ctx -> {
            discoveries.incrementAndGet();
            return SplitDiscoveryResult.EMPTY;
        };
        ExternalSourceFactory files = new ExternalSourceFactory() {
            @Override
            public String type() {
                return "parquet";
            }

            @Override
            public boolean canHandle(String location) {
                return false;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                return null;
            }

            @Override
            public void validateConfig(String location, Map<String, Object> config) {}

            @Override
            public SplitProvider splitProvider() {
                return splitter;
            }
        };
        OperatorFactoryRegistry operators = new OperatorFactoryRegistry(
            Map.of("parquet", files),
            Map.of(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        TransportActionServices transport = new TransportActionServices(
            transportService,
            null,
            exchangeService,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            mock(PlannerSettings.Holder.class),
            null
        );
        return new ComputeService(transport, null, null, threadPool, BigArrays.NON_RECYCLING_INSTANCE, blockFactory, operators, readers);
    }

    private static FormatReaderRegistry parquetRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        AggregatePushdownSupport support = (aggregates, groupings) -> {
            if (groupings.isEmpty() == false) {
                return AggregatePushdownSupport.Pushability.NO;
            }
            for (Expression agg : aggregates) {
                if (agg instanceof Count == false) {
                    return AggregatePushdownSupport.Pushability.NO;
                }
            }
            return AggregatePushdownSupport.Pushability.YES;
        };
        registry.registerLazy("parquet", (settings, blockFactory) -> new CountingReader(support), null, null);
        return registry;
    }

    private static ExternalRelation relation(FileList files, Map<String, Object> stats) {
        List<Attribute> attrs = List.of(referenceAttribute("x", DataType.INTEGER));
        SourceMetadata metadata = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return attrs;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "file:///data/*.parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return stats;
            }
        };
        return new ExternalRelation(Source.EMPTY, "file:///data/*.parquet", metadata, attrs, files, Map.of());
    }

    private static FileList resolvedFiles(int count) {
        return new FileList() {
            @Override
            public int fileCount() {
                return count;
            }

            @Override
            public StoragePath path(int i) {
                return StoragePath.of("file:///f" + i + ".parquet");
            }

            @Override
            public long size(int i) {
                return 1L;
            }

            @Override
            public long lastModifiedMillis(int i) {
                return 0L;
            }

            @Override
            public String originalPattern() {
                return "file:///*.parquet";
            }

            @Override
            public org.elasticsearch.xpack.esql.datasources.PartitionMetadata partitionMetadata() {
                return null;
            }

            @Override
            public boolean isResolved() {
                return true;
            }

            @Override
            public boolean isEmpty() {
                return count == 0;
            }

            @Override
            public long estimatedBytes() {
                return 0L;
            }
        };
    }

    private static Configuration configuration() {
        return new Configuration(
            java.time.Instant.EPOCH,
            Locale.ROOT,
            "test",
            "test",
            QueryPragmas.EMPTY,
            1000,
            1000,
            null,
            false,
            Map.of(),
            0L,
            false,
            1000,
            1000,
            ResolvedSettings.EMPTY,
            Map.of()
        );
    }

    private static EsqlExecutionInfo executionInfo() {
        return new EsqlExecutionInfo(Predicates.always(), EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
    }

    private static CircuitBreaker requestBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new HierarchyCircuitBreakerService(CircuitBreakerMetrics.NOOP, settings, List.of(), clusterSettings).getBreaker(
            CircuitBreaker.REQUEST
        );
    }

    private static final class CountingReader implements NoConfigFormatReader {
        private final AggregatePushdownSupport support;

        CountingReader(AggregatePushdownSupport support) {
            this.support = support;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public AggregatePushdownSupport aggregatePushdownSupport() {
            return support;
        }

        @Override
        public void close() {}
    }
}
