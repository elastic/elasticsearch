/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.MMROperator;
import org.elasticsearch.compute.operator.MetricsInfoOperator;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.SampleOperator;
import org.elasticsearch.compute.operator.TsInfoOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.operator.fuse.LinearScoreEvalOperator;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperatorStatus;
import org.elasticsearch.compute.operator.topn.TopNOperatorStatus;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;
import org.elasticsearch.xpack.esql.EsqlInfoTransportAction;
import org.elasticsearch.xpack.esql.EsqlUsageTransportAction;
import org.elasticsearch.xpack.esql.action.EsqlAsyncGetResultAction;
import org.elasticsearch.xpack.esql.action.EsqlAsyncStopAction;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlGetQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlListQueriesAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.action.RestEsqlAsyncQueryAction;
import org.elasticsearch.xpack.esql.action.RestEsqlDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.action.RestEsqlGetAsyncResultAction;
import org.elasticsearch.xpack.esql.action.RestEsqlListQueriesAction;
import org.elasticsearch.xpack.esql.action.RestEsqlQueryAction;
import org.elasticsearch.xpack.esql.action.RestEsqlStopAsyncAction;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.PlanCheckerProvider;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.datasources.CoalescedSplit;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.LocalFileAccess;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetService;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.GetDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.RestDeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.RestGetDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.RestPutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.TransportDeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.TransportGetDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.TransportPutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.GetDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.RestDeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.RestGetDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.RestPutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TransportDeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TransportGetDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TransportPutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.enrich.StreamingLookupFromIndexOperator;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.io.stream.ExpressionQueryBuilder;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamWrapperQueryBuilder;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.GetViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.RestDeleteViewAction;
import org.elasticsearch.xpack.esql.view.RestGetViewAction;
import org.elasticsearch.xpack.esql.view.RestPutViewAction;
import org.elasticsearch.xpack.esql.view.TransportDeleteViewAction;
import org.elasticsearch.xpack.esql.view.TransportGetViewAction;
import org.elasticsearch.xpack.esql.view.TransportPutViewAction;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.elasticsearch.xpack.esql.view.ViewService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class EsqlPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, SearchPlugin {

    private static final Logger logger = LogManager.getLogger(EsqlPlugin.class);

    public static final String ESQL_WORKER_THREAD_POOL_NAME = "esql_worker";

    /**
     * Name of the dedicated thread pool backing all external blob-store access: metadata discovery (glob expansion,
     * footer reads, and schema reconciliation performed by
     * {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver}) as well as the blocking data reads and
     * the streaming parse pipeline (segmentator + parser tasks of
     * {@link org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator}) routed through
     * {@code OperatorFactoryRegistry#fileReadExecutor}.
     * <p>
     * This is a separate pool from {@link #computePool()} on purpose. These tasks block their thread on network I/O
     * (a sequential decompressed stream read pulls compressed bytes from the object store) and on the parser's bounded
     * hand-off queues; the compute {@code Driver} that consumes the parsed pages runs on {@link #computePool()}. If the
     * two shared a fixed pool, the segmentator plus {@code parsing_parallelism} parser tasks would occupy every slot
     * and starve their own consumer, deadlocking the query (observed as a stalled heap-attack external query). Keeping
     * them apart also prevents a single heavy external query from starving compute. In-flight cloud API calls are still
     * bounded by the per-scheme permit semaphore in {@code StorageProviderRegistry}; the permits and this pool solve
     * different problems — concurrency fairness/back-pressure versus thread isolation.
     * <p>
     * Must never resolve to {@link ThreadPool.Names#SEARCH} or {@link ThreadPool.Names#GENERIC}: a single heavy
     * external query would otherwise starve regular searches or the rest of the node.
     */
    public static String externalBlobStorePool() {
        return EXTERNAL_IO_THREAD_POOL_NAME;
    }

    /**
     * Name of the thread pool backing ES|QL compute — driver execution and the parallel worker fan-out. Returns the
     * {@code esql_worker} pool. Distinct from {@link #externalBlobStorePool()} so the blocking external I/O and parse
     * pipeline cannot starve the compute drivers that consume its output (and vice versa).
     */
    public static String computePool() {
        return ESQL_WORKER_THREAD_POOL_NAME;
    }

    /**
     * Name of the dedicated scaling pool for blocking external blob-store I/O and the streaming parse pipeline. Sized
     * {@code 0..}{@link org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings#externalIoThreads(Settings)}
     * — tracking the same single CPU-scaled knob ({@code snapshot_meta} shape, capped at 100, or the
     * {@code esql.external.max_concurrent_requests} operator override) that sizes the permit semaphore and the S3/Azure
     * SDK connection pools, so the pool cannot diverge from the concurrency the reads are permitted. Scales from 0 so
     * idle nodes pay nothing. See {@link #externalBlobStorePool()} for why this is separate from {@code esql_worker}.
     */
    public static final String EXTERNAL_IO_THREAD_POOL_NAME = "esql_external_io";

    public static final Setting<Integer> ESQL_WORKER_THREAD_POOL_SIZE = Setting.intSetting(
        "esql.worker.thread_pool_size",
        -1,
        -1,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> QUERY_ALLOW_PARTIAL_RESULTS = Setting.boolSetting(
        "esql.query.allow_partial_results",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Controls whether LOOKUP JOIN uses the streaming lookup operator, which streams pages to the
     * lookup node instead of performing a request per input page. Acts as an escape hatch to fall
     * back to the non-streaming operator; even when enabled, streaming is only used if all target
     * nodes support the streaming protocol.
     */
    public static final Setting<Boolean> LOOKUP_JOIN_STREAMING = Setting.boolSetting(
        "esql.query.lookup_join_streaming",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Kill switch for the {@code flattened} data type in ES|QL. When {@code false}, {@code flattened} fields resolve as {@code unsupported}
     * during index resolution (the exact pre-flattened-support behavior: {@code FROM} still works and the column is
     * returned as {@code unsupported}, but any explicit use errors). Because {@code field_extract} only operates on
     * {@code flattened} fields, disabling the type also disables that function transitively.
     */
    public static final Setting<Boolean> FLATTENED_ENABLED = Setting.boolSetting(
        "esql.query.flattened.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_WARN_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.warn",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_INFO_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.info",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.debug",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.trace",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> ESQL_QUERYLOG_INCLUDE_USER_SETTING = Setting.boolSetting(
        "esql.querylog.include.user",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum time (in milliseconds) that a GROK matcher is allowed to run before being interrupted.
     * Limits how long a GROK matcher can run to protect against expensive regex patterns. Read directly
     * from each node's own {@link org.elasticsearch.common.settings.ClusterSettings} wherever the matcher
     * is built for execution (see {@code LocalExecutionPlanner#planGrok}) rather than being carried in the
     * ES|QL {@code Configuration} wire format, since every node can resolve this node-scoped setting on
     * its own — including live updates, since it is also dynamic.
     */
    public static final Setting<TimeValue> GROK_WATCHDOG_MAX_EXECUTION_TIME = Setting.timeSetting(
        "esql.grok.watchdog.max_execution_time",
        TimeValue.timeValueMillis(1000),
        TimeValue.timeValueMillis(0),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Tuning parameter for deciding when to use the "merge" stored field loader.
     * Think of it as "how similar to a sequential block of documents do I have to
     * be before I'll use the merge reader?" So a value of {@code 1} means I have to
     * be <strong>exactly</strong> a sequential block, like {@code 0, 1, 2, 3, .. 1299, 1300}.
     * A value of {@code .2} means we'll use the sequential reader even if we only
     * need one in ten documents.
     * <p>
     *     The default value of this was experimentally derived using a
     *     <a href="https://gist.github.com/nik9000/ac6857de10745aad210b6397915ff846">script</a>.
     *     And a little paranoia. A lower default value was looking good locally, but
     *     I'm concerned about the implications of effectively using this all the time.
     * </p>
     */
    public static final Setting<Double> STORED_FIELDS_SEQUENTIAL_PROPORTION = Setting.doubleSetting(
        "index.esql.stored_fields_sequential_proportion",
        0.20,
        0,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    private final List<PlanCheckerProvider> extraCheckerProviders = new ArrayList<>();
    private final List<DataSourcePlugin> dataSourcePlugins = new ArrayList<>();

    private final SetOnce<EsqlCapabilities> capabilities = new SetOnce<>();

    /** Closed by {@link #close()} on node shutdown to release S3/Azure workload-identity resources. */
    private volatile DataSourceModule dataSourceModule;

    @Override
    public void close() throws IOException {
        IOUtils.close(dataSourceModule);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Settings settings = services.clusterService().getSettings();
        BigArrays bigArrays = services.indicesService().getBigArrays().withCircuitBreaking();
        var blockFactoryProvider = blockFactoryProvider(
            BlockFactory.builder(bigArrays)
                .maxPrimitiveArraySize(
                    settings.getAsBytesSize(
                        BlockFactory.MAX_BLOCK_PRIMITIVE_ARRAY_SIZE_SETTING,
                        BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE
                    )
                )
                .bytesRefRamOverestimateThreshold(PlannerSettings.BYTES_REF_RAM_OVERESTIMATE_THRESHOLD.get(settings))
                .bytesRefRamOverestimateFactor(PlannerSettings.BYTES_REF_RAM_OVERESTIMATE_FACTOR.get(settings))
        );
        List<BiConsumer<LogicalPlan, Failures>> extraCheckers = extraCheckerProviders.stream()
            .flatMap(p -> p.checkers(services.projectResolver(), services.clusterService()).stream())
            .toList();

        // Discover DataSourcePlugin implementations via SPI (META-INF/services)
        // This discovers built-in plugins from this plugin's classloader
        List<DataSourcePlugin> allDataSourcePlugins = new ArrayList<>(dataSourcePlugins);
        SPIClassIterator<DataSourcePlugin> spiIterator = SPIClassIterator.get(DataSourcePlugin.class, getClass().getClassLoader());
        while (spiIterator.hasNext()) {
            Class<? extends DataSourcePlugin> pluginClass = spiIterator.next();
            try {
                allDataSourcePlugins.add(pluginClass.getConstructor().newInstance());
            } catch (Exception e) {
                throw new IllegalStateException("Failed to instantiate DataSourcePlugin: " + pluginClass.getName(), e);
            }
        }

        // Build capabilities from plugin declarations (cheap -- no I/O, no heavy deps)
        DataSourceCapabilities dataSourceCapabilities = DataSourceCapabilities.build(allDataSourcePlugins);

        EncryptionService encryptionService = EncryptionServiceRegistry.getEncryptionService();
        DataSourceCredentials dataSourceCredentials = new DataSourceCredentials(encryptionService);

        boolean isStateless = DiscoveryNode.isStateless(settings);
        // Read through MANAGED_IDENTITY_ENABLED, which falls back to the deprecated workload_identity key, so a
        // pre-rename operator config is still honored. Its update consumer fires on changes to either key because the
        // setting's raw value resolves the fallback.
        AtomicBoolean managedIdentityEnabled = new AtomicBoolean(
            isStateless == false && ExternalSourceSettings.MANAGED_IDENTITY_ENABLED.get(settings)
        );
        services.clusterService()
            .getClusterSettings()
            .addSettingsUpdateConsumer(
                ExternalSourceSettings.MANAGED_IDENTITY_ENABLED,
                v -> managedIdentityEnabled.set(isStateless == false && v)
            );

        // Local-disk gate: parsed once at startup (NodeScope setting — no update consumer needed).
        LocalFileAccess localFileAccess = LocalFileAccess.create(settings);

        // Kill switch for the flattened data type. The IndexResolver is a node-level singleton, so the dynamic
        // setting is tracked here in an AtomicBoolean and read (at field-caps resolution time) through a supplier.
        AtomicBoolean flattenedDataTypeEnabled = new AtomicBoolean(FLATTENED_ENABLED.get(settings));
        services.clusterService().getClusterSettings().addSettingsUpdateConsumer(FLATTENED_ENABLED, flattenedDataTypeEnabled::set);

        // Create DataSourceModule with all discovered plugins.
        // This executor backs SPI coordination, decompression, and async-I/O plugin callbacks (e.g. the HTTP
        // client) — NOT the file-read path. Blocking external reads run on the esql_worker pool via
        // OperatorFactoryRegistry#fileReadExecutor (wired in TransportEsqlQueryAction), bounded by the per-scheme
        // permit semaphore in StorageProviderRegistry rather than a dedicated thread pool.
        dataSourceModule = new DataSourceModule(
            allDataSourcePlugins,
            dataSourceCapabilities,
            settings,
            blockFactoryProvider.blockFactory(),
            services.threadPool().executor(ThreadPool.Names.GENERIC),
            dataSourceCredentials,
            managedIdentityEnabled::get,
            services.threadPool(),
            services.environment(),
            services.resourceWatcherService(),
            services.telemetryProvider().getMeterRegistry(),
            localFileAccess
        );

        EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        EsqlParser parser = new EsqlParser(new EsqlConfig(functionRegistry));
        capabilities.set(EsqlCapabilities.capabilities(functionRegistry, false));

        services.ipLocationService()
            .addDatabaseAvailabilityListener(
                (projectId, databaseFile) -> logger.trace(
                    "IP location database [{}] became available for project [{}]",
                    databaseFile,
                    projectId
                )
            );

        ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings);
        services.clusterService()
            .getClusterSettings()
            .addSettingsUpdateConsumer(ExternalSourceCacheSettings.CACHE_ENABLED, cacheService::setEnabled);

        // Build the format metadata the dataset CRUD validator uses to (a) accept format-specific
        // fields (e.g. CSV's "delimiter") so they persist in cluster state and reach the format reader
        // at query time, and (b) resolve a dataset's format from an explicit "format" setting or the
        // resource extension. Iterate ALL FormatSpec declarations (including formats with no extra
        // config keys, e.g. orc) so every registered format is a valid "format" value and every
        // extension resolves to its logical format name.
        //
        // NOTE: FormatReaderRegistry.registerExtension uses a plain put (last writer wins) for the
        // extension→reader mapping at runtime. Here we fail on conflicts so an inconsistency surfaces
        // early at startup; FormatReaderRegistry should be aligned to also reject duplicates.
        // DataSourceCapabilities.build (above) already throws on a duplicate format NAME, so divergent
        // config keys for one format name cannot arise and need no separate check here.
        Map<String, Set<String>> formatToConfigKeys = new HashMap<>();
        Map<String, String> extToFormat = new HashMap<>();
        for (DataSourcePlugin p : allDataSourcePlugins) {
            for (FormatSpec spec : p.formatSpecs()) {
                String format = spec.format().toLowerCase(Locale.ROOT);
                formatToConfigKeys.put(format, spec.configKeys());
                for (String ext : spec.extensions()) {
                    String normalized = ext.toLowerCase(Locale.ROOT);
                    if (normalized.startsWith(".") == false) {
                        normalized = "." + normalized;
                    }
                    String existing = extToFormat.putIfAbsent(normalized, format);
                    if (existing != null && existing.equals(format) == false) {
                        throw new IllegalStateException(
                            "conflicting formats for extension [" + normalized + "]: [" + existing + "] vs [" + format + "]"
                        );
                    }
                }
            }
        }
        // The resolver captures immutable copies of the maps (it is held by every file validator and
        // read concurrently by admin PUT-dataset threads) and derives knownFormats from the config-keys
        // map's key set, so the two sources cannot diverge.
        FileDataSourceValidator.FormatConfigKeyResolver formatKeyResolver = formatToConfigKeys.isEmpty()
            ? null
            : FileDataSourceValidator.FormatConfigKeyResolver.of(formatToConfigKeys, extToFormat);

        // Collect known compression extensions so the CRUD validator only falls back to
        // inner extensions for compound paths (e.g. data.csv.gz) when the outer extension
        // is a registered compression codec — matching DecompressionCodecRegistry behavior.
        Set<String> compressionExtensions = new HashSet<>();
        for (DataSourcePlugin p : allDataSourcePlugins) {
            for (DecompressionCodec codec : p.decompressionCodecs(settings)) {
                for (String ext : codec.extensions()) {
                    String normalized = ext.toLowerCase(Locale.ROOT);
                    if (normalized.startsWith(".") == false) {
                        normalized = "." + normalized;
                    }
                    compressionExtensions.add(normalized);
                }
            }
        }

        Map<String, DataSourceValidator> crudValidators = new HashMap<>();
        for (DataSourcePlugin p : allDataSourcePlugins) {
            p.datasourceValidators(settings).forEach((type, v) -> {
                DataSourceValidator effective = v;
                if (effective instanceof FileDataSourceValidator fdv) {
                    effective = fdv.withManagedIdentityEnabled(managedIdentityEnabled::get)
                        .withFederatedIdentityEnabled(
                            FileDataSourceValidator.ESQL_EXTERNAL_DATASOURCES_FEDERATED_IDENTITY_FEATURE_FLAG::isEnabled
                        );
                }
                if (formatKeyResolver != null && effective instanceof FileDataSourceValidator fdv) {
                    effective = fdv.withFormatConfigKeyResolver(formatKeyResolver, compressionExtensions);
                }
                if (crudValidators.putIfAbsent(type, effective) != null) {
                    throw new IllegalStateException("duplicate DataSourceValidator for type [" + type + "]");
                }
            });
        }

        return List.of(
            new PlanExecutor(
                new IndexResolver(services.client(), flattenedDataTypeEnabled::get),
                services.telemetryProvider().getMeterRegistry(),
                getLicenseState(),
                new EsqlQueryLog(services.clusterService().getClusterSettings(), services.loggingFieldsProvider()),
                extraCheckers,
                services.crossProjectModeDecider(),
                dataSourceModule,
                functionRegistry,
                PromqlFunctionRegistry.INSTANCE,
                parser,
                cacheService,
                services.indicesService().getAnalysis()
            ),
            new ExchangeService(
                services.clusterService().getSettings(),
                services.threadPool(),
                ThreadPool.Names.SEARCH,
                blockFactoryProvider.blockFactory()
            ),
            blockFactoryProvider,
            dataSourceModule,
            dataSourceCredentials,
            new ViewResolver(
                services.threadPool(),
                services.clusterService(),
                services.projectResolver(),
                services.client(),
                services.crossProjectModeDecider()
            ),
            new ViewService(services.clusterService(), parser),
            new DataSourceService(services.clusterService(), crudValidators, encryptionService),
            new DatasetService(services.clusterService(), crudValidators)
        );
    }

    protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
        return new BlockFactoryProvider(builder.build());
    }

    // to be overriden by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    /**
     * The settings defined by the ESQL plugin.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(
            List.of(
                AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE,
                AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE,
                AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE,
                AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE,
                QUERY_ALLOW_PARTIAL_RESULTS,
                LOOKUP_JOIN_STREAMING,
                FLATTENED_ENABLED,
                ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING,
                ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING,
                ESQL_QUERYLOG_THRESHOLD_INFO_SETTING,
                ESQL_QUERYLOG_THRESHOLD_WARN_SETTING,
                ESQL_QUERYLOG_INCLUDE_USER_SETTING,
                STORED_FIELDS_SEQUENTIAL_PROPORTION,
                ESQL_WORKER_THREAD_POOL_SIZE,
                EsqlFlags.ESQL_STRING_LIKE_ON_INDEX,
                EsqlFlags.ESQL_ROUNDTO_PUSHDOWN_THRESHOLD,
                ViewService.MAX_VIEWS_COUNT_SETTING,
                ViewService.MAX_VIEW_LENGTH_SETTING,
                ViewResolver.MAX_VIEW_DEPTH_SETTING,
                DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING,
                DatasetService.MAX_DATASETS_COUNT_SETTING,
                GROK_WATCHDOG_MAX_EXECUTION_TIME
            )
        );
        settings.addAll(PlannerSettings.settings());

        // Inference command settings
        settings.addAll(InferenceSettings.getSettings());

        // External source rate limiting settings
        settings.addAll(ExternalSourceSettings.settings());

        // External source cache settings
        settings.addAll(ExternalSourceCacheSettings.settings());

        return Collections.unmodifiableList(settings);
    }

    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>(
            List.of(
                new ActionHandler(EsqlQueryAction.INSTANCE, TransportEsqlQueryAction.class),
                new ActionHandler(EsqlAsyncGetResultAction.INSTANCE, TransportEsqlAsyncGetResultsAction.class),
                new ActionHandler(EsqlStatsAction.INSTANCE, TransportEsqlStatsAction.class),
                new ActionHandler(XPackUsageFeatureAction.ESQL, EsqlUsageTransportAction.class),
                new ActionHandler(XPackInfoFeatureAction.ESQL, EsqlInfoTransportAction.class),
                new ActionHandler(EsqlResolveFieldsAction.TYPE, EsqlResolveFieldsAction.class),
                new ActionHandler(EsqlSearchShardsAction.TYPE, EsqlSearchShardsAction.class),
                new ActionHandler(EsqlAsyncStopAction.INSTANCE, TransportEsqlAsyncStopAction.class),
                new ActionHandler(EsqlListQueriesAction.INSTANCE, TransportEsqlListQueriesAction.class),
                new ActionHandler(EsqlGetQueryAction.INSTANCE, TransportEsqlGetQueryAction.class),
                new ActionHandler(PutViewAction.INSTANCE, TransportPutViewAction.class),
                new ActionHandler(DeleteViewAction.INSTANCE, TransportDeleteViewAction.class),
                new ActionHandler(EsqlResolveViewAction.TYPE, EsqlResolveViewAction.class),
                // Unconditional like resolve_views: the FROM <dataset> rewrite is gated on datasets being present
                // in cluster state, not on the feature flag, so its authorization gate must always be resolvable.
                new ActionHandler(EsqlResolveDatasetAction.TYPE, EsqlResolveDatasetAction.class),
                new ActionHandler(GetViewAction.INSTANCE, TransportGetViewAction.class)
            )
        );
        if (DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled()) {
            actions.add(new ActionHandler(PutDataSourceAction.INSTANCE, TransportPutDataSourceAction.class));
            actions.add(new ActionHandler(GetDataSourceAction.INSTANCE, TransportGetDataSourceAction.class));
            actions.add(new ActionHandler(DeleteDataSourceAction.INSTANCE, TransportDeleteDataSourceAction.class));
            actions.add(new ActionHandler(PutDatasetAction.INSTANCE, TransportPutDatasetAction.class));
            actions.add(new ActionHandler(GetDatasetAction.INSTANCE, TransportGetDatasetAction.class));
            actions.add(new ActionHandler(DeleteDatasetAction.INSTANCE, TransportDeleteDatasetAction.class));
        }
        return List.copyOf(actions);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        EsqlCapabilities capabilities = this.capabilities.get();
        List<RestHandler> handlers = new ArrayList<>(
            List.of(
                new RestEsqlQueryAction(capabilities),
                new RestEsqlAsyncQueryAction(capabilities),
                new RestEsqlGetAsyncResultAction(),
                new RestEsqlStopAsyncAction(),
                new RestEsqlDeleteAsyncResultAction(),
                new RestEsqlListQueriesAction(),
                new RestPutViewAction(),
                new RestDeleteViewAction(),
                new RestGetViewAction()
            )
        );
        if (DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled()) {
            handlers.add(new RestPutDataSourceAction());
            handlers.add(new RestGetDataSourceAction());
            handlers.add(new RestDeleteDataSourceAction());
            handlers.add(new RestPutDatasetAction());
            handlers.add(new RestGetDatasetAction());
            handlers.add(new RestDeleteDatasetAction());
        }
        return List.copyOf(handlers);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(DriverStatus.ENTRY);
        entries.add(AbstractPageMappingOperator.Status.ENTRY);
        entries.add(AbstractPageMappingToIteratorOperator.Status.ENTRY);
        entries.add(AggregationOperator.Status.ENTRY);
        entries.add(EsqlQueryStatus.ENTRY);
        entries.add(ExchangeSinkOperator.Status.ENTRY);
        entries.add(ExchangeSourceOperator.Status.ENTRY);
        entries.add(org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator.Status.ENTRY);
        entries.add(org.elasticsearch.xpack.esql.datasources.ExternalFieldExtractOperator.Status.ENTRY);
        entries.add(HashAggregationOperator.Status.ENTRY);
        entries.add(LimitOperator.Status.ENTRY);
        entries.add(GroupedLimitOperator.Status.ENTRY);
        entries.add(GroupedTopNOperatorStatus.ENTRY);
        entries.add(LuceneOperator.Status.ENTRY);
        entries.add(TopNOperatorStatus.ENTRY);
        entries.add(MvExpandOperator.Status.ENTRY);
        entries.add(ValuesSourceReaderOperatorStatus.ENTRY);
        entries.add(SingleValueQuery.ENTRY);
        entries.add(AsyncOperator.Status.ENTRY);
        entries.add(EnrichQuerySourceOperator.Status.ENTRY);
        entries.add(EnrichLookupOperator.Status.ENTRY);
        entries.add(LookupFromIndexOperator.Status.ENTRY);
        entries.add(StreamingLookupFromIndexOperator.StreamingLookupStatus.ENTRY);
        entries.add(SampleOperator.Status.ENTRY);
        entries.add(MetricsInfoOperator.Status.ENTRY);
        entries.add(TsInfoOperator.Status.ENTRY);
        entries.add(LinearScoreEvalOperator.Status.ENTRY);
        entries.add(MMROperator.Status.ENTRY);

        entries.add(FileSplit.ENTRY);
        entries.add(CoalescedSplit.ENTRY);

        entries.add(ExpressionQueryBuilder.ENTRY);
        entries.add(PlanStreamWrapperQueryBuilder.ENTRY);
        entries.addAll(ViewMetadata.ENTRIES);
        entries.addAll(DataSourceMetadata.ENTRIES);
        entries.addAll(DatasetMetadata.ENTRIES);
        // Lets an encrypted data-source secret ride the plan's generic-value serialization to data nodes.
        entries.add(EncryptedData.ENTRY);

        entries.addAll(ExpressionWritables.getNamedWriteables());
        entries.addAll(PlanWritables.getNamedWriteables());
        return entries;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(ViewMetadata.TYPE), ViewMetadata::fromXContent),
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(DataSourceMetadata.TYPE),
                DataSourceMetadata::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(DatasetMetadata.TYPE),
                DatasetMetadata::fromXContent
            )
        );
    }

    // visible for testing
    static int workerQueueSize(long heapBytes, int allocatedProcessors) {
        long heapMb = heapBytes / (1024 * 1024);
        return (int) Math.max((heapMb + (long) allocatedProcessors * 400) / 2, 1000);
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        int configuredSize = ESQL_WORKER_THREAD_POOL_SIZE.get(settings);
        int poolSize = configuredSize > 0 ? configuredSize : ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors);
        int queueSize = workerQueueSize(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes(), allocatedProcessors);
        return List.of(
            new FixedExecutorBuilder(
                settings,
                ESQL_WORKER_THREAD_POOL_NAME,
                poolSize,
                queueSize,
                ESQL_WORKER_THREAD_POOL_NAME,
                EsExecutors.TaskTrackingConfig.DEFAULT
            ),
            // Dedicated scaling pool for blocking external blob-store I/O and the streaming parse pipeline, kept
            // separate from esql_worker so the segmentator/parser tasks cannot starve the compute drivers that
            // consume their output. Max is the single CPU-scaled concurrency knob (snapshot_meta shape, capped at
            // 100, or the esql.external.max_concurrent_requests override) that also sizes the permit semaphore, so
            // pool capacity tracks the concurrency the reads are permitted. Scales from 0 so idle nodes pay nothing.
            new ScalingExecutorBuilder(
                EXTERNAL_IO_THREAD_POOL_NAME,
                0,
                ExternalSourceSettings.externalIoThreads(settings),
                TimeValue.timeValueSeconds(30),
                false
            )
        );
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        extraCheckerProviders.addAll(loader.loadExtensions(PlanCheckerProvider.class));
        dataSourcePlugins.addAll(loader.loadExtensions(DataSourcePlugin.class));
    }

    @Override
    public List<SearchPlugin.GenericNamedWriteableSpec> getGenericNamedWriteables() {
        List<SearchPlugin.GenericNamedWriteableSpec> entries = new ArrayList<>(ExpressionWritables.getGenericNamedWriteables());
        entries.add(new SearchPlugin.GenericNamedWriteableSpec("LongRange", LongRangeBlockBuilder.LongRange::new));
        return entries;
    }
}
