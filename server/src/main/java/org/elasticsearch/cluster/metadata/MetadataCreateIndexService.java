/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.resolveSettings;
import static org.elasticsearch.index.IndexModule.INDEX_RECOVERY_TYPE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

/**
 * Service responsible for submitting create index requests
 */
public class MetadataCreateIndexService {
    private static final Logger logger = LogManager.getLogger(MetadataCreateIndexService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MetadataCreateIndexService.class);

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final Environment env;
    private final IndexScopedSettings indexScopedSettings;
    private final NamedXContentRegistry xContentRegistry;
    private final SystemIndices systemIndices;
    private final ShardLimitValidator shardLimitValidator;
    private final boolean forbidPrivateIndexSettings;
    private final Set<IndexSettingProvider> indexSettingProviders;
    private final ThreadPool threadPool;

    public MetadataCreateIndexService(
        final Settings settings,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final ShardLimitValidator shardLimitValidator,
        final Environment env,
        final IndexScopedSettings indexScopedSettings,
        final ThreadPool threadPool,
        final NamedXContentRegistry xContentRegistry,
        final SystemIndices systemIndices,
        final boolean forbidPrivateIndexSettings,
        final IndexSettingProviders indexSettingProviders
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.xContentRegistry = xContentRegistry;
        this.systemIndices = systemIndices;
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
        this.shardLimitValidator = shardLimitValidator;
        this.indexSettingProviders = indexSettingProviders.getIndexSettingProviders();
        this.threadPool = threadPool;
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     */
    public static void validateIndexName(String index, Metadata metadata, RoutingTable routingTable) {
        validateIndexOrAliasName(index, InvalidIndexNameException::new);
        if (index.toLowerCase(Locale.ROOT).equals(index) == false) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }

        // NOTE: dot-prefixed index names are validated after template application, not here

        if (routingTable.hasIndex(index)) {
            throw new ResourceAlreadyExistsException(routingTable.index(index).getIndex());
        }
        if (metadata.hasIndex(index)) {
            throw new ResourceAlreadyExistsException(metadata.index(index).getIndex());
        }
        if (metadata.hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
    }

    /**
     * Validates (if this index has a dot-prefixed name) whether it follows the rules for dot-prefixed indices.
     * @param index The name of the index in question
     * @param isHidden Whether or not this is a hidden index
     */
    public boolean validateDotIndex(String index, @Nullable Boolean isHidden) {
        boolean isSystem = false;
        if (index.charAt(0) == '.') {
            isSystem = systemIndices.isSystemName(index);
            if (isSystem) {
                logger.trace("index [{}] is a system index", index);
            } else if (isHidden) {
                logger.trace("index [{}] is a hidden index", index);
            } else {
                deprecationLogger.warn(
                    DeprecationCategory.INDICES,
                    "index_name_starts_with_dot",
                    "index name [{}] starts with a dot '.', in the next major version, index names "
                        + "starting with a dot are reserved for hidden indices and system indices",
                    index
                );
            }
        }

        return isSystem;
    }

    public SystemIndices getSystemIndices() {
        return systemIndices;
    }

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public static void validateIndexOrAliasName(String index, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (Strings.validFileName(index) == false) {
            throw exceptionCtor.apply(index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.contains("#")) {
            throw exceptionCtor.apply(index, "must not contain '#'");
        }
        if (index.contains(":")) {
            throw exceptionCtor.apply(index, "must not contain ':'");
        }
        if (index.charAt(0) == '_' || index.charAt(0) == '-' || index.charAt(0) == '+') {
            throw exceptionCtor.apply(index, "must not start with '_', '-', or '+'");
        }
        int byteCount = index.getBytes(StandardCharsets.UTF_8).length;
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(index, "index name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (index.equals(".") || index.equals("..")) {
            throw exceptionCtor.apply(index, "must not be '.' or '..'");
        }
    }

    /**
     * Creates an index in the cluster state and waits for the specified number of shard copies to
     * become active (as specified in {@link CreateIndexClusterStateUpdateRequest#waitForActiveShards()})
     * before sending the response on the listener. If the index creation was successfully applied on
     * the cluster state, then {@link ShardsAcknowledgedResponse#isAcknowledged()} will return
     * true, otherwise it will return false and no waiting will occur for started shards
     * ({@link ShardsAcknowledgedResponse#isShardsAcknowledged()} will also be false).  If the index
     * creation in the cluster state was successful and the requisite shard copies were started before
     * the timeout, then {@link ShardsAcknowledgedResponse#isShardsAcknowledged()} will
     * return true, otherwise if the operation timed out, then it will return false.
     *
     * @param masterNodeTimeout timeout on cluster state update in pending task queue
     * @param ackTimeout        timeout on waiting for all nodes to ack the cluster state update
     * @param waitForActiveShardsTimeout timeout for waiting for the {@link ActiveShardCount} specified in
     *                                   {@link CreateIndexClusterStateUpdateRequest#waitForActiveShards()} to be satisfied.
     *                                   May also be {@code null}, in which case it waits forever.
     * @param request the index creation cluster state update request
     * @param listener the listener on which to send the index creation cluster state update response
     */
    public void createIndex(
        final TimeValue masterNodeTimeout,
        final TimeValue ackTimeout,
        @Nullable final TimeValue waitForActiveShardsTimeout,
        final CreateIndexClusterStateUpdateRequest request,
        final ActionListener<ShardsAcknowledgedResponse> listener
    ) {
        logger.trace("createIndex[{}]", request);
        onlyCreateIndex(masterNodeTimeout, ackTimeout, request, listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged()) {
                logger.trace(
                    "[{}] index creation acknowledged, waiting for active shards [{}]",
                    request.index(),
                    request.waitForActiveShards()
                );
                ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    new String[] { request.index() },
                    request.waitForActiveShards(),
                    waitForActiveShardsTimeout,
                    delegate.map(shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug(
                                "[{}] index created, but the operation timed out while waiting for enough shards to be started.",
                                request.index()
                            );
                        } else {
                            logger.trace("[{}] index created and shards acknowledged", request.index());
                        }
                        return ShardsAcknowledgedResponse.of(true, shardsAcknowledged);
                    })
                );
            } else {
                logger.trace("index creation not acknowledged for [{}]", request);
                delegate.onResponse(ShardsAcknowledgedResponse.NOT_ACKNOWLEDGED);
            }
        }));
    }

    private void onlyCreateIndex(
        final TimeValue masterNodeTimeout,
        final TimeValue ackTimeout,
        final CreateIndexClusterStateUpdateRequest request,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        normalizeRequestSetting(request);

        var delegate = new AllocationActionListener<>(listener, threadPool.getThreadContext());
        submitUnbatchedTask(
            "create-index [" + request.index() + "], cause [" + request.cause() + "]",
            new AckedClusterStateUpdateTask(Priority.URGENT, masterNodeTimeout, ackTimeout, delegate.clusterStateUpdate()) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return applyCreateIndexRequest(currentState, request, false, null, delegate.reroute());
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.trace(() -> "[" + request.index() + "] failed to create", e);
                    } else {
                        logger.debug(() -> "[" + request.index() + "] failed to create", e);
                    }
                    super.onFailure(e);
                }
            }
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private void normalizeRequestSetting(CreateIndexClusterStateUpdateRequest createIndexClusterStateRequest) {
        Settings build = Settings.builder()
            .put(createIndexClusterStateRequest.settings())
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        indexScopedSettings.validate(build, true);
        createIndexClusterStateRequest.settings(build);
    }

    /**
     * Handles the cluster state transition to a version that reflects the {@link CreateIndexClusterStateUpdateRequest}.
     * All the requested changes are firstly validated before mutating the {@link ClusterState}.
     */
    public ClusterState applyCreateIndexRequest(
        ClusterState currentState,
        CreateIndexClusterStateUpdateRequest request,
        boolean silent,
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        ActionListener<Void> rerouteListener
    ) throws Exception {

        normalizeRequestSetting(request);
        logger.trace("executing IndexCreationTask for [{}] against cluster state version [{}]", request, currentState.version());

        validate(request, currentState.metadata(), currentState.routingTable());

        final Index recoverFromIndex = request.recoverFrom();
        final IndexMetadata sourceMetadata = recoverFromIndex == null ? null : currentState.metadata().getIndexSafe(recoverFromIndex);

        if (sourceMetadata != null) {
            // If source metadata was provided, it means we're recovering from an existing index,
            // in which case templates don't apply, so create the index from the source metadata
            return applyCreateIndexRequestWithExistingMetadata(
                currentState,
                request,
                silent,
                sourceMetadata,
                metadataTransformer,
                rerouteListener
            );
        } else {
            // The backing index may have a different name or prefix than the data stream name.
            final String name = request.dataStreamName() != null ? request.dataStreamName() : request.index();

            // The index being created is for a system data stream, so the backing index will also be a system index
            if (request.systemDataStreamDescriptor() != null) {
                return applyCreateIndexRequestForSystemDataStream(currentState, request, silent, metadataTransformer, rerouteListener);
            }

            SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(request.index());
            // ignore all templates for all system indices that do not allow templates.
            // Essentially, all but .kibana indices, see KibanaPlugin.java.
            if (Objects.nonNull(descriptor) && descriptor.allowsTemplates() == false) {
                return applyCreateIndexRequestForSystemIndex(currentState, request, silent, descriptor.getIndexPattern(), rerouteListener);
            }

            // Hidden indices apply templates slightly differently (ignoring wildcard '*'
            // templates), so we need to check to see if the request is creating a hidden index
            // prior to resolving which templates it matches
            final Boolean isHiddenFromRequest = IndexMetadata.INDEX_HIDDEN_SETTING.exists(request.settings())
                ? IndexMetadata.INDEX_HIDDEN_SETTING.get(request.settings())
                : null;

            // Check to see if a v2 template matched
            final String v2Template = MetadataIndexTemplateService.findV2Template(
                currentState.metadata(),
                name,
                isHiddenFromRequest != null && isHiddenFromRequest
            );

            if (v2Template != null) {
                // If a v2 template was found, it takes precedence over all v1 templates, so create
                // the index using that template and the request's specified settings
                return applyCreateIndexRequestWithV2Template(
                    currentState,
                    request,
                    silent,
                    v2Template,
                    metadataTransformer,
                    rerouteListener
                );
            } else {
                // A v2 template wasn't found, check the v1 templates, in the event no templates are
                // found creation still works using the request's specified index settings
                final List<IndexTemplateMetadata> v1Templates = MetadataIndexTemplateService.findV1Templates(
                    currentState.metadata(),
                    request.index(),
                    isHiddenFromRequest
                );

                if (v1Templates.size() > 1) {
                    deprecationLogger.warn(
                        DeprecationCategory.TEMPLATES,
                        "index_template_multiple_match",
                        "index [{}] matches multiple legacy templates [{}], composable templates will only match a single template",
                        request.index(),
                        v1Templates.stream().map(IndexTemplateMetadata::name).sorted().collect(Collectors.joining(", "))
                    );
                }

                return applyCreateIndexRequestWithV1Templates(
                    currentState,
                    request,
                    silent,
                    v1Templates,
                    metadataTransformer,
                    rerouteListener
                );
            }
        }
    }

    public ClusterState applyCreateIndexRequest(
        ClusterState currentState,
        CreateIndexClusterStateUpdateRequest request,
        boolean silent,
        ActionListener<Void> rerouteListener
    ) throws Exception {
        return applyCreateIndexRequest(currentState, request, silent, null, rerouteListener);
    }

    /**
     * Given the state and a request as well as the metadata necessary to build a new index,
     * validate the configuration with an actual index service as return a new cluster state with
     * the index added (and rerouted)
     * @param currentState the current state to base the new state off of
     * @param request the create index request
     * @param silent a boolean for whether logging should be at a lower or higher level
     * @param sourceMetadata when recovering from an existing index, metadata that should be copied to the new index
     * @param temporaryIndexMeta metadata for the new index built from templates, source metadata, and request settings
     * @param mappings a list of all mapping definitions to apply, in order
     * @param aliasSupplier a function that takes the real {@link IndexService} and returns a list of {@link AliasMetadata} aliases
     * @param templatesApplied a list of the names of the templates applied, for logging
     * @param metadataTransformer if provided, a function that may alter cluster metadata in the same cluster state update that
     *                            creates the index
     * @return a new cluster state with the index added
     */
    private ClusterState applyCreateIndexWithTemporaryService(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final IndexMetadata sourceMetadata,
        final IndexMetadata temporaryIndexMeta,
        final List<CompressedXContent> mappings,
        final Function<IndexService, List<AliasMetadata>> aliasSupplier,
        final List<String> templatesApplied,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        final ActionListener<Void> rerouteListener
    ) throws Exception {
        // create the index here (on the master) to validate it can be created, as well as adding the mapping
        assert indicesService.hasIndex(temporaryIndexMeta.getIndex()) == false
            : Strings.format("Index [%s] already exists", temporaryIndexMeta.getIndex().getName());
        return indicesService.<ClusterState, Exception>withTempIndexService(temporaryIndexMeta, indexService -> {
            try {
                updateIndexMappingsAndBuildSortOrder(indexService, request, mappings, sourceMetadata);
            } catch (Exception e) {
                logger.log(silent ? Level.DEBUG : Level.INFO, "failed on parsing mappings on index creation [{}]", request.index(), e);
                throw e;
            }

            final List<AliasMetadata> aliases = aliasSupplier.apply(indexService);

            final IndexMetadata indexMetadata;
            try {
                indexMetadata = buildIndexMetadata(
                    request.index(),
                    aliases,
                    indexService.mapperService()::documentMapper,
                    temporaryIndexMeta.getSettings(),
                    temporaryIndexMeta.getRoutingNumShards(),
                    sourceMetadata,
                    temporaryIndexMeta.isSystem(),
                    currentState.getMinTransportVersion()
                );
            } catch (Exception e) {
                logger.info("failed to build index metadata [{}]", request.index());
                throw e;
            }

            logger.log(
                silent ? Level.DEBUG : Level.INFO,
                "[{}] creating index, cause [{}], templates {}, shards [{}]/[{}]",
                request.index(),
                request.cause(),
                templatesApplied,
                indexMetadata.getNumberOfShards(),
                indexMetadata.getNumberOfReplicas()
            );

            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetadata.getIndex(), indexMetadata.getSettings());

            ClusterState updated = clusterStateCreateIndex(
                currentState,
                indexMetadata,
                metadataTransformer,
                allocationService.getShardRoutingRoleStrategy()
            );
            if (request.performReroute()) {
                updated = allocationService.reroute(updated, "index [" + indexMetadata.getIndex().getName() + "] created", rerouteListener);
            }
            return updated;
        });
    }

    /**
     * Given a state and index settings calculated after applying templates, validate metadata for
     * the new index, returning an {@link IndexMetadata} for the new index
     */
    private IndexMetadata buildAndValidateTemporaryIndexMetadata(
        final Settings aggregatedIndexSettings,
        final CreateIndexClusterStateUpdateRequest request,
        final int routingNumShards
    ) {

        final boolean isHiddenAfterTemplates = IndexMetadata.INDEX_HIDDEN_SETTING.get(aggregatedIndexSettings);
        final boolean isSystem = validateDotIndex(request.index(), isHiddenAfterTemplates);

        // remove the setting it's temporary and is only relevant once we create the index
        final Settings.Builder settingsBuilder = Settings.builder().put(aggregatedIndexSettings);
        settingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
        final Settings indexSettings = settingsBuilder.build();

        final IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(request.index());
        tmpImdBuilder.setRoutingNumShards(routingNumShards);
        tmpImdBuilder.settings(indexSettings);
        tmpImdBuilder.system(isSystem);

        // Set up everything, now locally create the index to see that things are ok, and apply
        IndexMetadata tempMetadata = tmpImdBuilder.build();
        validateActiveShardCount(request.waitForActiveShards(), tempMetadata);

        return tempMetadata;
    }

    // TODO: this method can be removed in 9.0 because we will no longer use v1 templates to create indices (only v2 templates)
    private ClusterState applyCreateIndexRequestWithV1Templates(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final List<IndexTemplateMetadata> templates,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        final ActionListener<Void> rerouteListener
    ) throws Exception {
        logger.debug(
            "applying create index request using legacy templates {}",
            templates.stream().map(IndexTemplateMetadata::name).toList()
        );

        final Map<String, Object> mappingsMap = parseV1Mappings(
            request.mappings(),
            templates.stream().map(IndexTemplateMetadata::getMappings).collect(toList()),
            xContentRegistry
        );

        final CompressedXContent mappings;
        if (mappingsMap.isEmpty()) {
            mappings = null;
        } else {
            mappings = new CompressedXContent(mappingsMap);
        }

        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            resolveSettings(templates),
            mappings == null ? List.of() : List.of(mappings),
            null,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders
        );
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            null,
            tmpImd,
            mappings == null ? List.of() : List.of(mappings),
            indexService -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                MetadataIndexTemplateService.resolveAliases(templates),
                currentState.metadata(),
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                indexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(request.getNameResolvedAt()),
                systemIndices::isSystemName
            ),
            templates.stream().map(IndexTemplateMetadata::getName).collect(toList()),
            metadataTransformer,
            rerouteListener
        );
    }

    private ClusterState applyCreateIndexRequestWithV2Template(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final String templateName,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        final ActionListener<Void> rerouteListener
    ) throws Exception {
        logger.debug("applying create index request using composable template [{}]", templateName);

        ComposableIndexTemplate template = currentState.getMetadata().templatesV2().get(templateName);
        final boolean isDataStream = template.getDataStreamTemplate() != null;
        if (isDataStream && request.dataStreamName() == null) {
            throw new IllegalArgumentException(
                "cannot create index with name ["
                    + request.index()
                    + "], because it matches with template ["
                    + templateName
                    + "] that creates data streams only, "
                    + "use create data stream api instead"
            );
        }

        final List<CompressedXContent> mappings = collectV2Mappings(
            request.mappings(),
            currentState,
            templateName,
            xContentRegistry,
            request.index()
        );
        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            resolveSettings(currentState.metadata(), templateName),
            mappings,
            null,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders
        );
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            null,
            tmpImd,
            mappings,
            indexService -> resolveAndValidateAliases(
                request.index(),
                // data stream aliases are created separately in MetadataCreateDataStreamService::createDataStream
                isDataStream ? Set.of() : request.aliases(),
                isDataStream ? List.of() : MetadataIndexTemplateService.resolveAliases(currentState.metadata(), templateName),
                currentState.metadata(),
                xContentRegistry,
                // the context is used ony for validation so it's fine to pass fake values for the shard id and the current timestamp
                indexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(request.getNameResolvedAt()),
                systemIndices::isSystemName
            ),
            Collections.singletonList(templateName),
            metadataTransformer,
            rerouteListener
        );
    }

    private ClusterState applyCreateIndexRequestForSystemIndex(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final String indexPattern,
        final ActionListener<Void> rerouteListener
    ) throws Exception {
        logger.debug("applying create index request for system index [{}] matching pattern [{}]", request.index(), indexPattern);

        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            Settings.EMPTY,
            null,
            null,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders
        );
        final int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        final IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            null,
            tmpImd,
            List.of(new CompressedXContent(MapperService.parseMapping(xContentRegistry, request.mappings()))),
            indexService -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                List.of(),
                currentState.metadata(),
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                indexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(request.getNameResolvedAt()),
                systemIndices::isSystemName
            ),
            List.of(),
            null,
            rerouteListener
        );
    }

    private ClusterState applyCreateIndexRequestForSystemDataStream(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        final ActionListener<Void> rerouteListener
    ) throws Exception {
        Objects.requireNonNull(request.systemDataStreamDescriptor());
        logger.debug("applying create index request for system data stream [{}]", request.systemDataStreamDescriptor());

        ComposableIndexTemplate template = request.systemDataStreamDescriptor().getComposableIndexTemplate();
        if (request.dataStreamName() == null && template.getDataStreamTemplate() != null) {
            throw new IllegalArgumentException(
                "cannot create index with name [" + request.index() + "], because it matches with a system data stream"
            );
        }

        final Map<String, ComponentTemplate> componentTemplates = request.systemDataStreamDescriptor().getComponentTemplates();
        final List<CompressedXContent> mappings = collectSystemV2Mappings(template, componentTemplates, xContentRegistry, request.index());

        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            resolveSettings(template, componentTemplates),
            mappings,
            null,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders
        );
        final int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        final IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            null,
            tmpImd,
            mappings,
            indexService -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                MetadataIndexTemplateService.resolveAliases(template, componentTemplates),
                currentState.metadata(),
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry,
                indexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(request.getNameResolvedAt()),
                systemIndices::isSystemName
            ),
            List.of(),
            metadataTransformer,
            rerouteListener
        );
    }

    private static List<CompressedXContent> collectSystemV2Mappings(
        final ComposableIndexTemplate composableIndexTemplate,
        final Map<String, ComponentTemplate> componentTemplates,
        final NamedXContentRegistry xContentRegistry,
        final String indexName
    ) throws Exception {
        List<CompressedXContent> templateMappings = MetadataIndexTemplateService.collectMappings(
            composableIndexTemplate,
            componentTemplates,
            indexName
        );
        return collectV2Mappings(null, templateMappings, xContentRegistry);
    }

    public static List<CompressedXContent> collectV2Mappings(
        @Nullable final String requestMappings,
        final ClusterState currentState,
        final String templateName,
        final NamedXContentRegistry xContentRegistry,
        final String indexName
    ) throws Exception {
        List<CompressedXContent> templateMappings = MetadataIndexTemplateService.collectMappings(currentState, templateName, indexName);
        return collectV2Mappings(requestMappings, templateMappings, xContentRegistry);
    }

    private static List<CompressedXContent> collectV2Mappings(
        @Nullable final String requestMappings,
        final List<CompressedXContent> templateMappings,
        final NamedXContentRegistry xContentRegistry
    ) throws Exception {
        List<CompressedXContent> result = new ArrayList<>(templateMappings.size() + 1);
        result.addAll(templateMappings);
        if (requestMappings != null) {
            Map<String, Object> parsedRequestMappings = MapperService.parseMapping(xContentRegistry, requestMappings);
            if (parsedRequestMappings.isEmpty() == false) {
                result.add(new CompressedXContent(parsedRequestMappings));
            }
        }
        return result;
    }

    private ClusterState applyCreateIndexRequestWithExistingMetadata(
        final ClusterState currentState,
        final CreateIndexClusterStateUpdateRequest request,
        final boolean silent,
        final IndexMetadata sourceMetadata,
        final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        final ActionListener<Void> rerouteListener
    ) throws Exception {
        logger.info("applying create index request using existing index [{}] metadata", sourceMetadata.getIndex().getName());

        final Map<String, Object> mappings = MapperService.parseMapping(xContentRegistry, request.mappings());
        if (mappings.isEmpty() == false) {
            throw new IllegalArgumentException(
                "mappings are not allowed when creating an index from a source index, " + "all mappings are copied from the source index"
            );
        }

        final Settings aggregatedIndexSettings = aggregateIndexSettings(
            currentState,
            request,
            Settings.EMPTY,
            null,
            sourceMetadata,
            settings,
            indexScopedSettings,
            shardLimitValidator,
            indexSettingProviders
        );
        final int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, sourceMetadata);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(
            currentState,
            request,
            silent,
            sourceMetadata,
            tmpImd,
            List.of(),
            indexService -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                Collections.emptyList(),
                currentState.metadata(),
                xContentRegistry,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                indexService.newSearchExecutionContext(0, 0, null, () -> 0L, null, emptyMap()),
                IndexService.dateMathExpressionResolverAt(request.getNameResolvedAt()),
                systemIndices::isSystemName
            ),
            List.of(),
            metadataTransformer,
            rerouteListener
        );
    }

    /**
     * Parses the provided mappings json and the inheritable mappings from the templates (if any)
     * into a map.
     *
     * The template mappings are applied in the order they are encountered in the list (clients
     * should make sure the lower index, closer to the head of the list, templates have the highest
     * {@link IndexTemplateMetadata#order()}). This merging makes no distinction between field
     * definitions, as may result in an invalid field definition
     */
    public static Map<String, Object> parseV1Mappings(
        String mappingsJson,
        List<CompressedXContent> templateMappings,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        Map<String, Object> mappings = MapperService.parseMapping(xContentRegistry, mappingsJson);
        // apply templates, merging the mappings into the request mapping if exists
        for (CompressedXContent mapping : templateMappings) {
            if (mapping != null) {
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mapping);
                if (templateMapping.isEmpty()) {
                    // Someone provided an empty '{}' for mappings, which is okay, but to avoid
                    // tripping the below assertion, we can safely ignore it
                    continue;
                }
                assert templateMapping.size() == 1 : "expected exactly one mapping value, got: " + templateMapping;
                // pre-8x templates may have a wrapper type other than _doc, so we re-wrap things here
                templateMapping = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, templateMapping.values().iterator().next());
                if (mappings.isEmpty()) {
                    mappings = templateMapping;
                } else {
                    XContentHelper.mergeDefaults(mappings, templateMapping);
                }
            }
        }
        return mappings;
    }

    /**
     * Validates and creates the settings for the new index based on the explicitly configured settings via the
     * {@link CreateIndexClusterStateUpdateRequest}, inherited from templates and, if recovering from another index (ie. split, shrink,
     * clone), the resize settings.
     *
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetadata#order()})
     *
     * @return the aggregated settings for the new index
     */
    static Settings aggregateIndexSettings(
        ClusterState currentState,
        CreateIndexClusterStateUpdateRequest request,
        Settings combinedTemplateSettings,
        List<CompressedXContent> combinedTemplateMappings,
        @Nullable IndexMetadata sourceMetadata,
        Settings settings,
        IndexScopedSettings indexScopedSettings,
        ShardLimitValidator shardLimitValidator,
        Set<IndexSettingProvider> indexSettingProviders
    ) {
        final boolean isDataStreamIndex = request.dataStreamName() != null;
        final var metadata = currentState.getMetadata();

        // Create builders for the template and request settings. We transform these into builders
        // because we may want settings to be "removed" from these prior to being set on the new
        // index (see more comments below)
        final Settings.Builder templateSettings = Settings.builder().put(combinedTemplateSettings);
        final Settings.Builder requestSettings = Settings.builder().put(request.settings());

        final Settings.Builder indexSettingsBuilder = Settings.builder();
        if (sourceMetadata == null) {
            final Settings templateAndRequestSettings = Settings.builder().put(combinedTemplateSettings).put(request.settings()).build();

            final IndexMode templateIndexMode = Optional.of(request)
                .filter(r -> r.isFailureIndex() == false)
                .map(CreateIndexClusterStateUpdateRequest::matchingTemplate)
                .map(metadata::retrieveIndexModeFromTemplate)
                .orElse(null);

            // Loop through all the explicit index setting providers, adding them to the
            // additionalIndexSettings map
            final Settings.Builder additionalIndexSettings = Settings.builder();
            final var resolvedAt = Instant.ofEpochMilli(request.getNameResolvedAt());
            Set<String> overrulingSettings = new HashSet<>();
            for (IndexSettingProvider provider : indexSettingProviders) {
                var newAdditionalSettings = provider.getAdditionalIndexSettings(
                    request.index(),
                    request.dataStreamName(),
                    templateIndexMode,
                    currentState.getMetadata(),
                    resolvedAt,
                    templateAndRequestSettings,
                    combinedTemplateMappings
                );
                validateAdditionalSettings(provider, newAdditionalSettings, additionalIndexSettings);
                additionalIndexSettings.put(newAdditionalSettings);
                if (provider.overrulesTemplateAndRequestSettings()) {
                    overrulingSettings.addAll(newAdditionalSettings.keySet());
                }
            }

            for (String explicitSetting : additionalIndexSettings.keys()) {
                if (overrulingSettings.contains(explicitSetting)) {
                    // Remove any conflicting template and request settings to use the provided values.
                    templateSettings.remove(explicitSetting);
                    requestSettings.remove(explicitSetting);
                } else {
                    // For all the explicit settings, we go through the template and request level settings
                    // and see if either a template or the request has "cancelled out" an explicit default
                    // setting. For example, if a plugin had as an explicit setting:
                    // "index.mysetting": "blah
                    // And either a template or create index request had:
                    // "index.mysetting": null
                    // We want to remove the explicit setting not only from the explicitly set settings, but
                    // also from the template and request settings, so that from the newly create index's
                    // perspective it is as though the setting has not been set at all (using the default
                    // value).
                    if (templateSettings.keys().contains(explicitSetting) && templateSettings.get(explicitSetting) == null) {
                        logger.debug(
                            "removing default [{}] setting as it is set to null in a template for [{}] creation",
                            explicitSetting,
                            request.index()
                        );
                        additionalIndexSettings.remove(explicitSetting);
                        templateSettings.remove(explicitSetting);
                    }
                    if (requestSettings.keys().contains(explicitSetting) && requestSettings.get(explicitSetting) == null) {
                        logger.debug(
                            "removing default [{}] setting as it is set to null in the request for [{}] creation",
                            explicitSetting,
                            request.index()
                        );
                        additionalIndexSettings.remove(explicitSetting);
                        requestSettings.remove(explicitSetting);
                    }
                }
            }

            // Finally, we actually add the explicit defaults prior to the template settings and the
            // request settings, so that the precedence goes:
            // Explicit Defaults -> Template -> Request -> Filter out failure store settings -> Necessary Settings (# of shards, uuid, etc)
            indexSettingsBuilder.put(additionalIndexSettings.build());
            indexSettingsBuilder.put(templateSettings.build());
        }
        if (request.isFailureIndex()) {
            DataStreamFailureStoreDefinition.filterUserDefinedSettings(indexSettingsBuilder);
        }
        // now, put the request settings, so they override templates
        indexSettingsBuilder.put(requestSettings.build());

        if (sourceMetadata == null) { // not for shrink/split/clone
            // regardless of any previous logic, we're going to force there
            // to be an appropriate non-empty value for the tier preference
            String currentTierPreference = indexSettingsBuilder.get(DataTier.TIER_PREFERENCE);
            if (DataTier.parseTierList(currentTierPreference).isEmpty()) {
                String newTierPreference = isDataStreamIndex ? DataTier.DATA_HOT : DataTier.DATA_CONTENT;
                logger.debug(
                    "enforcing default [{}] setting for [{}] creation, replacing [{}] with [{}]",
                    DataTier.TIER_PREFERENCE,
                    request.index(),
                    currentTierPreference,
                    newTierPreference
                );
                indexSettingsBuilder.put(DataTier.TIER_PREFERENCE, newTierPreference);
            }
        }

        if (indexSettingsBuilder.get(IndexMetadata.SETTING_VERSION_CREATED) == null) {
            DiscoveryNodes nodes = currentState.nodes();
            IndexVersion createdVersion = IndexVersion.min(IndexVersion.current(), nodes.getMaxDataNodeCompatibleIndexVersion());
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, createdVersion);
        }
        if (INDEX_NUMBER_OF_SHARDS_SETTING.exists(indexSettingsBuilder) == false) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, INDEX_NUMBER_OF_SHARDS_SETTING.get(settings));
        }
        if (INDEX_NUMBER_OF_REPLICAS_SETTING.exists(indexSettingsBuilder) == false) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings));
        }
        if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
        }

        if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
        }
        indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName());
        indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        if (sourceMetadata != null) {
            assert request.resizeType() != null;
            prepareResizeIndexSettings(
                currentState.metadata(),
                currentState.blocks(),
                currentState.routingTable(),
                indexSettingsBuilder,
                request.recoverFrom(),
                request.index(),
                request.resizeType(),
                request.copySettings(),
                indexScopedSettings
            );
        }

        Settings indexSettings = indexSettingsBuilder.build();
        /*
         * We can not validate settings until we have applied templates, otherwise we do not know the actual settings
         * that will be used to create this index.
         */
        shardLimitValidator.validateShardLimit(indexSettings, currentState.nodes(), currentState.metadata());
        validateSoftDeleteSettings(indexSettings);
        validateTranslogRetentionSettings(indexSettings);
        validateStoreTypeSetting(indexSettings);
        return indexSettings;
    }

    /**
     * Validates whether additional settings don't have keys that are already defined in all additional settings.
     *
     * @param provider                  The {@link IndexSettingProvider} that produced <code>additionalSettings</code>
     * @param additionalSettings        The settings produced by the specified <code>provider</code>
     * @param allAdditionalSettings     A settings builder containing all additional settings produced by any {@link IndexSettingProvider}
     *                                  that already executed
     * @throws IllegalArgumentException If keys in additionalSettings are already defined in allAdditionalSettings
     */
    public static void validateAdditionalSettings(
        IndexSettingProvider provider,
        Settings additionalSettings,
        Settings.Builder allAdditionalSettings
    ) throws IllegalArgumentException {
        for (String settingName : additionalSettings.keySet()) {
            if (allAdditionalSettings.keys().contains(settingName)) {
                var name = provider.getClass().getSimpleName();
                var message = Strings.format("additional index setting [%s] added by [%s] is already present", settingName, name);
                throw new IllegalArgumentException(message);
            }
        }
    }

    private static void validateSoftDeleteSettings(Settings indexSettings) {
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings) == false
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(IndexVersions.V_8_0_0)) {
            throw new IllegalArgumentException(
                "Creating indices with soft-deletes disabled is no longer supported. "
                    + "Please do not specify a value for setting [index.soft_deletes.enabled]."
            );
        }
    }

    /**
     * Calculates the number of routing shards based on the configured value in indexSettings or if recovering from another index
     * it will return the value configured for that index.
     */
    static int getIndexNumberOfRoutingShards(Settings indexSettings, @Nullable IndexMetadata sourceMetadata) {
        final int numTargetShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        final IndexVersion indexVersionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        final int routingNumShards;
        if (sourceMetadata == null || sourceMetadata.getNumberOfShards() == 1) {
            // in this case we either have no index to recover from or
            // we have a source index with 1 shard and without an explicit split factor
            // or one that is valid in that case we can split into whatever and auto-generate a new factor.
            // (Don't use IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings) here, otherwise
            // we get the default value when `null` has been provided as value)
            if (indexSettings.get(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey()) != null) {
                routingNumShards = IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings);
            } else {
                routingNumShards = calculateNumRoutingShards(numTargetShards, indexVersionCreated);
            }
        } else {
            assert IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings) == false
                : "index.number_of_routing_shards should not be present on the target index on resize";
            routingNumShards = sourceMetadata.getRoutingNumShards();
        }
        return routingNumShards;
    }

    /**
     * Validate and resolve the aliases explicitly set for the index, together with the ones inherited from the specified
     * templates.
     *
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetadata#order()})
     *
     * @return the list of resolved aliases, with the explicitly provided aliases occurring first (having a higher priority) followed by
     * the ones inherited from the templates
     */
    public static List<AliasMetadata> resolveAndValidateAliases(
        String index,
        Set<Alias> aliases,
        List<Map<String, AliasMetadata>> templateAliases,
        Metadata metadata,
        NamedXContentRegistry xContentRegistry,
        SearchExecutionContext searchExecutionContext,
        Function<String, String> indexNameExpressionResolver,
        Predicate<String> systemNamePredicate
    ) {

        // Keep a separate set to facilitate searches when processing aliases from the template
        Set<Alias> resolvedExpressions = new HashSet<>();
        List<AliasMetadata> resolvedAliases = new ArrayList<>();
        for (Alias alias : aliases) {
            final String resolvedExpression = indexNameExpressionResolver.apply(alias.name());
            alias = alias.name(resolvedExpression);
            AliasValidator.validateAlias(alias, index, metadata);
            if (Strings.hasLength(alias.filter())) {
                AliasValidator.validateAliasFilter(alias.name(), alias.filter(), searchExecutionContext, xContentRegistry);
            }

            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name())
                .filter(alias.filter())
                .indexRouting(alias.indexRouting())
                .searchRouting(alias.searchRouting())
                .writeIndex(alias.writeIndex())
                .isHidden(systemNamePredicate.test(alias.name()) ? Boolean.TRUE : alias.isHidden())
                .build();
            resolvedAliases.add(aliasMetadata);
            resolvedExpressions.add(new Alias(resolvedExpression));
        }

        Map<String, AliasMetadata> templatesAliases = new HashMap<>();
        for (Map<String, AliasMetadata> templateAliasConfig : templateAliases) {
            // handle aliases
            for (Map.Entry<String, AliasMetadata> entry : templateAliasConfig.entrySet()) {
                String resolvedTemplateExpression = indexNameExpressionResolver.apply(entry.getValue().alias());
                AliasMetadata aliasMetadata = AliasMetadata.newAliasMetadata(entry.getValue(), resolvedTemplateExpression);

                // if an alias with same name came with the create index request itself,
                // ignore this one taken from the index template
                if (resolvedExpressions.contains(new Alias(aliasMetadata.alias()))) {
                    continue;
                }
                // if an alias with same name was already processed, ignore this one
                if (templatesAliases.containsKey(entry.getKey())) {
                    continue;
                }

                // Allow templatesAliases to be templated by replacing a token with the
                // name of the index that we are applying it to
                if (aliasMetadata.alias().contains("{index}")) {
                    String templatedAlias = aliasMetadata.alias().replace("{index}", index);
                    aliasMetadata = AliasMetadata.newAliasMetadata(aliasMetadata, templatedAlias);
                }

                // set system aliases from templates to hidden
                if (systemNamePredicate.test(aliasMetadata.alias())) {
                    aliasMetadata = AliasMetadata.builder(aliasMetadata.alias())
                        .filter(aliasMetadata.filter())
                        .indexRouting(aliasMetadata.indexRouting())
                        .searchRouting(aliasMetadata.searchRouting())
                        .writeIndex(aliasMetadata.writeIndex())
                        .isHidden(true)
                        .build();
                }

                AliasValidator.validateAliasMetadata(aliasMetadata, index, metadata);
                if (aliasMetadata.filter() != null) {
                    AliasValidator.validateAliasFilter(
                        aliasMetadata.alias(),
                        aliasMetadata.filter().uncompressed(),
                        searchExecutionContext,
                        xContentRegistry
                    );
                }
                templatesAliases.put(aliasMetadata.alias(), aliasMetadata);
                resolvedAliases.add((aliasMetadata));
            }
        }
        return resolvedAliases;

    }

    /**
     * Creates the index into the cluster state applying the provided blocks. The final cluster state will contain an updated routing
     * table based on the live nodes.
     */
    static ClusterState clusterStateCreateIndex(
        ClusterState currentState,
        IndexMetadata indexMetadata,
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy
    ) {
        final Metadata newMetadata;
        if (metadataTransformer != null) {
            Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(indexMetadata, false);
            metadataTransformer.accept(builder, indexMetadata);
            newMetadata = builder.build();
        } else {
            newMetadata = currentState.metadata().withAddedIndex(indexMetadata);
        }

        var blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        blocksBuilder.updateBlocks(indexMetadata);

        var routingTableBuilder = RoutingTable.builder(shardRoutingRoleStrategy, currentState.routingTable())
            .addAsNew(newMetadata.index(indexMetadata.getIndex().getName()));

        return ClusterState.builder(currentState).blocks(blocksBuilder).metadata(newMetadata).routingTable(routingTableBuilder).build();
    }

    static IndexMetadata buildIndexMetadata(
        String indexName,
        List<AliasMetadata> aliases,
        Supplier<DocumentMapper> documentMapperSupplier,
        Settings indexSettings,
        int routingNumShards,
        @Nullable IndexMetadata sourceMetadata,
        boolean isSystem,
        TransportVersion minClusterTransportVersion
    ) {
        IndexMetadata.Builder indexMetadataBuilder = createIndexMetadataBuilder(indexName, sourceMetadata, indexSettings, routingNumShards);
        indexMetadataBuilder.system(isSystem);
        if (minClusterTransportVersion.before(TransportVersions.V_8_15_0)) {
            // promote to UNKNOWN for older versions since they don't know how to handle event.ingested in cluster state
            indexMetadataBuilder.eventIngestedRange(IndexLongFieldRange.UNKNOWN, minClusterTransportVersion);
        }
        // now, update the mappings with the actual source
        Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();
        DocumentMapper docMapper = documentMapperSupplier.get();
        if (docMapper != null) {
            MappingMetadata mappingMd = new MappingMetadata(docMapper);
            mappingsMetadata.put(docMapper.type(), mappingMd);
            indexMetadataBuilder.putInferenceFields(docMapper.mappers().inferenceFields());
        }

        for (MappingMetadata mappingMd : mappingsMetadata.values()) {
            indexMetadataBuilder.putMapping(mappingMd);
        }

        // apply the aliases in reverse order as the lower index ones have higher order
        for (int i = aliases.size() - 1; i >= 0; i--) {
            indexMetadataBuilder.putAlias(aliases.get(i));
        }

        indexMetadataBuilder.state(IndexMetadata.State.OPEN);
        return indexMetadataBuilder.build();
    }

    /**
     * Creates an {@link IndexMetadata.Builder} for the provided index and sets a valid primary term for all the shards if a source
     * index meta data is provided (this represents the case where we're shrinking/splitting an index and the primary term for the newly
     * created index needs to be gte than the maximum term in the source index).
     */
    private static IndexMetadata.Builder createIndexMetadataBuilder(
        String indexName,
        @Nullable IndexMetadata sourceMetadata,
        Settings indexSettings,
        int routingNumShards
    ) {
        final IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        builder.setRoutingNumShards(routingNumShards);
        builder.settings(indexSettings);

        if (sourceMetadata != null) {
            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm = IntStream.range(0, sourceMetadata.getNumberOfShards())
                .mapToLong(sourceMetadata::primaryTerm)
                .max()
                .getAsLong();
            for (int shardId = 0; shardId < builder.numberOfShards(); shardId++) {
                builder.primaryTerm(shardId, primaryTerm);
            }
        }
        return builder;
    }

    private static void updateIndexMappingsAndBuildSortOrder(
        IndexService indexService,
        CreateIndexClusterStateUpdateRequest request,
        List<CompressedXContent> mappings,
        @Nullable IndexMetadata sourceMetadata
    ) throws IOException {
        MapperService mapperService = indexService.mapperService();
        IndexMode indexMode = indexService.getIndexSettings() != null ? indexService.getIndexSettings().getMode() : IndexMode.STANDARD;
        List<CompressedXContent> allMappings = new ArrayList<>();
        final CompressedXContent defaultMapping = indexMode.getDefaultMapping(indexService.getIndexSettings());
        if (defaultMapping != null) {
            allMappings.add(defaultMapping);
        }
        allMappings.addAll(mappings);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, allMappings, MergeReason.INDEX_TEMPLATE);

        indexMode.validateTimestampFieldMapping(request.dataStreamName() != null, mapperService.mappingLookup());

        if (sourceMetadata == null) {
            // now that the mapping is merged we can validate the index sort.
            // we cannot validate for index shrinking since the mapping is empty
            // at this point. The validation will take place later in the process
            // (when all shards are copied in a single place).
            indexService.getIndexSortSupplier().get();
        }
    }

    private static void validateActiveShardCount(ActiveShardCount waitForActiveShards, IndexMetadata indexMetadata) {
        if (waitForActiveShards == ActiveShardCount.DEFAULT) {
            waitForActiveShards = indexMetadata.getWaitForActiveShards();
        }
        if (waitForActiveShards.validate(indexMetadata.getNumberOfReplicas()) == false) {
            throw new IllegalArgumentException(
                "invalid wait_for_active_shards["
                    + waitForActiveShards
                    + "]: cannot be greater than number of shard copies ["
                    + (indexMetadata.getNumberOfReplicas() + 1)
                    + "]"
            );
        }
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, Metadata metadata, RoutingTable routingTable) {
        validateIndexName(request.index(), metadata, routingTable);
        validateIndexSettings(request.index(), request.settings(), forbidPrivateIndexSettings);
    }

    public void validateIndexSettings(String indexName, final Settings settings, final boolean forbidPrivateIndexSettings)
        throws IndexCreationException {
        List<String> validationErrors = getIndexSettingsValidationErrors(settings, forbidPrivateIndexSettings);

        if (validationErrors.isEmpty() == false) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new IndexCreationException(indexName, validationException);
        }
    }

    List<String> getIndexSettingsValidationErrors(final Settings settings, final boolean forbidPrivateIndexSettings) {
        List<String> validationErrors = validateIndexCustomPath(settings, env.sharedDataFile());
        if (forbidPrivateIndexSettings) {
            validationErrors.addAll(validatePrivateSettingsNotExplicitlySet(settings, indexScopedSettings));
        }
        return validationErrors;
    }

    private static List<String> validatePrivateSettingsNotExplicitlySet(Settings settings, IndexScopedSettings indexScopedSettings) {
        List<String> validationErrors = new ArrayList<>();
        for (final String key : settings.keySet()) {
            final Setting<?> setting = indexScopedSettings.get(key);
            if (setting == null) {
                assert indexScopedSettings.isPrivateSetting(key) : "expected [" + key + "] to be private but it was not";
            } else if (setting.isPrivateIndex()) {
                validationErrors.add("private index setting [" + key + "] can not be set explicitly");
            }
        }
        return validationErrors;
    }

    /**
     * Validates that the configured index data path (if any) is a sub-path of the configured shared data path (if any)
     *
     * @param settings the index configured settings
     * @param sharedDataPath the configured `path.shared_data` (if any)
     * @return a list containing validaton errors or an empty list if there aren't any errors
     */
    private static List<String> validateIndexCustomPath(Settings settings, @Nullable Path sharedDataPath) {
        String customPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
        List<String> validationErrors = new ArrayList<>();
        if (Strings.isEmpty(customPath) == false) {
            if (sharedDataPath == null) {
                validationErrors.add("path.shared_data must be set in order to use custom data paths");
            } else {
                Path resolvedPath = PathUtils.get(new Path[] { sharedDataPath }, customPath);
                if (resolvedPath == null) {
                    validationErrors.add("custom path [" + customPath + "] is not a sub-path of path.shared_data [" + sharedDataPath + "]");
                }
            }
        }
        return validationErrors;
    }

    /**
     * Validates the settings and mappings for shrinking an index.
     *
     * @return the list of nodes at least one instance of the source index shards are allocated
     */
    static List<String> validateShrinkIndex(
        Metadata metadata,
        ClusterBlocks clusterBlocks,
        RoutingTable routingTable,
        String sourceIndex,
        String targetIndexName,
        Settings targetIndexSettings
    ) {
        IndexMetadata sourceMetadata = validateResize(metadata, clusterBlocks, sourceIndex, targetIndexName, targetIndexSettings);
        if (sourceMetadata.isSearchableSnapshot()) {
            throw new IllegalArgumentException("can't shrink searchable snapshot index [" + sourceIndex + ']');
        }
        assert INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings);
        IndexMetadata.selectShrinkShards(0, sourceMetadata, INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));

        if (sourceMetadata.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }

        // now check that index is all on one node
        final IndexRoutingTable table = routingTable.index(sourceIndex);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = sourceMetadata.getNumberOfShards();
        for (ShardRouting routing : table.shardsWithState(ShardRoutingState.STARTED)) {
            nodesToNumRouting.computeIfAbsent(routing.currentNodeId(), (s) -> new AtomicInteger(0)).incrementAndGet();
        }
        List<String> nodesToAllocateOn = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entries : nodesToNumRouting.entrySet()) {
            int numAllocations = entries.getValue().get();
            assert numAllocations <= numShards : "wait what? " + numAllocations + " is > than num shards " + numShards;
            if (numAllocations == numShards) {
                nodesToAllocateOn.add(entries.getKey());
            }
        }
        if (nodesToAllocateOn.isEmpty()) {
            throw new IllegalStateException("index " + sourceIndex + " must have all shards allocated on the same node to shrink index");
        }
        return nodesToAllocateOn;
    }

    static void validateSplitIndex(
        Metadata metadata,
        ClusterBlocks clusterBlocks,
        String sourceIndex,
        String targetIndexName,
        Settings targetIndexSettings
    ) {
        IndexMetadata sourceMetadata = validateResize(metadata, clusterBlocks, sourceIndex, targetIndexName, targetIndexSettings);
        if (sourceMetadata.isSearchableSnapshot()) {
            throw new IllegalArgumentException("can't split searchable snapshot index [" + sourceIndex + ']');
        }
        IndexMetadata.selectSplitShard(0, sourceMetadata, INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static void validateCloneIndex(
        Metadata metadata,
        ClusterBlocks clusterBlocks,
        String sourceIndex,
        String targetIndexName,
        Settings targetIndexSettings
    ) {
        IndexMetadata sourceMetadata = validateResize(metadata, clusterBlocks, sourceIndex, targetIndexName, targetIndexSettings);
        if (sourceMetadata.isSearchableSnapshot()) {
            for (Setting<?> nonCloneableSetting : Arrays.asList(INDEX_STORE_TYPE_SETTING, INDEX_RECOVERY_TYPE_SETTING)) {
                if (nonCloneableSetting.exists(targetIndexSettings) == false) {
                    throw new IllegalArgumentException(
                        "can't clone searchable snapshot index ["
                            + sourceIndex
                            + "]; setting ["
                            + nonCloneableSetting.getKey()
                            + "] should be overridden"
                    );
                }
            }
        }
        IndexMetadata.selectCloneShard(0, sourceMetadata, INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetadata validateResize(
        Metadata metadata,
        ClusterBlocks clusterBlocks,
        String sourceIndex,
        String targetIndexName,
        Settings targetIndexSettings
    ) {
        if (metadata.hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(metadata.index(targetIndexName).getIndex());
        }
        final IndexMetadata sourceMetadata = metadata.index(sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(sourceIndex);
        }

        IndexAbstraction source = metadata.getIndicesLookup().get(sourceIndex);
        assert source != null;
        if (source.getParentDataStream() != null && source.getParentDataStream().getWriteIndex().equals(sourceMetadata.getIndex())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "cannot resize the write index [%s] for data stream [%s]",
                    sourceIndex,
                    source.getParentDataStream().getName()
                )
            );
        }
        // ensure index is read-only
        if (clusterBlocks.indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException("index " + sourceIndex + " must be read-only to resize index. use \"index.blocks.write=true\"");
        }

        if (INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetadata.getRoutingFactor(sourceMetadata.getNumberOfShards(), INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
        }
        return sourceMetadata;
    }

    static void prepareResizeIndexSettings(
        final Metadata metadata,
        final ClusterBlocks clusterBlocks,
        final RoutingTable routingTable,
        final Settings.Builder indexSettingsBuilder,
        final Index resizeSourceIndex,
        final String resizeIntoName,
        final ResizeType type,
        final boolean copySettings,
        final IndexScopedSettings indexScopedSettings
    ) {
        final IndexMetadata sourceMetadata = metadata.index(resizeSourceIndex.getName());
        if (type == ResizeType.SHRINK) {
            final List<String> nodesToAllocateOn = validateShrinkIndex(
                metadata,
                clusterBlocks,
                routingTable,
                resizeSourceIndex.getName(),
                resizeIntoName,
                indexSettingsBuilder.build()
            );
            indexSettingsBuilder.put(INDEX_SHRINK_INITIAL_RECOVERY_KEY, Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()));
        } else if (type == ResizeType.SPLIT) {
            validateSplitIndex(metadata, clusterBlocks, resizeSourceIndex.getName(), resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(INDEX_SHRINK_INITIAL_RECOVERY_KEY);
        } else if (type == ResizeType.CLONE) {
            validateCloneIndex(metadata, clusterBlocks, resizeSourceIndex.getName(), resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(INDEX_SHRINK_INITIAL_RECOVERY_KEY);
        } else {
            throw new IllegalStateException("unknown resize type is " + type);
        }

        final Settings.Builder builder = Settings.builder();
        if (copySettings) {
            // copy all settings and non-copyable settings and settings that have already been set (e.g., from the request)
            for (final String key : sourceMetadata.getSettings().keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key) : key;
                } else if (setting.getProperties().contains(Setting.Property.NotCopyableOnResize)) {
                    continue;
                }
                // do not override settings that have already been set (for example, from the request)
                if (indexSettingsBuilder.keys().contains(key)) {
                    continue;
                }
                builder.copy(key, sourceMetadata.getSettings());
            }
        } else {
            final Predicate<String> sourceSettingsPredicate = (s) -> (s.startsWith("index.similarity.")
                || s.startsWith("index.analysis.")
                || s.startsWith("index.sort.")
                || s.equals("index.soft_deletes.enabled")) && indexSettingsBuilder.keys().contains(s) == false;
            builder.put(sourceMetadata.getSettings().filter(sourceSettingsPredicate));
        }

        indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, sourceMetadata.getCreationVersion())
            .put(builder.build())
            .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, sourceMetadata.getRoutingPartitionSize())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
        if (sourceMetadata.getSettings().hasValue(IndexMetadata.SETTING_VERSION_COMPATIBILITY)) {
            indexSettingsBuilder.put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, sourceMetadata.getCompatibilityVersion());
        }
    }

    /**
     * Returns a default number of routing shards based on the number of shards of the index. The default number of routing shards will
     * allow any index to be split at least once and at most 10 times by a factor of two. The closer the number or shards gets to 1024
     * the less default split operations are supported
     */
    public static int calculateNumRoutingShards(int numShards, IndexVersion indexVersionCreated) {
        if (indexVersionCreated.onOrAfter(IndexVersions.V_7_0_0)) {
            // only select this automatically for indices that are created on or after 7.0 this will prevent this new behaviour
            // until we have a fully upgraded cluster. Additionally it will make integrating testing easier since mixed clusters
            // will always have the behavior of the min node in the cluster.
            //
            // We use as a default number of routing shards the higher number that can be expressed
            // as {@code numShards * 2^x`} that is less than or equal to the maximum number of shards: 1024.
            int log2MaxNumShards = 10; // logBase2(1024)
            int log2NumShards = 32 - Integer.numberOfLeadingZeros(numShards - 1); // ceil(logBase2(numShards))
            int numSplits = log2MaxNumShards - log2NumShards;
            numSplits = Math.max(1, numSplits); // Ensure the index can be split at least once
            return numShards * 1 << numSplits;
        } else {
            return numShards;
        }
    }

    public static void validateTranslogRetentionSettings(Settings indexSettings) {
        if (IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(IndexVersions.V_8_0_0)
            && (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings))) {
            throw new IllegalArgumentException(
                "Translog retention settings [index.translog.retention.age] "
                    + "and [index.translog.retention.size] are no longer supported. Please do not specify values for these settings"
            );
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings)
            && (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings))) {
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "translog_retention",
                "Translog retention settings [index.translog.retention.age] and [index.translog.retention.size] are deprecated and "
                    + "effectively ignored. They will be removed in a future version."
            );
        }
    }

    public static void validateStoreTypeSetting(Settings indexSettings) {
        final String storeType = IndexModule.INDEX_STORE_TYPE_SETTING.get(indexSettings);
        if (IndexModule.Type.SIMPLEFS.match(storeType)) {
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "store_type_setting",
                "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                    + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type as it offers superior "
                    + "or equivalent performance to [simplefs]."
            );
        }
    }
}
