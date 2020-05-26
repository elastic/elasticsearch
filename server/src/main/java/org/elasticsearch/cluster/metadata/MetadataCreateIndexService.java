/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.CreateIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * Service responsible for submitting create index requests
 */
public class MetadataCreateIndexService {
    private static final Logger logger = LogManager.getLogger(MetadataCreateIndexService.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final AliasValidator aliasValidator;
    private final Environment env;
    private final IndexScopedSettings indexScopedSettings;
    private final ActiveShardsObserver activeShardsObserver;
    private final NamedXContentRegistry xContentRegistry;
    private final Collection<SystemIndexDescriptor> systemIndexDescriptors;
    private final boolean forbidPrivateIndexSettings;

    public MetadataCreateIndexService(
        final Settings settings,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final AliasValidator aliasValidator,
        final Environment env,
        final IndexScopedSettings indexScopedSettings,
        final ThreadPool threadPool,
        final NamedXContentRegistry xContentRegistry,
        final Collection<SystemIndexDescriptor> systemIndexDescriptors,
        final boolean forbidPrivateIndexSettings) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.aliasValidator = aliasValidator;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.xContentRegistry = xContentRegistry;
        this.systemIndexDescriptors = systemIndexDescriptors;
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     */
    public void validateIndexName(String index, ClusterState state) {
        validateIndexOrAliasName(index, InvalidIndexNameException::new);
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }

        // NOTE: dot-prefixed index names are validated after template application, not here

        if (state.routingTable().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metadata().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(index).getIndex());
        }
        if (state.metadata().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
    }

    /**
     * Validates (if this index has a dot-prefixed name) whether it follows the rules for dot-prefixed indices.
     * @param index The name of the index in question
     * @param state The current cluster state
     * @param isHidden Whether or not this is a hidden index
     */
    public void validateDotIndex(String index, ClusterState state, @Nullable Boolean isHidden) {
        if (index.charAt(0) == '.') {
            List<SystemIndexDescriptor> matchingDescriptors = systemIndexDescriptors.stream()
                .filter(descriptor -> descriptor.matchesIndexPattern(index))
                .collect(toList());
            if (matchingDescriptors.isEmpty() && (isHidden == null || isHidden == Boolean.FALSE)) {
                deprecationLogger.deprecatedAndMaybeLog("index_name_starts_with_dot",
                    "index name [{}] starts with a dot '.', in the next major version, index names " +
                    "starting with a dot are reserved for hidden indices and system indices", index);
            } else if (matchingDescriptors.size() > 1) {
                // This should be prevented by erroring on overlapping patterns at startup time, but is here just in case.
                StringBuilder errorMessage = new StringBuilder()
                    .append("index name [")
                    .append(index)
                    .append("] is claimed as a system index by multiple system index patterns: [")
                    .append(matchingDescriptors.stream()
                        .map(descriptor -> "pattern: [" + descriptor.getIndexPattern() +
                            "], description: [" + descriptor.getDescription() + "]").collect(Collectors.joining("; ")));
                // Throw AssertionError if assertions are enabled, or a regular exception otherwise:
                assert false : errorMessage.toString();
                throw new IllegalStateException(errorMessage.toString());
            }
        }
    }

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public static void validateIndexOrAliasName(String index, BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (!Strings.validFileName(index)) {
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
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of index name", e);
        }
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
     * the cluster state, then {@link CreateIndexClusterStateUpdateResponse#isAcknowledged()} will return
     * true, otherwise it will return false and no waiting will occur for started shards
     * ({@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will also be false).  If the index
     * creation in the cluster state was successful and the requisite shard copies were started before
     * the timeout, then {@link CreateIndexClusterStateUpdateResponse#isShardsAcknowledged()} will
     * return true, otherwise if the operation timed out, then it will return false.
     *
     * @param request the index creation cluster state update request
     * @param listener the listener on which to send the index creation cluster state update response
     */
    public void createIndex(final CreateIndexClusterStateUpdateRequest request,
                            final ActionListener<CreateIndexClusterStateUpdateResponse> listener) {
        logger.trace("createIndex[{}]", request);
        onlyCreateIndex(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                logger.trace("[{}] index creation acknowledged, waiting for active shards [{}]",
                    request.index(), request.waitForActiveShards());
                activeShardsObserver.waitForActiveShards(new String[]{request.index()}, request.waitForActiveShards(), request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug("[{}] index created, but the operation timed out while waiting for " +
                                             "enough shards to be started.", request.index());
                        } else {
                            logger.trace("[{}] index created and shards acknowledged", request.index());
                        }
                        listener.onResponse(new CreateIndexClusterStateUpdateResponse(response.isAcknowledged(), shardsAcknowledged));
                    }, listener::onFailure);
            } else {
                logger.trace("index creation not acknowledged for [{}]", request);
                listener.onResponse(new CreateIndexClusterStateUpdateResponse(false, false));
            }
        }, listener::onFailure));
    }

    private void onlyCreateIndex(final CreateIndexClusterStateUpdateRequest request,
                                 final ActionListener<ClusterStateUpdateResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        Settings build = updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        indexScopedSettings.validate(build, true); // we do validate here - index setting must be consistent
        request.settings(build);
        clusterService.submitStateUpdateTask(
            "create-index [" + request.index() + "], cause [" + request.cause() + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return applyCreateIndexRequest(currentState, request, false);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.trace(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
                    } else {
                        logger.debug(() -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
                    }
                    super.onFailure(source, e);
                }
            });
    }

    /**
     * Handles the cluster state transition to a version that reflects the {@link CreateIndexClusterStateUpdateRequest}.
     * All the requested changes are firstly validated before mutating the {@link ClusterState}.
     */
    public ClusterState applyCreateIndexRequest(ClusterState currentState, CreateIndexClusterStateUpdateRequest request, boolean silent,
                                                BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer) throws Exception {
        logger.trace("executing IndexCreationTask for [{}] against cluster state version [{}]", request, currentState.version());

        validate(request, currentState);

        final Index recoverFromIndex = request.recoverFrom();
        final IndexMetadata sourceMetadata = recoverFromIndex == null ? null : currentState.metadata().getIndexSafe(recoverFromIndex);

        if (sourceMetadata != null) {
            // If source metadata was provided, it means we're recovering from an existing index,
            // in which case templates don't apply, so create the index from the source metadata
            return applyCreateIndexRequestWithExistingMetadata(currentState, request, silent, sourceMetadata, metadataTransformer);
        } else {
            // Hidden indices apply templates slightly differently (ignoring wildcard '*'
            // templates), so we need to check to see if the request is creating a hidden index
            // prior to resolving which templates it matches
            final Boolean isHiddenFromRequest = IndexMetadata.INDEX_HIDDEN_SETTING.exists(request.settings()) ?
                IndexMetadata.INDEX_HIDDEN_SETTING.get(request.settings()) : null;

            // Check to see if a v2 template matched
            final String v2Template = MetadataIndexTemplateService.findV2Template(currentState.metadata(),
                request.index(), isHiddenFromRequest == null ? false : isHiddenFromRequest);

            if (v2Template != null) {
                // If a v2 template was found, it takes precedence over all v1 templates, so create
                // the index using that template and the request's specified settings
                return applyCreateIndexRequestWithV2Template(currentState, request, silent, v2Template, metadataTransformer);
            } else {
                // A v2 template wasn't found, check the v1 templates, in the event no templates are
                // found creation still works using the request's specified index settings
                final List<IndexTemplateMetadata> v1Templates = MetadataIndexTemplateService.findV1Templates(currentState.metadata(),
                    request.index(), isHiddenFromRequest);

                if (v1Templates.size() > 1) {
                    deprecationLogger.deprecatedAndMaybeLog("index_template_multiple_match", "index [{}] matches multiple v1 templates " +
                        "[{}], v2 index templates will only match a single index template", request.index(),
                        v1Templates.stream().map(IndexTemplateMetadata::name).sorted().collect(Collectors.joining(", ")));
                }

                return applyCreateIndexRequestWithV1Templates(currentState, request, silent, v1Templates, metadataTransformer);
            }
        }
    }

    public ClusterState applyCreateIndexRequest(ClusterState currentState, CreateIndexClusterStateUpdateRequest request,
                                                boolean silent) throws Exception {
        return applyCreateIndexRequest(currentState, request, silent, null);
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
     * @param mappings a map of mappings for the new index
     * @param aliasSupplier a function that takes the real {@link IndexService} and returns a list of {@link AliasMetadata} aliases
     * @param templatesApplied a list of the names of the templates applied, for logging
     * @param metadataTransformer if provided, a function that may alter cluster metadata in the same cluster state update that
     *                            creates the index
     * @return a new cluster state with the index added
     */
    private ClusterState applyCreateIndexWithTemporaryService(final ClusterState currentState,
                                                              final CreateIndexClusterStateUpdateRequest request,
                                                              final boolean silent,
                                                              final IndexMetadata sourceMetadata,
                                                              final IndexMetadata temporaryIndexMeta,
                                                              final Map<String, Object> mappings,
                                                              final Function<IndexService, List<AliasMetadata>> aliasSupplier,
                                                              final List<String> templatesApplied,
                                                              final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                        throws Exception {
        // create the index here (on the master) to validate it can be created, as well as adding the mapping
        return indicesService.<ClusterState, Exception>withTempIndexService(temporaryIndexMeta, indexService -> {
            try {
                updateIndexMappingsAndBuildSortOrder(indexService, mappings, sourceMetadata);
            } catch (Exception e) {
                logger.debug("failed on parsing mappings on index creation [{}]", request.index());
                throw e;
            }

            final List<AliasMetadata> aliases = aliasSupplier.apply(indexService);

            final IndexMetadata indexMetadata;
            try {
                indexMetadata = buildIndexMetadata(request.index(), aliases, indexService.mapperService()::documentMapper,
                    temporaryIndexMeta.getSettings(), temporaryIndexMeta.getRoutingNumShards(), sourceMetadata);
            } catch (Exception e) {
                logger.info("failed to build index metadata [{}]", request.index());
                throw e;
            }

            logger.log(silent ? Level.DEBUG : Level.INFO, "[{}] creating index, cause [{}], templates {}, shards [{}]/[{}], mappings {}",
                request.index(), request.cause(), templatesApplied, indexMetadata.getNumberOfShards(),
                indexMetadata.getNumberOfReplicas(), mappings.keySet());

            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetadata.getIndex(),
                indexMetadata.getSettings());
            return clusterStateCreateIndex(currentState, request.blocks(), indexMetadata, allocationService::reroute, metadataTransformer);
        });
    }

    /**
     * Given a state and index settings calculated after applying templates, validate metadata for
     * the new index, returning an {@link IndexMetadata} for the new index
     */
    private IndexMetadata buildAndValidateTemporaryIndexMetadata(final ClusterState currentState,
                                                                 final Settings aggregatedIndexSettings,
                                                                 final CreateIndexClusterStateUpdateRequest request,
                                                                 final int routingNumShards) {

        final boolean isHiddenAfterTemplates = IndexMetadata.INDEX_HIDDEN_SETTING.get(aggregatedIndexSettings);
        validateDotIndex(request.index(), currentState, isHiddenAfterTemplates);

        // remove the setting it's temporary and is only relevant once we create the index
        final Settings.Builder settingsBuilder = Settings.builder().put(aggregatedIndexSettings);
        settingsBuilder.remove(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
        final Settings indexSettings = settingsBuilder.build();

        final IndexMetadata.Builder tmpImdBuilder = IndexMetadata.builder(request.index());
        tmpImdBuilder.setRoutingNumShards(routingNumShards);
        tmpImdBuilder.settings(indexSettings);

        // Set up everything, now locally create the index to see that things are ok, and apply
        IndexMetadata tempMetadata = tmpImdBuilder.build();
        validateActiveShardCount(request.waitForActiveShards(), tempMetadata);

        return tempMetadata;
    }

    // TODO: this method can be removed in 9.0 because we will no longer use v1 templates to create indices (only v2 templates)
    private ClusterState applyCreateIndexRequestWithV1Templates(final ClusterState currentState,
                                                                final CreateIndexClusterStateUpdateRequest request,
                                                                final boolean silent,
                                                                final List<IndexTemplateMetadata> templates,
                                                                final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                        throws Exception {
        logger.debug("applying create index request using legacy templates {}",
            templates.stream().map(IndexTemplateMetadata::name).collect(Collectors.toList()));

        final Map<String, Object> mappings = Collections.unmodifiableMap(parseV1Mappings(request.mappings(),
            templates.stream().map(IndexTemplateMetadata::getMappings).collect(toList()), xContentRegistry));

        final Settings aggregatedIndexSettings =
            aggregateIndexSettings(currentState, request, MetadataIndexTemplateService.resolveSettings(templates), mappings,
                null, settings, indexScopedSettings);
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(currentState, aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(currentState, request, silent, null, tmpImd, mappings,
            indexService -> resolveAndValidateAliases(request.index(), request.aliases(),
                MetadataIndexTemplateService.resolveAliases(templates), currentState.metadata(), aliasValidator,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry, indexService.newQueryShardContext(0, null, () -> 0L, null)),
            templates.stream().map(IndexTemplateMetadata::getName).collect(toList()), metadataTransformer);
    }

    private ClusterState applyCreateIndexRequestWithV2Template(final ClusterState currentState,
                                                               final CreateIndexClusterStateUpdateRequest request,
                                                               final boolean silent,
                                                               final String templateName,
                                                               final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                    throws Exception {
        logger.debug("applying create index request using composable template [{}]", templateName);

        final Map<String, Object> mappings = resolveV2Mappings(request.mappings(), currentState, templateName, xContentRegistry);

        final Settings aggregatedIndexSettings =
            aggregateIndexSettings(currentState, request,
                MetadataIndexTemplateService.resolveSettings(currentState.metadata(), templateName),
                mappings, null, settings, indexScopedSettings);
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, null);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(currentState, aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(currentState, request, silent, null, tmpImd, mappings,
            indexService -> resolveAndValidateAliases(request.index(), request.aliases(),
                MetadataIndexTemplateService.resolveAliases(currentState.metadata(), templateName), currentState.metadata(), aliasValidator,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                xContentRegistry, indexService.newQueryShardContext(0, null, () -> 0L, null)),
            Collections.singletonList(templateName), metadataTransformer);
    }

    public static Map<String, Object> resolveV2Mappings(final String requestMappings,
                                                        final ClusterState currentState,
                                                        final String templateName,
                                                        final NamedXContentRegistry xContentRegistry) throws Exception {
        final Map<String, Object> mappings = Collections.unmodifiableMap(parseV2Mappings(requestMappings,
            MetadataIndexTemplateService.resolveMappings(currentState, templateName), xContentRegistry));
        return mappings;
    }

    private ClusterState applyCreateIndexRequestWithExistingMetadata(final ClusterState currentState,
                                                                     final CreateIndexClusterStateUpdateRequest request,
                                                                     final boolean silent,
                                                                     final IndexMetadata sourceMetadata,
                                                                     final BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer)
                                                                                            throws Exception {
        logger.info("applying create index request using existing index [{}] metadata", sourceMetadata.getIndex().getName());

        final Map<String, Object> mappings = Collections.unmodifiableMap(MapperService.parseMapping(xContentRegistry, request.mappings()));

        final Settings aggregatedIndexSettings =
            aggregateIndexSettings(currentState, request, Settings.EMPTY, mappings, sourceMetadata, settings, indexScopedSettings);
        final int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, sourceMetadata);
        IndexMetadata tmpImd = buildAndValidateTemporaryIndexMetadata(currentState, aggregatedIndexSettings, request, routingNumShards);

        return applyCreateIndexWithTemporaryService(currentState, request, silent, sourceMetadata, tmpImd, mappings,
            indexService -> resolveAndValidateAliases(request.index(), request.aliases(), Collections.emptyList(),
                currentState.metadata(), aliasValidator, xContentRegistry,
                // the context is only used for validation so it's fine to pass fake values for the
                // shard id and the current timestamp
                indexService.newQueryShardContext(0, null, () -> 0L, null)),
            List.of(), metadataTransformer);
    }

    /**
     * Parses the provided mappings json and the inheritable mappings from the templates (if any)
     * into a map.
     *
     * The template mappings are applied in the order they are encountered in the list, with the
     * caveat that mapping fields are only merged at the top-level, meaning that field settings are
     * not merged, instead they replace any previous field definition.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> parseV2Mappings(String mappingsJson, List<CompressedXContent> templateMappings,
                                               NamedXContentRegistry xContentRegistry) throws Exception {
        Map<String, Object> requestMappings = MapperService.parseMapping(xContentRegistry, mappingsJson);
        // apply templates, merging the mappings into the request mapping if exists
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> nonProperties = new HashMap<>();
        for (CompressedXContent mapping : templateMappings) {
            if (mapping != null) {
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mapping.string());
                if (templateMapping.isEmpty()) {
                    // Someone provided an empty '{}' for mappings, which is okay, but to avoid
                    // tripping the below assertion, we can safely ignore it
                    continue;
                }
                assert templateMapping.size() == 1 : "expected exactly one mapping value, got: " + templateMapping;
                if (templateMapping.get(MapperService.SINGLE_MAPPING_NAME) instanceof Map == false) {
                    throw new IllegalStateException("invalid mapping definition, expected a single map underneath [" +
                        MapperService.SINGLE_MAPPING_NAME + "] but it was: [" + templateMapping + "]");
                }

                Map<String, Object> innerTemplateMapping = (Map<String, Object>) templateMapping.get(MapperService.SINGLE_MAPPING_NAME);
                Map<String, Object> innerTemplateNonProperties = new HashMap<>(innerTemplateMapping);
                Map<String, Object> maybeProperties = (Map<String, Object>) innerTemplateNonProperties.remove("properties");

                nonProperties = removeDuplicatedDynamicTemplates(nonProperties, innerTemplateNonProperties);
                XContentHelper.mergeDefaults(innerTemplateNonProperties, nonProperties);
                nonProperties = innerTemplateNonProperties;

                if (maybeProperties != null) {
                    properties = mergeIgnoringDots(properties, maybeProperties);
                }
            }
        }

        if (requestMappings.get(MapperService.SINGLE_MAPPING_NAME) != null) {
            Map<String, Object> innerRequestMappings = (Map<String, Object>) requestMappings.get(MapperService.SINGLE_MAPPING_NAME);
            Map<String, Object> innerRequestNonProperties = new HashMap<>(innerRequestMappings);
            Map<String, Object> maybeRequestProperties = (Map<String, Object>) innerRequestNonProperties.remove("properties");

            nonProperties = removeDuplicatedDynamicTemplates(nonProperties, innerRequestMappings);
            XContentHelper.mergeDefaults(innerRequestNonProperties, nonProperties);
            nonProperties = innerRequestNonProperties;

            if (maybeRequestProperties != null) {
                properties = mergeIgnoringDots(properties, maybeRequestProperties);
            }
        }

        Map<String, Object> finalMappings = dedupDynamicTemplates(nonProperties);
        finalMappings.put("properties", properties);
        return Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME, finalMappings);
    }

    /**
     * Removes the already seen/processed dynamic templates from the previouslySeenMapping if they are defined (we're
     * identifying the dynamic templates based on the name only, *not* on the full definition) in the newMapping we are about to
     * process (and merge)
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> removeDuplicatedDynamicTemplates(Map<String, Object> previouslySeenMapping,
                                                                        Map<String, Object> newMapping) {
        Map<String, Object> result = new HashMap<>(previouslySeenMapping);
        List<Map<String, Object>> newDynamicTemplates = (List<Map<String, Object>>) newMapping.get("dynamic_templates");
        List<Map<String, Object>> previouslySeenDynamicTemplates =
            (List<Map<String, Object>>) previouslySeenMapping.get("dynamic_templates");

        List<Map<String, Object>> filteredDynamicTemplates = removeOverlapping(previouslySeenDynamicTemplates, newDynamicTemplates);

        // if we removed any mappings from the previously seen ones, we'll re-add them on merge time, see
        // {@link XContentHelper#mergeDefaults}, so update the result to contain the filtered ones
        if (filteredDynamicTemplates != previouslySeenDynamicTemplates) {
            result.put("dynamic_templates", filteredDynamicTemplates);
        }
        return result;
    }

    /**
     * Removes all the items from the first list that are already present in the second list
     *
     * Similar to {@link List#removeAll(Collection)} but the list parameters are not modified.
     *
     * This expects both list values to be Maps of size one and the "contains" operation that will determine if a value
     * from the second list is present in the first list (and be removed from the first list) is based on key name.
     *
     * eg.
     *      removeAll([ {"key1" : {}}, {"key2" : {}} ], [ {"key1" : {}}, {"key3" : {}} ])
     * Returns:
     *     [ {"key2" : {}} ]
     */
    private static List<Map<String, Object>> removeOverlapping(List<Map<String, Object>> first, List<Map<String, Object>> second) {
        if (first == null) {
            return first;
        } else {
            validateValuesAreMapsOfSizeOne(first);
        }

        if (second == null) {
            return first;
        } else {
            validateValuesAreMapsOfSizeOne(second);
        }

        Set<String> keys = second.stream()
            .map(value -> value.keySet().iterator().next())
            .collect(Collectors.toSet());

        return first.stream().filter(value -> keys.contains(value.keySet().iterator().next()) == false).collect(toList());
    }

    private static void validateValuesAreMapsOfSizeOne(List<Map<String, Object>> second) {
        for (Map<String, Object> map : second) {
            // all are in the form of [ {"key1" : {}}, {"key2" : {}} ]
            if (map.size() != 1) {
                throw new IllegalArgumentException("unexpected argument, expected maps with one key, but got " + map);
            }
        }
    }

    /**
     * Parses the `dynamic_templates` from the provided mappings, if any are configured, and returns a mappings map containing dynamic
     * templates with unique names.
     *
     * The later templates in the provided mapping's `dynamic_templates` array will override the templates with the same name defined
     * earlier in the `dynamic_templates` array.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> dedupDynamicTemplates(Map<String, Object> mappings) {
        Objects.requireNonNull(mappings, "deduping the dynamic templates a non-null mapping");
        Map<String, Object> results = new HashMap<>(mappings);
        List<Map<String, Object>> dynamicTemplates = (List<Map<String, Object>>) mappings.get("dynamic_templates");
        if (dynamicTemplates == null) {
            return results;
        }

        LinkedHashMap<String, Map<String, Object>> dedupedDynamicTemplates = new LinkedHashMap<>(dynamicTemplates.size(), 1f);
        for (Map<String, Object> dynamicTemplate : dynamicTemplates) {
            dedupedDynamicTemplates.put(dynamicTemplate.keySet().iterator().next(), dynamicTemplate);
        }

        results.put("dynamic_templates", new ArrayList<>(dedupedDynamicTemplates.values()));
        return results;
    }

    /**
     * Add the objects in the second map to the first, where the keys in the {@code second} map have
     * higher predecence and overwrite the keys in the {@code first} map. In the event of a key with
     * a dot in it (ie, "foo.bar"), the keys are treated as only the prefix counting towards
     * equality. If the {@code second} map has a key such as "foo", all keys starting from "foo." in
     * the {@code first} map are discarded.
     */
    static Map<String, Object> mergeIgnoringDots(Map<String, Object> first, Map<String, Object> second) {
        Objects.requireNonNull(first, "merging requires two non-null maps but the first map was null");
        Objects.requireNonNull(second, "merging requires two non-null maps but the second map was null");
        Map<String, Object> results = new HashMap<>(first);
        Set<String> prefixes = second.keySet().stream().map(MetadataCreateIndexService::prefix).collect(Collectors.toSet());
        results.keySet().removeIf(k -> prefixes.contains(prefix(k)));
        results.putAll(second);
        return results;
    }

    private static String prefix(String s) {
        return s.split("\\.", 2)[0];
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
    static Map<String, Object> parseV1Mappings(String mappingsJson, List<CompressedXContent> templateMappings,
                                               NamedXContentRegistry xContentRegistry) throws Exception {
        Map<String, Object> mappings = MapperService.parseMapping(xContentRegistry, mappingsJson);
        // apply templates, merging the mappings into the request mapping if exists
        for (CompressedXContent mapping : templateMappings) {
            if (mapping != null) {
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mapping.string());
                if (templateMapping.isEmpty()) {
                    // Someone provided an empty '{}' for mappings, which is okay, but to avoid
                    // tripping the below assertion, we can safely ignore it
                    continue;
                }
                assert templateMapping.size() == 1 : "expected exactly one mapping value, got: " + templateMapping;
                // pre-8x templates may have a wrapper type other than _doc, so we re-wrap things here
                templateMapping = Collections.singletonMap(MapperService.SINGLE_MAPPING_NAME,
                    templateMapping.values().iterator().next());
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
    static Settings aggregateIndexSettings(ClusterState currentState, CreateIndexClusterStateUpdateRequest request,
                                           Settings templateSettings, Map<String, Object> mappings,
                                           @Nullable IndexMetadata sourceMetadata, Settings settings,
                                           IndexScopedSettings indexScopedSettings) {
        Settings.Builder indexSettingsBuilder = Settings.builder();
        if (sourceMetadata == null) {
            indexSettingsBuilder.put(templateSettings);
        }
        // now, put the request settings, so they override templates
        indexSettingsBuilder.put(request.settings());
        if (indexSettingsBuilder.get(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
            final DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
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
                currentState,
                mappings.keySet(),
                indexSettingsBuilder,
                request.recoverFrom(),
                request.index(),
                request.resizeType(),
                request.copySettings(),
                indexScopedSettings);
        }

        Settings indexSettings = indexSettingsBuilder.build();
        /*
         * We can not check the shard limit until we have applied templates, otherwise we do not know the actual number of shards
         * that will be used to create this index.
         */
        MetadataCreateIndexService.checkShardLimit(indexSettings, currentState);
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings) == false
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(Version.V_8_0_0)) {
            throw new IllegalArgumentException("Creating indices with soft-deletes disabled is no longer supported. " +
                "Please do not specify a value for setting [index.soft_deletes.enabled].");
        }
        validateTranslogRetentionSettings(indexSettings);
        return indexSettings;
    }

    /**
     * Calculates the number of routing shards based on the configured value in indexSettings or if recovering from another index
     * it will return the value configured for that index.
     */
    static int getIndexNumberOfRoutingShards(Settings indexSettings, @Nullable IndexMetadata sourceMetadata) {
        final int numTargetShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        final Version indexVersionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        final int routingNumShards;
        if (sourceMetadata == null || sourceMetadata.getNumberOfShards() == 1) {
            // in this case we either have no index to recover from or
            // we have a source index with 1 shard and without an explicit split factor
            // or one that is valid in that case we can split into whatever and auto-generate a new factor.
            if (IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings)) {
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
    public static List<AliasMetadata> resolveAndValidateAliases(String index, Set<Alias> aliases,
                                                                List<Map<String, AliasMetadata>> templateAliases, Metadata metadata,
                                                                AliasValidator aliasValidator, NamedXContentRegistry xContentRegistry,
                                                                QueryShardContext queryShardContext) {
        List<AliasMetadata> resolvedAliases = new ArrayList<>();
        for (Alias alias : aliases) {
            aliasValidator.validateAlias(alias, index, metadata);
            if (Strings.hasLength(alias.filter())) {
                aliasValidator.validateAliasFilter(alias.name(), alias.filter(), queryShardContext, xContentRegistry);
            }
            AliasMetadata aliasMetadata = AliasMetadata.builder(alias.name()).filter(alias.filter())
                .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).writeIndex(alias.writeIndex())
                .isHidden(alias.isHidden()).build();
            resolvedAliases.add(aliasMetadata);
        }

        Map<String, AliasMetadata> templatesAliases = new HashMap<>();
        for (Map<String, AliasMetadata> templateAliasConfig : templateAliases) {
            // handle aliases
            for (Map.Entry<String, AliasMetadata> entry : templateAliasConfig.entrySet()) {
                AliasMetadata aliasMetadata = entry.getValue();
                // if an alias with same name came with the create index request itself,
                // ignore this one taken from the index template
                if (aliases.contains(new Alias(aliasMetadata.alias()))) {
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

                aliasValidator.validateAliasMetadata(aliasMetadata, index, metadata);
                if (aliasMetadata.filter() != null) {
                    aliasValidator.validateAliasFilter(aliasMetadata.alias(), aliasMetadata.filter().uncompressed(),
                        queryShardContext, xContentRegistry);
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
    static ClusterState clusterStateCreateIndex(ClusterState currentState, Set<ClusterBlock> clusterBlocks, IndexMetadata indexMetadata,
                                                BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable,
                                                BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer) {
        Metadata.Builder builder = Metadata.builder(currentState.metadata())
            .put(indexMetadata, false);
        if (metadataTransformer != null) {
            metadataTransformer.accept(builder, indexMetadata);
        }
        Metadata newMetadata = builder.build();

        String indexName = indexMetadata.getIndex().getName();
        ClusterBlocks.Builder blocks = createClusterBlocksBuilder(currentState, indexName, clusterBlocks);
        blocks.updateBlocks(indexMetadata);

        ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metadata(newMetadata).build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
            .addAsNew(updatedState.metadata().index(indexName));
        updatedState = ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build();
        return rerouteRoutingTable.apply(updatedState, "index [" + indexName + "] created");
    }

    static IndexMetadata buildIndexMetadata(String indexName, List<AliasMetadata> aliases,
                                            Supplier<DocumentMapper> documentMapperSupplier, Settings indexSettings, int routingNumShards,
                                            @Nullable IndexMetadata sourceMetadata) {
        IndexMetadata.Builder indexMetadataBuilder = createIndexMetadataBuilder(indexName, sourceMetadata, indexSettings, routingNumShards);
        // now, update the mappings with the actual source
        Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();
        DocumentMapper mapper = documentMapperSupplier.get();
        if (mapper != null) {
            MappingMetadata mappingMd = new MappingMetadata(mapper);
            mappingsMetadata.put(mapper.type(), mappingMd);
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
    private static IndexMetadata.Builder createIndexMetadataBuilder(String indexName, @Nullable IndexMetadata sourceMetadata,
                                                                    Settings indexSettings, int routingNumShards) {
        final IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
        builder.setRoutingNumShards(routingNumShards);
        builder.settings(indexSettings);

        if (sourceMetadata != null) {
            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm =
                IntStream
                    .range(0, sourceMetadata.getNumberOfShards())
                    .mapToLong(sourceMetadata::primaryTerm)
                    .max()
                    .getAsLong();
            for (int shardId = 0; shardId < builder.numberOfShards(); shardId++) {
                builder.primaryTerm(shardId, primaryTerm);
            }
        }
        return builder;
    }

    private static ClusterBlocks.Builder createClusterBlocksBuilder(ClusterState currentState, String index, Set<ClusterBlock> blocks) {
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        if (!blocks.isEmpty()) {
            for (ClusterBlock block : blocks) {
                blocksBuilder.addIndexBlock(index, block);
            }
        }
        return blocksBuilder;
    }

    private static void updateIndexMappingsAndBuildSortOrder(IndexService indexService, Map<String, Object> mappings,
                                                             @Nullable IndexMetadata sourceMetadata) throws IOException {
        MapperService mapperService = indexService.mapperService();
        if (!mappings.isEmpty()) {
            assert mappings.size() == 1 : mappings;
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappings, MergeReason.MAPPING_UPDATE);
        }

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
            throw new IllegalArgumentException("invalid wait_for_active_shards[" + waitForActiveShards +
                "]: cannot be greater than number of shard copies [" +
                (indexMetadata.getNumberOfReplicas() + 1) + "]");
        }
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        validateIndexName(request.index(), state);
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

    /**
     * Checks whether an index can be created without going over the cluster shard limit.
     *
     * @param settings     the settings of the index to be created
     * @param clusterState the current cluster state
     * @throws ValidationException if creating this index would put the cluster over the cluster shard limit
     */
    public static void checkShardLimit(final Settings settings, final ClusterState clusterState) {
        final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
        final int shardsToCreate = numberOfShards * (1 + numberOfReplicas);

        final Optional<String> shardLimit = IndicesService.checkShardLimit(shardsToCreate, clusterState);
        if (shardLimit.isPresent()) {
            final ValidationException e = new ValidationException();
            e.addValidationError(shardLimit.get());
            throw e;
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
        if (!Strings.isEmpty(customPath)) {
            if (sharedDataPath == null) {
                validationErrors.add("path.shared_data must be set in order to use custom data paths");
            } else {
                Path resolvedPath = PathUtils.get(new Path[]{sharedDataPath}, customPath);
                if (resolvedPath == null) {
                    validationErrors.add("custom path [" + customPath +
                        "] is not a sub-path of path.shared_data [" + sharedDataPath + "]");
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
    static List<String> validateShrinkIndex(ClusterState state, String sourceIndex,
                                            Set<String> targetIndexMappingsTypes, String targetIndexName,
                                            Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        assert INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings);
        IndexMetadata.selectShrinkShards(0, sourceMetadata, INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));

        if (sourceMetadata.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }

        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndex);
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
            throw new IllegalStateException("index " + sourceIndex +
                " must have all shards allocated on the same node to shrink index");
        }
        return nodesToAllocateOn;
    }

    static void validateSplitIndex(ClusterState state, String sourceIndex,
                                   Set<String> targetIndexMappingsTypes, String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetadata.selectSplitShard(0, sourceMetadata, INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static void validateCloneIndex(ClusterState state, String sourceIndex,
                                   Set<String> targetIndexMappingsTypes, String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetadata sourceMetadata = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetadata.selectCloneShard(0, sourceMetadata, INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetadata validateResize(ClusterState state, String sourceIndex,
                                        Set<String> targetIndexMappingsTypes, String targetIndexName,
                                        Settings targetIndexSettings) {
        if (state.metadata().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(state.metadata().index(targetIndexName).getIndex());
        }
        final IndexMetadata sourceMetadata = state.metadata().index(sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(sourceIndex);
        }
        // ensure index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException("index " + sourceIndex + " must be read-only to resize index. use \"index.blocks.write=true\"");
        }

        if (targetIndexMappingsTypes.size() > 0) {
            throw new IllegalArgumentException("mappings are not allowed when resizing indices" +
                ", all mappings are copied from the source index");
        }

        if (INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetadata.getRoutingFactor(sourceMetadata.getNumberOfShards(),
                INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
        }
        return sourceMetadata;
    }

    static void prepareResizeIndexSettings(
            final ClusterState currentState,
            final Set<String> mappingKeys,
            final Settings.Builder indexSettingsBuilder,
            final Index resizeSourceIndex,
            final String resizeIntoName,
            final ResizeType type,
            final boolean copySettings,
            final IndexScopedSettings indexScopedSettings) {

        // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
        // once we are allocated.
        final String initialRecoveryIdFilter = IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id";

        final IndexMetadata sourceMetadata = currentState.metadata().index(resizeSourceIndex.getName());
        if (type == ResizeType.SHRINK) {
            final List<String> nodesToAllocateOn = validateShrinkIndex(currentState, resizeSourceIndex.getName(),
                mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.put(initialRecoveryIdFilter, Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()));
        } else if (type == ResizeType.SPLIT) {
            validateSplitIndex(currentState, resizeSourceIndex.getName(), mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(initialRecoveryIdFilter);
        } else if (type == ResizeType.CLONE) {
            validateCloneIndex(currentState, resizeSourceIndex.getName(), mappingKeys, resizeIntoName, indexSettingsBuilder.build());
            indexSettingsBuilder.putNull(initialRecoveryIdFilter);
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
            final Predicate<String> sourceSettingsPredicate =
                    (s) -> (s.startsWith("index.similarity.") || s.startsWith("index.analysis.") ||
                            s.startsWith("index.sort.") || s.equals("index.soft_deletes.enabled"))
                            && indexSettingsBuilder.keys().contains(s) == false;
            builder.put(sourceMetadata.getSettings().filter(sourceSettingsPredicate));
        }

        indexSettingsBuilder
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), sourceMetadata.getCreationVersion())
            .put(IndexMetadata.SETTING_VERSION_UPGRADED, sourceMetadata.getUpgradedVersion())
            .put(builder.build())
            .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, sourceMetadata.getRoutingPartitionSize())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
            .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
    }

    /**
     * Returns a default number of routing shards based on the number of shards of the index. The default number of routing shards will
     * allow any index to be split at least once and at most 10 times by a factor of two. The closer the number or shards gets to 1024
     * the less default split operations are supported
     */
    public static int calculateNumRoutingShards(int numShards, Version indexVersionCreated) {
        if (indexVersionCreated.onOrAfter(Version.V_7_0_0)) {
            // only select this automatically for indices that are created on or after 7.0 this will prevent this new behaviour
            // until we have a fully upgraded cluster. Additionally it will make integratin testing easier since mixed clusters
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
        if (IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings).onOrAfter(Version.V_8_0_0) &&
            (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings))) {
            throw new IllegalArgumentException("Translog retention settings [index.translog.retention.age] " +
                "and [index.translog.retention.size] are no longer supported. Please do not specify values for these settings");
        }
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexSettings) &&
            (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(indexSettings)
                || IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(indexSettings))) {
            deprecationLogger.deprecatedAndMaybeLog("translog_retention", "Translog retention settings [index.translog.retention.age] "
                + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version.");
        }
    }
}
