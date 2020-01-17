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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
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
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

/**
 * Service responsible for submitting create index requests
 */
public class MetaDataCreateIndexService {
    private static final Logger logger = LogManager.getLogger(MetaDataCreateIndexService.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(logger);

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
    private final boolean forbidPrivateIndexSettings;

    public MetaDataCreateIndexService(
            final Settings settings,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final AllocationService allocationService,
            final AliasValidator aliasValidator,
            final Environment env,
            final IndexScopedSettings indexScopedSettings,
            final ThreadPool threadPool,
            final NamedXContentRegistry xContentRegistry,
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
        this.forbidPrivateIndexSettings = forbidPrivateIndexSettings;
    }

    /**
     * Validate the name for an index against some static rules and a cluster state.
     */
    public static void validateIndexName(String index, ClusterState state) {
        validateIndexOrAliasName(index, InvalidIndexNameException::new);
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }
        if (state.routingTable().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metaData().hasIndex(index)) {
            throw new ResourceAlreadyExistsException(state.metaData().index(index).getIndex());
        }
        if (state.metaData().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
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
        Settings build = updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
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
                    return applyCreateIndexRequest(currentState, request);
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
    public ClusterState applyCreateIndexRequest(ClusterState currentState, CreateIndexClusterStateUpdateRequest request) throws Exception {
        logger.trace("executing IndexCreationTask for [{}] against cluster state version [{}]", request, currentState.version());
        Index createdIndex = null;
        String removalExtraInfo = null;
        IndexRemovalReason removalReason = IndexRemovalReason.FAILURE;

        validate(request, currentState);

        final Index recoverFromIndex = request.recoverFrom();
        final IndexMetaData sourceMetaData = recoverFromIndex == null ? null : currentState.metaData().getIndexSafe(recoverFromIndex);

        // we only find a template when its an API call (a new index)
        // find templates, highest order are better matching
        final List<IndexTemplateMetaData> templates = sourceMetaData == null ?
            Collections.unmodifiableList(MetaDataIndexTemplateService.findTemplates(currentState.metaData(), request.index())) :
            List.of();

        final Map<String, Object> mappings = Collections.unmodifiableMap(parseMappings(request.mappings(), templates, xContentRegistry));

        final Settings aggregatedIndexSettings =
            aggregateIndexSettings(currentState, request, templates, mappings, sourceMetaData, settings, indexScopedSettings);
        int routingNumShards = getIndexNumberOfRoutingShards(aggregatedIndexSettings, sourceMetaData);

        // remove the setting it's temporary and is only relevant once we create the index
        final Settings.Builder settingsBuilder = Settings.builder().put(aggregatedIndexSettings);
        settingsBuilder.remove(IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
        final Settings indexSettings = settingsBuilder.build();

        try {
            final IndexService indexService = validateActiveShardCountAndCreateIndexService(request.index(), request.waitForActiveShards(),
                indexSettings, routingNumShards, indicesService);
            // create the index here (on the master) to validate it can be created, as well as adding the mapping
            createdIndex = indexService.index();

            try {
                updateIndexMappingsAndBuildSortOrder(indexService, mappings, sourceMetaData);
            } catch (Exception e) {
                removalExtraInfo = "failed on parsing mappings on index creation";
                throw e;
            }

            // the context is only used for validation so it's fine to pass fake values for the shard id and the current
            // timestamp
            final List<AliasMetaData> aliases = Collections.unmodifiableList(
                resolveAndValidateAliases(request.index(), request.aliases(), templates, currentState.metaData(), aliasValidator,
                    xContentRegistry, indexService.newQueryShardContext(0, null, () -> 0L, null))
            );

            final IndexMetaData indexMetaData;
            try {
                indexMetaData = buildIndexMetaData(request.index(), aliases, indexService.mapperService()::documentMapper, indexSettings,
                    routingNumShards, sourceMetaData);
            } catch (Exception e) {
                removalExtraInfo = "failed to build index metadata";
                throw e;
            }

            logger.info("[{}] creating index, cause [{}], templates {}, shards [{}]/[{}], mappings {}",
                request.index(), request.cause(), templates.stream().map(IndexTemplateMetaData::getName).collect(toList()),
                indexMetaData.getNumberOfShards(), indexMetaData.getNumberOfReplicas(), mappings.keySet());

            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetaData.getIndex(),
                indexMetaData.getSettings());
            final ClusterState updatedState = clusterStateCreateIndex(currentState, request.blocks(), indexMetaData,
                allocationService::reroute);

            removalExtraInfo = "cleaning up after validating index on master";
            removalReason = IndexRemovalReason.NO_LONGER_ASSIGNED;
            return updatedState;
        } finally {
            if (createdIndex != null) {
                // Index was already partially created - need to clean up
                indicesService.removeIndex(createdIndex, removalReason, removalExtraInfo);
            }
        }
    }

    /**
     * Parses the provided mappings json and the inheritable mappings from the templates (if any) into a map.
     *
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetaData#order()})
     */
    static Map<String, Object> parseMappings(String mappingsJson, List<IndexTemplateMetaData> templates,
                                             NamedXContentRegistry xContentRegistry) throws Exception {
        Map<String, Object> mappings = MapperService.parseMapping(xContentRegistry, mappingsJson);
        // apply templates, merging the mappings into the request mapping if exists
        for (IndexTemplateMetaData template : templates) {
            for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                String mappingString = cursor.value.string();
                // Templates are wrapped with their _type names, which for pre-8x templates may not
                // be _doc.  For now, we unwrap them based on the _type name, and then re-wrap with
                // _doc
                // TODO in 9x these will all have a _type of _doc so no re-wrapping will be necessary
                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mappingString);
                assert templateMapping.size() == 1 : templateMapping;
                assert cursor.key.equals(templateMapping.keySet().iterator().next()) : cursor.key + " != " + templateMapping;
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
     * to the head of the list, templates have the highest {@link IndexTemplateMetaData#order()})
     *
     * @return the aggregated settings for the new index
     */
    static Settings aggregateIndexSettings(ClusterState currentState, CreateIndexClusterStateUpdateRequest request,
                                           List<IndexTemplateMetaData> templates, Map<String, Object> mappings,
                                           @Nullable IndexMetaData sourceMetaData, Settings settings,
                                           IndexScopedSettings indexScopedSettings) {
        Settings.Builder indexSettingsBuilder = Settings.builder();
        if (sourceMetaData == null) {
            // apply templates, here, in reverse order, since first ones are better matching
            for (int i = templates.size() - 1; i >= 0; i--) {
                indexSettingsBuilder.put(templates.get(i).settings());
            }
        }
        // now, put the request settings, so they override templates
        indexSettingsBuilder.put(request.settings());
        if (indexSettingsBuilder.get(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey()) == null) {
            final DiscoveryNodes nodes = currentState.nodes();
            final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
            indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion);
        }
        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
        }
        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
        }
        if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
            indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
        }

        if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
            indexSettingsBuilder.put(SETTING_CREATION_DATE, Instant.now().toEpochMilli());
        }
        indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName());
        indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());

        if (sourceMetaData != null) {
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
        MetaDataCreateIndexService.checkShardLimit(indexSettings, currentState);
        if (indexSettings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            DEPRECATION_LOGGER.deprecatedAndMaybeLog("soft_deletes_disabled",
                "Creating indices with soft-deletes disabled is deprecated and will be removed in future Elasticsearch versions. " +
                "Please do not specify value for setting [index.soft_deletes.enabled] of index [" + request.index() + "].");
        }
        return indexSettings;
    }

    /**
     * Calculates the number of routing shards based on the configured value in indexSettings or if recovering from another index
     * it will return the value configured for that index.
     */
    static int getIndexNumberOfRoutingShards(Settings indexSettings, @Nullable IndexMetaData sourceMetaData) {
        final int numTargetShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        final Version indexVersionCreated = IndexMetaData.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        final int routingNumShards;
        if (sourceMetaData == null || sourceMetaData.getNumberOfShards() == 1) {
            // in this case we either have no index to recover from or
            // we have a source index with 1 shard and without an explicit split factor
            // or one that is valid in that case we can split into whatever and auto-generate a new factor.
            if (IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings)) {
                routingNumShards = IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexSettings);
            } else {
                routingNumShards = calculateNumRoutingShards(numTargetShards, indexVersionCreated);
            }
        } else {
            assert IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettings) == false
                : "index.number_of_routing_shards should not be present on the target index on resize";
            routingNumShards = sourceMetaData.getRoutingNumShards();
        }
        return routingNumShards;
    }

    /**
     * Validate and resolve the aliases explicitly set for the index, together with the ones inherited from the specified
     * templates.
     *
     * The template mappings are applied in the order they are encountered in the list (clients should make sure the lower index, closer
     * to the head of the list, templates have the highest {@link IndexTemplateMetaData#order()})
     *
     * @return the list of resolved aliases, with the explicitly provided aliases occurring first (having a higher priority) followed by
     * the ones inherited from the templates
     */
    static List<AliasMetaData> resolveAndValidateAliases(String index, Set<Alias> aliases, List<IndexTemplateMetaData> templates,
                                                         MetaData metaData, AliasValidator aliasValidator,
                                                         NamedXContentRegistry xContentRegistry, QueryShardContext queryShardContext) {
        List<AliasMetaData> resolvedAliases = new ArrayList<>();
        for (Alias alias : aliases) {
            aliasValidator.validateAlias(alias, index, metaData);
            if (Strings.hasLength(alias.filter())) {
                aliasValidator.validateAliasFilter(alias.name(), alias.filter(), queryShardContext, xContentRegistry);
            }
            AliasMetaData aliasMetaData = AliasMetaData.builder(alias.name()).filter(alias.filter())
                .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).writeIndex(alias.writeIndex()).build();
            resolvedAliases.add(aliasMetaData);
        }

        Map<String, AliasMetaData> templatesAliases = new HashMap<>();
        for (IndexTemplateMetaData template : templates) {
            // handle aliases
            for (ObjectObjectCursor<String, AliasMetaData> cursor : template.aliases()) {
                AliasMetaData aliasMetaData = cursor.value;
                // if an alias with same name came with the create index request itself,
                // ignore this one taken from the index template
                if (aliases.contains(new Alias(aliasMetaData.alias()))) {
                    continue;
                }
                // if an alias with same name was already processed, ignore this one
                if (templatesAliases.containsKey(cursor.key)) {
                    continue;
                }

                // Allow templatesAliases to be templated by replacing a token with the
                // name of the index that we are applying it to
                if (aliasMetaData.alias().contains("{index}")) {
                    String templatedAlias = aliasMetaData.alias().replace("{index}", index);
                    aliasMetaData = AliasMetaData.newAliasMetaData(aliasMetaData, templatedAlias);
                }

                aliasValidator.validateAliasMetaData(aliasMetaData, index, metaData);
                if (aliasMetaData.filter() != null) {
                    aliasValidator.validateAliasFilter(aliasMetaData.alias(), aliasMetaData.filter().uncompressed(),
                        queryShardContext, xContentRegistry);
                }
                templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
                resolvedAliases.add((aliasMetaData));
            }
        }
        return resolvedAliases;
    }

    /**
     * Creates the index into the cluster state applying the provided blocks. The final cluster state will contain an updated routing
     * table based on the live nodes.
     */
    static ClusterState clusterStateCreateIndex(ClusterState currentState, Set<ClusterBlock> clusterBlocks, IndexMetaData indexMetaData,
                                                BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable) {
        MetaData newMetaData = MetaData.builder(currentState.metaData())
            .put(indexMetaData, false)
            .build();

        String indexName = indexMetaData.getIndex().getName();
        ClusterBlocks.Builder blocks = createClusterBlocksBuilder(currentState, indexName, clusterBlocks);
        blocks.updateBlocks(indexMetaData);

        ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
            .addAsNew(updatedState.metaData().index(indexName));
        updatedState = ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build();
        return rerouteRoutingTable.apply(updatedState, "index [" + indexName + "] created");
    }

    static IndexMetaData buildIndexMetaData(String indexName, List<AliasMetaData> aliases,
                                            Supplier<DocumentMapper> documentMapperSupplier, Settings indexSettings, int routingNumShards,
                                            @Nullable IndexMetaData sourceMetaData) {
        IndexMetaData.Builder indexMetaDataBuilder = createIndexMetadataBuilder(indexName, sourceMetaData, indexSettings, routingNumShards);
        // now, update the mappings with the actual source
        Map<String, MappingMetaData> mappingsMetaData = new HashMap<>();
        DocumentMapper mapper = documentMapperSupplier.get();
        if (mapper != null) {
            MappingMetaData mappingMd = new MappingMetaData(mapper);
            mappingsMetaData.put(mapper.type(), mappingMd);
        }

        for (MappingMetaData mappingMd : mappingsMetaData.values()) {
            indexMetaDataBuilder.putMapping(mappingMd);
        }

        // apply the aliases in reverse order as the lower index ones have higher order
        for (int i = aliases.size() - 1; i >= 0; i--) {
            indexMetaDataBuilder.putAlias(aliases.get(i));
        }

        indexMetaDataBuilder.state(IndexMetaData.State.OPEN);
        return indexMetaDataBuilder.build();
    }

    /**
     * Creates an {@link IndexMetaData.Builder} for the provided index and sets a valid primary term for all the shards if a source
     * index meta data is provided (this represents the case where we're shrinking/splitting an index and the primary term for the newly
     * created index needs to be gte than the maximum term in the source index).
     */
    private static IndexMetaData.Builder createIndexMetadataBuilder(String indexName, @Nullable IndexMetaData sourceMetaData,
                                                                    Settings indexSettings, int routingNumShards) {
        final IndexMetaData.Builder builder = IndexMetaData.builder(indexName);
        builder.setRoutingNumShards(routingNumShards);
        builder.settings(indexSettings);

        if (sourceMetaData != null) {
            /*
             * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
             * the maximum primary term on all the shards in the source index. This ensures that we have correct
             * document-level semantics regarding sequence numbers in the shrunken index.
             */
            final long primaryTerm =
                IntStream
                    .range(0, sourceMetaData.getNumberOfShards())
                    .mapToLong(sourceMetaData::primaryTerm)
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
                                                             @Nullable IndexMetaData sourceMetaData) throws IOException {
        MapperService mapperService = indexService.mapperService();
        if (!mappings.isEmpty()) {
            assert mappings.size() == 1 : mappings;
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappings, MergeReason.MAPPING_UPDATE);
        }

        if (sourceMetaData == null) {
            // now that the mapping is merged we can validate the index sort.
            // we cannot validate for index shrinking since the mapping is empty
            // at this point. The validation will take place later in the process
            // (when all shards are copied in a single place).
            indexService.getIndexSortSupplier().get();
        }
    }

    private static IndexService validateActiveShardCountAndCreateIndexService(String indexName, ActiveShardCount waitForActiveShards,
                                                                              Settings indexSettings, int routingNumShards,
                                                                              IndicesService indicesService) throws IOException {
        final IndexMetaData.Builder tmpImdBuilder = IndexMetaData.builder(indexName);
        tmpImdBuilder.setRoutingNumShards(routingNumShards);
        tmpImdBuilder.settings(indexSettings);

        // Set up everything, now locally create the index to see that things are ok, and apply
        IndexMetaData tmpImd = tmpImdBuilder.build();
        if (waitForActiveShards == ActiveShardCount.DEFAULT) {
            waitForActiveShards = tmpImd.getWaitForActiveShards();
        }
        if (waitForActiveShards.validate(tmpImd.getNumberOfReplicas()) == false) {
            throw new IllegalArgumentException("invalid wait_for_active_shards[" + waitForActiveShards +
                "]: cannot be greater than number of shard copies [" +
                (tmpImd.getNumberOfReplicas() + 1) + "]");
        }
        return indicesService.createIndex(tmpImd, Collections.emptyList(), false);
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
        final int numberOfShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        final int numberOfReplicas = IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
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
                assert indexScopedSettings.isPrivateSetting(key);
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
        String customPath = IndexMetaData.INDEX_DATA_PATH_SETTING.get(settings);
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
        IndexMetaData sourceMetaData = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        assert IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings);
        IndexMetaData.selectShrinkShards(0, sourceMetaData, IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));

        if (sourceMetaData.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }

        // now check that index is all on one node
        final IndexRoutingTable table = state.routingTable().index(sourceIndex);
        Map<String, AtomicInteger> nodesToNumRouting = new HashMap<>();
        int numShards = sourceMetaData.getNumberOfShards();
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
        IndexMetaData sourceMetaData = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetaData.selectSplitShard(0, sourceMetaData, IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static void validateCloneIndex(ClusterState state, String sourceIndex,
                                   Set<String> targetIndexMappingsTypes, String targetIndexName,
                                   Settings targetIndexSettings) {
        IndexMetaData sourceMetaData = validateResize(state, sourceIndex, targetIndexMappingsTypes, targetIndexName, targetIndexSettings);
        IndexMetaData.selectCloneShard(0, sourceMetaData, IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
    }

    static IndexMetaData validateResize(ClusterState state, String sourceIndex,
                                        Set<String> targetIndexMappingsTypes, String targetIndexName,
                                        Settings targetIndexSettings) {
        if (state.metaData().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(state.metaData().index(targetIndexName).getIndex());
        }
        final IndexMetaData sourceMetaData = state.metaData().index(sourceIndex);
        if (sourceMetaData == null) {
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

        if (IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetaData.getRoutingFactor(sourceMetaData.getNumberOfShards(),
                IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
        }
        return sourceMetaData;
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
        final String initialRecoveryIdFilter = IndexMetaData.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id";

        final IndexMetaData sourceMetaData = currentState.metaData().index(resizeSourceIndex.getName());
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
            for (final String key : sourceMetaData.getSettings().keySet()) {
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
                builder.copy(key, sourceMetaData.getSettings());
            }
        } else {
            final Predicate<String> sourceSettingsPredicate =
                    (s) -> (s.startsWith("index.similarity.") || s.startsWith("index.analysis.") ||
                            s.startsWith("index.sort.") || s.equals("index.soft_deletes.enabled"))
                            && indexSettingsBuilder.keys().contains(s) == false;
            builder.put(sourceMetaData.getSettings().filter(sourceSettingsPredicate));
        }

        indexSettingsBuilder
            .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), sourceMetaData.getCreationVersion())
            .put(IndexMetaData.SETTING_VERSION_UPGRADED, sourceMetaData.getUpgradedVersion())
            .put(builder.build())
            .put(IndexMetaData.SETTING_ROUTING_PARTITION_SIZE, sourceMetaData.getRoutingPartitionSize())
            .put(IndexMetaData.INDEX_RESIZE_SOURCE_NAME.getKey(), resizeSourceIndex.getName())
            .put(IndexMetaData.INDEX_RESIZE_SOURCE_UUID.getKey(), resizeSourceIndex.getUUID());
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
}
