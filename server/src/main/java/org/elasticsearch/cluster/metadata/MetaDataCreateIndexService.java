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
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.IntStream;

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
                new IndexCreationTask(
                        logger,
                        allocationService,
                        request,
                        listener,
                        indicesService,
                        aliasValidator,
                        xContentRegistry,
                        settings,
                        this::validate,
                        indexScopedSettings));
    }

    interface IndexValidator {
        void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state);
    }

    static class IndexCreationTask extends AckedClusterStateUpdateTask<ClusterStateUpdateResponse> {

        private final IndicesService indicesService;
        private final AliasValidator aliasValidator;
        private final NamedXContentRegistry xContentRegistry;
        private final CreateIndexClusterStateUpdateRequest request;
        private final Logger logger;
        private final AllocationService allocationService;
        private final Settings settings;
        private final IndexValidator validator;
        private final IndexScopedSettings indexScopedSettings;

        IndexCreationTask(Logger logger, AllocationService allocationService, CreateIndexClusterStateUpdateRequest request,
                          ActionListener<ClusterStateUpdateResponse> listener, IndicesService indicesService,
                          AliasValidator aliasValidator, NamedXContentRegistry xContentRegistry,
                          Settings settings, IndexValidator validator, IndexScopedSettings indexScopedSettings) {
            super(Priority.URGENT, request, listener);
            this.request = request;
            this.logger = logger;
            this.allocationService = allocationService;
            this.indicesService = indicesService;
            this.aliasValidator = aliasValidator;
            this.xContentRegistry = xContentRegistry;
            this.settings = settings;
            this.validator = validator;
            this.indexScopedSettings = indexScopedSettings;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            logger.trace("executing IndexCreationTask for [{}] against cluster state version [{}]", request, currentState.version());
            Index createdIndex = null;
            String removalExtraInfo = null;
            IndexRemovalReason removalReason = IndexRemovalReason.FAILURE;
            try {
                validator.validate(request, currentState);

                for (Alias alias : request.aliases()) {
                    aliasValidator.validateAlias(alias, request.index(), currentState.metaData());
                }

                // we only find a template when its an API call (a new index)
                // find templates, highest order are better matching
                List<IndexTemplateMetaData> templates =
                        MetaDataIndexTemplateService.findTemplates(currentState.metaData(), request.index());

                // add the request mapping
                Map<String, Map<String, Object>> mappings = new HashMap<>();

                Map<String, AliasMetaData> templatesAliases = new HashMap<>();

                List<String> templateNames = new ArrayList<>();

                for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                    Map<String, Object> mapping = MapperService.parseMapping(xContentRegistry, entry.getValue());
                    assert mapping.size() == 1 : mapping;
                    assert entry.getKey().equals(mapping.keySet().iterator().next()) : entry.getKey() + " != " + mapping;
                    mappings.put(entry.getKey(), mapping);
                }

                final Index recoverFromIndex = request.recoverFrom();

                if (recoverFromIndex == null) {
                    // apply templates, merging the mappings into the request mapping if exists
                    for (IndexTemplateMetaData template : templates) {
                        templateNames.add(template.getName());
                        for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                            String mappingString = cursor.value.string();
                            if (mappings.containsKey(cursor.key)) {
                                XContentHelper.mergeDefaults(mappings.get(cursor.key),
                                    MapperService.parseMapping(xContentRegistry, mappingString));
                            } else if (mappings.size() == 1 && cursor.key.equals(MapperService.SINGLE_MAPPING_NAME)) {
                                // Typeless template with typed mapping
                                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mappingString);
                                assert templateMapping.size() == 1 : templateMapping;
                                assert cursor.key.equals(templateMapping.keySet().iterator().next()) :
                                    cursor.key + " != " + templateMapping;
                                Map.Entry<String, Map<String, Object>> mappingEntry = mappings.entrySet().iterator().next();
                                templateMapping = Collections.singletonMap(
                                        mappingEntry.getKey(),                       // reuse type name from the mapping
                                        templateMapping.values().iterator().next()); // but actual mappings from the template
                                XContentHelper.mergeDefaults(mappingEntry.getValue(), templateMapping);
                            } else if (template.mappings().size() == 1 && mappings.containsKey(MapperService.SINGLE_MAPPING_NAME)) {
                                // Typed template with typeless mapping
                                Map<String, Object> templateMapping = MapperService.parseMapping(xContentRegistry, mappingString);
                                assert templateMapping.size() == 1 : templateMapping;
                                assert cursor.key.equals(templateMapping.keySet().iterator().next()) :
                                    cursor.key + " != " + templateMapping;
                                Map<String, Object> mapping = mappings.get(MapperService.SINGLE_MAPPING_NAME);
                                templateMapping = Collections.singletonMap(
                                        MapperService.SINGLE_MAPPING_NAME,           // make template mapping typeless
                                        templateMapping.values().iterator().next());
                                XContentHelper.mergeDefaults(mapping, templateMapping);
                            } else {
                                mappings.put(cursor.key,
                                    MapperService.parseMapping(xContentRegistry, mappingString));
                            }
                        }
                        //handle aliases
                        for (ObjectObjectCursor<String, AliasMetaData> cursor : template.aliases()) {
                            AliasMetaData aliasMetaData = cursor.value;
                            //if an alias with same name came with the create index request itself,
                            // ignore this one taken from the index template
                            if (request.aliases().contains(new Alias(aliasMetaData.alias()))) {
                                continue;
                            }
                            //if an alias with same name was already processed, ignore this one
                            if (templatesAliases.containsKey(cursor.key)) {
                                continue;
                            }

                            // Allow templatesAliases to be templated by replacing a token with the
                            // name of the index that we are applying it to
                            if (aliasMetaData.alias().contains("{index}")) {
                                String templatedAlias = aliasMetaData.alias().replace("{index}", request.index());
                                aliasMetaData = AliasMetaData.newAliasMetaData(aliasMetaData, templatedAlias);
                            }

                            aliasValidator.validateAliasMetaData(aliasMetaData, request.index(), currentState.metaData());
                            templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
                        }
                    }
                }
                Settings.Builder indexSettingsBuilder = Settings.builder();
                if (recoverFromIndex == null) {
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
                final IndexMetaData.Builder tmpImdBuilder = IndexMetaData.builder(request.index());
                final Settings idxSettings = indexSettingsBuilder.build();
                int numTargetShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(idxSettings);
                final int routingNumShards;
                final Version indexVersionCreated = IndexMetaData.SETTING_INDEX_VERSION_CREATED.get(idxSettings);
                final IndexMetaData sourceMetaData = recoverFromIndex == null ? null :
                    currentState.metaData().getIndexSafe(recoverFromIndex);
                if (sourceMetaData == null || sourceMetaData.getNumberOfShards() == 1) {
                    // in this case we either have no index to recover from or
                    // we have a source index with 1 shard and without an explicit split factor
                    // or one that is valid in that case we can split into whatever and auto-generate a new factor.
                    if (IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(idxSettings)) {
                        routingNumShards = IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(idxSettings);
                    } else {
                        routingNumShards = calculateNumRoutingShards(numTargetShards, indexVersionCreated);
                    }
                } else {
                    assert IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.exists(indexSettingsBuilder.build()) == false
                        : "index.number_of_routing_shards should not be present on the target index on resize";
                    routingNumShards = sourceMetaData.getRoutingNumShards();
                }
                // remove the setting it's temporary and is only relevant once we create the index
                indexSettingsBuilder.remove(IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey());
                tmpImdBuilder.setRoutingNumShards(routingNumShards);

                if (recoverFromIndex != null) {
                    assert request.resizeType() != null;
                    prepareResizeIndexSettings(
                            currentState,
                            mappings.keySet(),
                            indexSettingsBuilder,
                            recoverFromIndex,
                            request.index(),
                            request.resizeType(),
                            request.copySettings(),
                            indexScopedSettings);
                }
                final Settings actualIndexSettings = indexSettingsBuilder.build();

                /*
                 * We can not check the shard limit until we have applied templates, otherwise we do not know the actual number of shards
                 * that will be used to create this index.
                 */
                checkShardLimit(actualIndexSettings, currentState);

                tmpImdBuilder.settings(actualIndexSettings);

                if (recoverFromIndex != null) {
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
                    for (int shardId = 0; shardId < tmpImdBuilder.numberOfShards(); shardId++) {
                        tmpImdBuilder.primaryTerm(shardId, primaryTerm);
                    }
                }
                // Set up everything, now locally create the index to see that things are ok, and apply
                final IndexMetaData tmpImd = tmpImdBuilder.build();
                ActiveShardCount waitForActiveShards = request.waitForActiveShards();
                if (waitForActiveShards == ActiveShardCount.DEFAULT) {
                    waitForActiveShards = tmpImd.getWaitForActiveShards();
                }
                if (waitForActiveShards.validate(tmpImd.getNumberOfReplicas()) == false) {
                    throw new IllegalArgumentException("invalid wait_for_active_shards[" + request.waitForActiveShards() +
                        "]: cannot be greater than number of shard copies [" +
                        (tmpImd.getNumberOfReplicas() + 1) + "]");
                }
                // create the index here (on the master) to validate it can be created, as well as adding the mapping
                final IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList());
                createdIndex = indexService.index();
                // now add the mappings
                MapperService mapperService = indexService.mapperService();
                try {
                    mapperService.merge(mappings, MergeReason.MAPPING_UPDATE);
                } catch (Exception e) {
                    removalExtraInfo = "failed on parsing default mapping/mappings on index creation";
                    throw e;
                }

                if (request.recoverFrom() == null) {
                    // now that the mapping is merged we can validate the index sort.
                    // we cannot validate for index shrinking since the mapping is empty
                    // at this point. The validation will take place later in the process
                    // (when all shards are copied in a single place).
                    indexService.getIndexSortSupplier().get();
                }

                // the context is only used for validation so it's fine to pass fake values for the shard id and the current
                // timestamp
                final QueryShardContext queryShardContext =
                    indexService.newQueryShardContext(0, null, () -> 0L, null);

                for (Alias alias : request.aliases()) {
                    if (Strings.hasLength(alias.filter())) {
                        aliasValidator.validateAliasFilter(alias.name(), alias.filter(), queryShardContext, xContentRegistry);
                    }
                }
                for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                    if (aliasMetaData.filter() != null) {
                        aliasValidator.validateAliasFilter(aliasMetaData.alias(), aliasMetaData.filter().uncompressed(),
                            queryShardContext, xContentRegistry);
                    }
                }

                // now, update the mappings with the actual source
                Map<String, MappingMetaData> mappingsMetaData = new HashMap<>();
                for (DocumentMapper mapper : Arrays.asList(mapperService.documentMapper(),
                                                           mapperService.documentMapper(MapperService.DEFAULT_MAPPING))) {
                    if (mapper != null) {
                        MappingMetaData mappingMd = new MappingMetaData(mapper);
                        mappingsMetaData.put(mapper.type(), mappingMd);
                    }
                }

                final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(request.index())
                    .settings(actualIndexSettings)
                    .setRoutingNumShards(routingNumShards);

                for (int shardId = 0; shardId < tmpImd.getNumberOfShards(); shardId++) {
                    indexMetaDataBuilder.primaryTerm(shardId, tmpImd.primaryTerm(shardId));
                }

                for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                    indexMetaDataBuilder.putMapping(mappingMd);
                }

                for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                    indexMetaDataBuilder.putAlias(aliasMetaData);
                }
                for (Alias alias : request.aliases()) {
                    AliasMetaData aliasMetaData = AliasMetaData.builder(alias.name()).filter(alias.filter())
                        .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).writeIndex(alias.writeIndex()).build();
                    indexMetaDataBuilder.putAlias(aliasMetaData);
                }

                indexMetaDataBuilder.state(IndexMetaData.State.OPEN);

                final IndexMetaData indexMetaData;
                try {
                    indexMetaData = indexMetaDataBuilder.build();
                } catch (Exception e) {
                    removalExtraInfo = "failed to build index metadata";
                    throw e;
                }

                indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetaData.getIndex(),
                    indexMetaData.getSettings());

                MetaData newMetaData = MetaData.builder(currentState.metaData())
                    .put(indexMetaData, false)
                    .build();

                logger.info("[{}] creating index, cause [{}], templates {}, shards [{}]/[{}], mappings {}",
                    request.index(), request.cause(), templateNames, indexMetaData.getNumberOfShards(),
                    indexMetaData.getNumberOfReplicas(), mappings.keySet());

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                if (!request.blocks().isEmpty()) {
                    for (ClusterBlock block : request.blocks()) {
                        blocks.addIndexBlock(request.index(), block);
                    }
                }
                blocks.updateBlocks(indexMetaData);

                ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                    .addAsNew(updatedState.metaData().index(request.index()));
                updatedState = allocationService.reroute(
                    ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                    "index [" + request.index() + "] created");
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

        protected void checkShardLimit(final Settings settings, final ClusterState clusterState) {
            MetaDataCreateIndexService.checkShardLimit(settings, clusterState);
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
        String customPath = IndexMetaData.INDEX_DATA_PATH_SETTING.get(settings);
        List<String> validationErrors = new ArrayList<>();
        if (Strings.isEmpty(customPath) == false && env.sharedDataFile() == null) {
            validationErrors.add("path.shared_data must be set in order to use custom data paths");
        } else if (Strings.isEmpty(customPath) == false) {
            Path resolvedPath = PathUtils.get(new Path[]{env.sharedDataFile()}, customPath);
            if (resolvedPath == null) {
                validationErrors.add("custom path [" + customPath +
                                     "] is not a sub-path of path.shared_data [" + env.sharedDataFile() + "]");
            }
        }
        if (forbidPrivateIndexSettings) {
            for (final String key : settings.keySet()) {
                final Setting<?> setting = indexScopedSettings.get(key);
                if (setting == null) {
                    assert indexScopedSettings.isPrivateSetting(key);
                } else if (setting.isPrivateIndex()) {
                    validationErrors.add("private index setting [" + key + "] can not be set explicitly");
                }
            }
        }
        return validationErrors;
    }

    /**
     * Validates the settings and mappings for shrinking an index.
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

        if ((targetIndexMappingsTypes.size() > 1 ||
            (targetIndexMappingsTypes.isEmpty() || targetIndexMappingsTypes.contains(MapperService.DEFAULT_MAPPING)) == false)) {
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
            indexSettingsBuilder
                .put(initialRecoveryIdFilter, Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()))
                // we only try once and then give up with a shrink index
                .put("index.allocation.max_retries", 1);
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
