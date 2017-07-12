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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.CreateIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData.Custom;
import org.elasticsearch.cluster.metadata.IndexMetaData.State;
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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Collections;
import java.util.Set;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.stream.IntStream;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;

/**
 * Service responsible for submitting create index requests
 */
public class MetaDataCreateIndexService extends AbstractComponent {

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final AliasValidator aliasValidator;
    private final Environment env;
    private final IndexScopedSettings indexScopedSettings;
    private final ActiveShardsObserver activeShardsObserver;
    private final NamedXContentRegistry xContentRegistry;
    private final ThreadPool threadPool;

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

        IndexCreationTask(Logger logger, AllocationService allocationService, CreateIndexClusterStateUpdateRequest request,
                          ActionListener<ClusterStateUpdateResponse> listener, IndicesService indicesService,
                          AliasValidator aliasValidator, NamedXContentRegistry xContentRegistry,
                          Settings settings, IndexValidator validator) {
            super(Priority.URGENT, request, listener);
            this.request = request;
            this.logger = logger;
            this.allocationService = allocationService;
            this.indicesService = indicesService;
            this.aliasValidator = aliasValidator;
            this.xContentRegistry = xContentRegistry;
            this.settings = settings;
            this.validator = validator;
        }

        @Override
        protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
            return new ClusterStateUpdateResponse(acknowledged);
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            Index createdIndex = null;
            String removalExtraInfo = null;
            IndexRemovalReason removalReason = IndexRemovalReason.FAILURE;
            try {
                validator.validate(request, currentState);
                request.aliases().forEach(a -> aliasValidator.validateAlias(a, request.index(), currentState.metaData()));

                final Index shrinkFromIndex = request.shrinkFrom();
                final Map<String, Custom> customs = new HashMap<>(request.customs());
                // add the request mapping
                final Map<String, Map<String, Object>> mappings = getRequestMappings();
                // we only find a template when its an API call (a new index)
                // find templates, highest order are better matching
                final List<IndexTemplateMetaData> templates = findTemplates(request, currentState);
                final List<String> templateNames = new ArrayList<>();
                final Map<String, AliasMetaData> templatesAliases = new HashMap<>();

                // apply templates, merging the mappings into the request mapping if exists
                if (shrinkFromIndex == null) {
                    fillFromTemplates(currentState, customs, mappings, templateNames, templatesAliases, templates);
                }

                final Settings.Builder indexSettingsBuilder = getIndexSettingsBuilder(request, currentState, templates,
                    shrinkFromIndex != null);

                if (shrinkFromIndex != null) {
                    prepareShrinkIndexSettings(currentState, mappings.keySet(), indexSettingsBuilder, shrinkFromIndex, request.index());
                }

                final Settings actualIndexSettings = indexSettingsBuilder.build();
                final int routingNumShards = getRoutingShardNum(shrinkFromIndex, currentState, actualIndexSettings);
                final IndexMetaData.Builder tmpImdBuilder = getTmpIndexMetaDataBuilder(actualIndexSettings, routingNumShards);

                if (shrinkFromIndex != null) {
                    applyPrimaryTermFromSource(currentState, shrinkFromIndex, tmpImdBuilder);
                }

                // Set up everything, now locally create the index to see that things are ok, and apply
                final IndexMetaData tmpImd = tmpImdBuilder.build();

                validateWaitForActiveShardsValue(tmpImd);

                // create the index here (on the master) to validate it can be created, as well as adding the mapping
                final IndexService indexService = indicesService.createIndex(tmpImd, Collections.emptyList());
                createdIndex = indexService.index();

                // now add the mappings
                final MapperService mapperService = indexService.mapperService();

                try {
                    mapperService.merge(mappings, MergeReason.MAPPING_UPDATE, request.updateAllTypes());
                } catch (Exception e) {
                    removalExtraInfo = "failed on parsing default mapping/mappings on index creation";
                    throw e;
                }

                if (request.shrinkFrom() == null) {
                    // now that the mapping is merged we can validate the index sort.
                    // we cannot validate for index shrinking since the mapping is empty
                    // at this point. The validation will take place later in the process
                    // (when all shards are copied in a single place).
                    validateIndexSort(indexService);
                }

                final Set<AliasMetaData> aliasesMetaData = getAliasMetaData(templatesAliases, indexService);
                // now, update the mappings with the actual source
                final Map<String, MappingMetaData> mappingsMetaData = getMappingsMetaData(mapperService);
                final IndexMetaData.Builder indexMetaDataBuilder = createMetaDataBuilder(request.index(), actualIndexSettings,
                    routingNumShards, mappingsMetaData, request.state(), aliasesMetaData, customs, tmpImd);

                final IndexMetaData indexMetaData;
                try {
                    indexMetaData = indexMetaDataBuilder.build();
                } catch (Exception e) {
                    removalExtraInfo = "failed to build index metadata";
                    throw e;
                }

                indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetaData.getIndex(), indexMetaData.getSettings());

                final MetaData newMetaData = MetaData.builder(currentState.metaData())
                    .put(indexMetaData, false)
                    .build();

                logger.info("[{}] creating index, cause [{}], templates {}, shards [{}]/[{}], mappings {}",
                    request.index(), request.cause(), templateNames, indexMetaData.getNumberOfShards(),
                    indexMetaData.getNumberOfReplicas(), mappings.keySet());

                final ClusterBlocks.Builder blocks = getClusterBlocksBuilder(currentState, indexMetaData);

                ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

                if (request.state() == State.OPEN) {
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                        .addAsNew(updatedState.metaData().index(request.index()));
                    updatedState = allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                        "index [" + request.index() + "] created");
                }
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

        private void validateWaitForActiveShardsValue(IndexMetaData tmpImd) {
            if (!getWaitForActiveShards(tmpImd).validate(tmpImd.getNumberOfReplicas())) {
                throw new IllegalArgumentException("invalid wait_for_active_shards[" + request.waitForActiveShards() +
                    "]: cannot be greater than number of shard copies [" +
                    (tmpImd.getNumberOfReplicas() + 1) + "]");
            }
        }

        private IndexMetaData.Builder getTmpIndexMetaDataBuilder(Settings actualIndexSettings, int routingNumShards) {
            return IndexMetaData.builder(request.index()).setRoutingNumShards(routingNumShards).settings(actualIndexSettings);
        }

        private ClusterBlocks.Builder getClusterBlocksBuilder(ClusterState currentState, IndexMetaData indexMetaData) {
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            request.blocks().forEach(block -> blocks.addIndexBlock(request.index(), block));
            blocks.updateBlocks(indexMetaData);
            return blocks;
        }

        private Map<String, Map<String, Object>> getRequestMappings() throws Exception {
            final Map<String, Map<String, Object>> mappings = new HashMap<>();
            for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                mappings.put(entry.getKey(), MapperService.parseMapping(xContentRegistry, entry.getValue()));
            }
            return mappings;
        }

        private Map<String, MappingMetaData> getMappingsMetaData(MapperService mapperService) {
            return StreamSupport.stream(mapperService.docMappers(true).spliterator(), false)
                .collect(toMap(DocumentMapper::type, MappingMetaData::new));
        }

        private ActiveShardCount getWaitForActiveShards(IndexMetaData tmpImd) {
            if (request.waitForActiveShards() == ActiveShardCount.DEFAULT) {
                return tmpImd.getWaitForActiveShards();
            }
            return request.waitForActiveShards();
        }

        private Set<AliasMetaData> getAliasMetaData(Map<String, AliasMetaData> templatesAliases, IndexService indexService) {
            // the context is only used for validation so it's fine to pass fake values for the shard id and the current
            // timestamp
            final QueryShardContext queryShardContext = indexService.newQueryShardContext(0, null, () -> 0L);

            request
                .aliases()
                .stream()
                .filter(alias -> Strings.hasLength(alias.filter()))
                .forEach(alias -> aliasValidator.validateAliasFilter(alias.name(), alias.filter(),
                    queryShardContext, xContentRegistry)
                );

            templatesAliases
                .values()
                .stream()
                .filter(aliasMetaData -> aliasMetaData.filter() != null)
                .forEach(aliasMetaData -> aliasValidator.validateAliasFilter(aliasMetaData.alias(),
                    aliasMetaData.filter().uncompressed(), queryShardContext, xContentRegistry)
                );

            final Set<AliasMetaData> aliasesMetaData = aliasesToMetaData(request.aliases());
            aliasesMetaData.addAll(templatesAliases.values());
            return aliasesMetaData;
        }

        private void applyPrimaryTermFromSource(ClusterState currentState, Index shrinkFromIndex, IndexMetaData.Builder tmpImdBuilder) {
            /*
            * We need to arrange that the primary term on all the shards in the shrunken index is at least as large as
            * the maximum primary term on all the shards in the source index. This ensures that we have correct
            * document-level semantics regarding sequence numbers in the shrunken index.
            */
            final IndexMetaData sourceMetaData = currentState.metaData().getIndexSafe(shrinkFromIndex);
            final long primaryTerm = IntStream
                                            .range(0, sourceMetaData.getNumberOfShards())
                                            .mapToLong(sourceMetaData::primaryTerm)
                                            .max()
                                            .getAsLong();
            for (int shardId = 0; shardId < tmpImdBuilder.numberOfShards(); shardId++) {
                tmpImdBuilder.primaryTerm(shardId, primaryTerm);
            }
        }

        private void validateIndexSort(IndexService indexService) {
            indexService.getIndexSortSupplier().get();
        }

        private void fillFromTemplates(ClusterState currentState, Map<String, Custom> customs, Map<String, Map<String, Object>> mappings,
                                         List<String> templateNames, Map<String, AliasMetaData> templatesAliases,
                                         List<IndexTemplateMetaData> templates) throws Exception {
            for (IndexTemplateMetaData template : templates) {
                templateNames.add(template.getName());
                fillMappingsFromTemplate(mappings, template);
                fillCustomsFromTemplate(customs, template);
                fillAliasesFromTemplate(currentState, templatesAliases, template);
            }
        }

        private int getRoutingShardNum(Index shrinkFromIndex, ClusterState currentState, Settings settings) {
            if (shrinkFromIndex != null) {
                return currentState.metaData().getIndexSafe(shrinkFromIndex).getRoutingNumShards();
            }
            return IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        }

        private void fillAliasesFromTemplate(ClusterState currentState, Map<String, AliasMetaData> templatesAliases,
                                             IndexTemplateMetaData template) {
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

                //Allow templatesAliases to be templated by replacing a token with the name of the index that we are applying it to
                if (aliasMetaData.alias().contains("{index}")) {
                    String templatedAlias = aliasMetaData.alias().replace("{index}", request.index());
                    aliasMetaData = AliasMetaData.newAliasMetaData(aliasMetaData, templatedAlias);
                }

                aliasValidator.validateAliasMetaData(aliasMetaData, request.index(), currentState.metaData());
                templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
            }
        }

        private void fillCustomsFromTemplate(Map<String, Custom> customs, IndexTemplateMetaData template) {
            for (ObjectObjectCursor<String, Custom> cursor : template.customs()) {
                String type = cursor.key;
                Custom custom = cursor.value;
                Custom existing = customs.get(type);
                if (existing == null) {
                    customs.put(type, custom);
                } else {
                    Custom merged = existing.mergeWith(custom);
                    customs.put(type, merged);
                }
            }
        }

        private void fillMappingsFromTemplate(Map<String, Map<String, Object>> mappings, IndexTemplateMetaData template) throws Exception {
            for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                String mappingString = cursor.value.string();
                if (mappings.containsKey(cursor.key)) {
                    XContentHelper.mergeDefaults(mappings.get(cursor.key),
                        MapperService.parseMapping(xContentRegistry, mappingString));
                } else {
                    mappings.put(cursor.key,
                        MapperService.parseMapping(xContentRegistry, mappingString));
                }
            }
        }

        private static Set<AliasMetaData> aliasesToMetaData(Set<Alias> aliases) {
            return aliases
                .stream()
                .map(alias -> AliasMetaData
                                        .builder(alias.name())
                                        .filter(alias.filter())
                                        .indexRouting(alias.indexRouting())
                                        .searchRouting(alias.searchRouting()).build())
                .collect(toSet());
        }

        private IndexMetaData.Builder createMetaDataBuilder(String index, Settings actualIndexSettings, int routingNumShards,
                                                            Map<String, MappingMetaData> mappingsMetaData, State state,
                                                            Set<AliasMetaData> aliasesMetaData, Map<String, Custom> customs,
                                                            IndexMetaData tmpImd) {
            final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData
                .builder(index)
                .settings(actualIndexSettings)
                .setRoutingNumShards(routingNumShards);

            for (int shardId = 0; shardId < tmpImd.getNumberOfShards(); shardId++) {
                indexMetaDataBuilder.primaryTerm(shardId, tmpImd.primaryTerm(shardId));
            }

            mappingsMetaData.values().forEach(indexMetaDataBuilder::putMapping);
            aliasesMetaData.forEach(indexMetaDataBuilder::putAlias);
            customs.forEach(indexMetaDataBuilder::putCustom);
            indexMetaDataBuilder.state(state);

            return indexMetaDataBuilder;
        }

        private static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
            final Iterable<T> iterable = () -> sourceIterator;
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        private List<IndexTemplateMetaData> findTemplates(CreateIndexClusterStateUpdateRequest request,
                                                          ClusterState state) throws IOException {
            return asStream(state.metaData().templates().values().iterator())
                .map(cursor -> cursor.value)
                .filter(metadata -> metadata.patterns().stream().anyMatch(template -> Regex.simpleMatch(template, request.index())))
                .sorted(comparingInt(IndexTemplateMetaData::order).reversed()) // timsort is default in JDK8
                .collect(toList());
        }

        private Settings.Builder getIndexSettingsBuilder(CreateIndexClusterStateUpdateRequest request, ClusterState currentState,
                                                         List<IndexTemplateMetaData> templates, boolean isShrinking) {
            Settings.Builder indexSettingsBuilder = Settings.builder();

            if (!isShrinking) {
                // apply templates, here, in reverse order, since first ones are better matching
                for (int i = templates.size() - 1; i >= 0; i--) {
                    indexSettingsBuilder.put(templates.get(i).settings());
                }
            }
            // now, put the request settings, so they override templates
            indexSettingsBuilder.put(request.settings());
            applyDefaultSettings(request, currentState, indexSettingsBuilder);

            return indexSettingsBuilder;
        }

        private void applyDefaultSettings(CreateIndexClusterStateUpdateRequest request, ClusterState currentState,
                                          Settings.Builder indexSettingsBuilder) {
            if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
            }
            if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
            }
            if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
                indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
            }

            if (indexSettingsBuilder.get(SETTING_VERSION_CREATED) == null) {
                final DiscoveryNodes nodes = currentState.nodes();
                final Version createdVersion = Version.min(Version.CURRENT, nodes.getSmallestNonClientNodeVersion());
                indexSettingsBuilder.put(SETTING_VERSION_CREATED, createdVersion);
            }

            if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
                indexSettingsBuilder.put(SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
            }
            indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, request.getProvidedName());
            indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
        }

        @Override
        public void onFailure(String source, Exception e) {
            if (e instanceof ResourceAlreadyExistsException) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
            } else {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to create", request.index()), e);
            }
            super.onFailure(source, e);
        }
    }

    @Inject
    public MetaDataCreateIndexService(Settings settings, ClusterService clusterService,
                                      IndicesService indicesService, AllocationService allocationService,
                                      AliasValidator aliasValidator, Environment env,
                                      IndexScopedSettings indexScopedSettings, ThreadPool threadPool,
                                      NamedXContentRegistry xContentRegistry) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.aliasValidator = aliasValidator;
        this.env = env;
        this.indexScopedSettings = indexScopedSettings;
        this.activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
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
     * ({@link CreateIndexClusterStateUpdateResponse#isShardsAcked()} will also be false).  If the index
     * creation in the cluster state was successful and the requisite shard copies were started before
     * the timeout, then {@link CreateIndexClusterStateUpdateResponse#isShardsAcked()} will
     * return true, otherwise if the operation timed out, then it will return false.
     *
     * @param request the index creation cluster state update request
     * @param listener the listener on which to send the index creation cluster state update response
     */
    public void createIndex(final CreateIndexClusterStateUpdateRequest request,
                            final ActionListener<CreateIndexClusterStateUpdateResponse> listener) {
        onlyCreateIndex(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                activeShardsObserver.waitForActiveShards(request.index(), request.waitForActiveShards(), request.ackTimeout(),
                    shardsAcked -> {
                        if (shardsAcked == false) {
                            logger.debug("[{}] index created, but the operation timed out while waiting for " +
                                             "enough shards to be started.", request.index());
                        }
                        listener.onResponse(new CreateIndexClusterStateUpdateResponse(response.isAcknowledged(), shardsAcked));
                    }, listener::onFailure);
            } else {
                listener.onResponse(new CreateIndexClusterStateUpdateResponse(false, false));
            }
        }, listener::onFailure));
    }

    private void onlyCreateIndex(final CreateIndexClusterStateUpdateRequest request,
                                 final ActionListener<ClusterStateUpdateResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(updatedSettingsBuilder);
        request.settings(updatedSettingsBuilder.build());

        clusterService.submitStateUpdateTask("create-index [" + request.index() + "], cause [" + request.cause() + "]",
                new IndexCreationTask(logger, allocationService, request, listener,
                    indicesService, aliasValidator, xContentRegistry, settings,
                    (req, state) -> {
                        validateIndexName(req.index(), state);
                        validateIndexSettings(req.index(), req.settings());
                    }
                )
        );
    }

    public void validateIndexSettings(String indexName, Settings settings) throws IndexCreationException {
        List<String> validationErrors = getIndexSettingsValidationErrors(settings);
        if (validationErrors.isEmpty() == false) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationErrors(validationErrors);
            throw new IndexCreationException(indexName, validationException);
        }
    }

    List<String> getIndexSettingsValidationErrors(Settings settings) {
        String customPath = IndexMetaData.INDEX_DATA_PATH_SETTING.get(settings);
        List<String> validationErrors = new ArrayList<>();
        if (Strings.isEmpty(customPath) == false && env.sharedDataFile() == null) {
            validationErrors.add("path.shared_data must be set in order to use custom data paths");
        } else if (Strings.isEmpty(customPath) == false) {
            Path resolvedPath = PathUtils.get(new Path[]{env.sharedDataFile()}, customPath);
            if (resolvedPath == null) {
                validationErrors.add(
                    "custom path [" + customPath + "] is not a sub-path of path.shared_data [" + env.sharedDataFile() + "]");
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
        if (state.metaData().hasIndex(targetIndexName)) {
            throw new ResourceAlreadyExistsException(state.metaData().index(targetIndexName).getIndex());
        }
        final IndexMetaData sourceMetaData = state.metaData().index(sourceIndex);
        if (sourceMetaData == null) {
            throw new IndexNotFoundException(sourceIndex);
        }
        // ensure index is read-only
        if (state.blocks().indexBlocked(ClusterBlockLevel.WRITE, sourceIndex) == false) {
            throw new IllegalStateException("index " + sourceIndex + " must be read-only to shrink index. use \"index.blocks.write=true\"");
        }

        if (sourceMetaData.getNumberOfShards() == 1) {
            throw new IllegalArgumentException("can't shrink an index with only one shard");
        }


        if ((targetIndexMappingsTypes.size() > 1 ||
            (targetIndexMappingsTypes.isEmpty() || targetIndexMappingsTypes.contains(MapperService.DEFAULT_MAPPING)) == false)) {
            throw new IllegalArgumentException("mappings are not allowed when shrinking indices" +
                ", all mappings are copied from the source index");
        }

        if (IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexSettings)) {
            // this method applies all necessary checks ie. if the target shards are less than the source shards
            // of if the source shards are divisible by the number of target shards
            IndexMetaData.getRoutingFactor(sourceMetaData, IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(targetIndexSettings));
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

    static void prepareShrinkIndexSettings(ClusterState currentState, Set<String> mappingKeys, Settings.Builder indexSettingsBuilder,
                                           Index shrinkFromIndex, String shrinkIntoName) {
        final IndexMetaData sourceMetaData = currentState.metaData().index(shrinkFromIndex.getName());

        final List<String> nodesToAllocateOn = validateShrinkIndex(currentState, shrinkFromIndex.getName(),
            mappingKeys, shrinkIntoName, indexSettingsBuilder.build());
        final Predicate<String> sourceSettingsPredicate = (s) -> s.startsWith("index.similarity.")
            || s.startsWith("index.analysis.") || s.startsWith("index.sort.");
        indexSettingsBuilder
            // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
            // once we are allocated.
            .put(IndexMetaData.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id",
                Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()))
            // we only try once and then give up with a shrink index
            .put("index.allocation.max_retries", 1)
            // now copy all similarity / analysis / sort settings - this overrides all settings from the user unless they
            // wanna add extra settings
            .put(IndexMetaData.SETTING_VERSION_CREATED, sourceMetaData.getCreationVersion())
            .put(IndexMetaData.SETTING_VERSION_UPGRADED, sourceMetaData.getUpgradedVersion())
            .put(sourceMetaData.getSettings().filter(sourceSettingsPredicate))
            .put(IndexMetaData.SETTING_ROUTING_PARTITION_SIZE, sourceMetaData.getRoutingPartitionSize())
            .put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME.getKey(), shrinkFromIndex.getName())
            .put(IndexMetaData.INDEX_SHRINK_SOURCE_UUID.getKey(), shrinkFromIndex.getUUID());
    }
}
