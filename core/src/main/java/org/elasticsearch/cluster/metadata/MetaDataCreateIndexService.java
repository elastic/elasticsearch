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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
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
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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

    public final static int MAX_INDEX_NAME_BYTES = 255;
    private static final DefaultIndexTemplateFilter DEFAULT_INDEX_TEMPLATE_FILTER = new DefaultIndexTemplateFilter();

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final Version version;
    private final AliasValidator aliasValidator;
    private final IndexTemplateFilter indexTemplateFilter;
    private final Environment env;
    private final NodeServicesProvider nodeServicesProvider;
    private final IndexScopedSettings indexScopedSettings;


    @Inject
    public MetaDataCreateIndexService(Settings settings, ClusterService clusterService,
                                      IndicesService indicesService, AllocationService allocationService,
                                      Version version, AliasValidator aliasValidator,
                                      Set<IndexTemplateFilter> indexTemplateFilters, Environment env, NodeServicesProvider nodeServicesProvider, IndexScopedSettings indexScopedSettings) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.version = version;
        this.aliasValidator = aliasValidator;
        this.env = env;
        this.nodeServicesProvider = nodeServicesProvider;
        this.indexScopedSettings = indexScopedSettings;

        if (indexTemplateFilters.isEmpty()) {
            this.indexTemplateFilter = DEFAULT_INDEX_TEMPLATE_FILTER;
        } else {
            IndexTemplateFilter[] templateFilters = new IndexTemplateFilter[indexTemplateFilters.size() + 1];
            templateFilters[0] = DEFAULT_INDEX_TEMPLATE_FILTER;
            int i = 1;
            for (IndexTemplateFilter indexTemplateFilter : indexTemplateFilters) {
                templateFilters[i++] = indexTemplateFilter;
            }
            this.indexTemplateFilter = new IndexTemplateFilter.Compound(templateFilters);
        }
    }

    public void validateIndexName(String index, ClusterState state) {
        if (state.routingTable().hasIndex(index)) {
            throw new IndexAlreadyExistsException(state.routingTable().index(index).getIndex());
        }
        if (state.metaData().hasIndex(index)) {
            throw new IndexAlreadyExistsException(state.metaData().index(index).getIndex());
        }
        if (!Strings.validFileName(index)) {
            throw new InvalidIndexNameException(index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.contains("#")) {
            throw new InvalidIndexNameException(index, "must not contain '#'");
        }
        if (index.charAt(0) == '_') {
            throw new InvalidIndexNameException(index, "must not start with '_'");
        }
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(index, "must be lowercase");
        }
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw new InvalidIndexNameException(index,
                    "index name is too long, (" + byteCount +
                            " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (state.metaData().hasAlias(index)) {
            throw new InvalidIndexNameException(index, "already exists as alias");
        }
        if (index.equals(".") || index.equals("..")) {
            throw new InvalidIndexNameException(index, "must not be '.' or '..'");
        }
    }

    public void createIndex(final CreateIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.builder();
        updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(updatedSettingsBuilder);
        request.settings(updatedSettingsBuilder.build());

        clusterService.submitStateUpdateTask("create-index [" + request.index() + "], cause [" + request.cause() + "]",
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
                    @Override
                    protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                        return new ClusterStateUpdateResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Index createdIndex = null;
                        String removalReason = null;
                        try {
                            validate(request, currentState);

                            for (Alias alias : request.aliases()) {
                                aliasValidator.validateAlias(alias, request.index(), currentState.metaData());
                            }

                            // we only find a template when its an API call (a new index)
                            // find templates, highest order are better matching
                            List<IndexTemplateMetaData> templates = findTemplates(request, currentState, indexTemplateFilter);

                            Map<String, Custom> customs = new HashMap<>();

                            // add the request mapping
                            Map<String, Map<String, Object>> mappings = new HashMap<>();

                            Map<String, AliasMetaData> templatesAliases = new HashMap<>();

                            List<String> templateNames = new ArrayList<>();

                            for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                                mappings.put(entry.getKey(), MapperService.parseMapping(entry.getValue()));
                            }

                            for (Map.Entry<String, Custom> entry : request.customs().entrySet()) {
                                customs.put(entry.getKey(), entry.getValue());
                            }

                            // apply templates, merging the mappings into the request mapping if exists
                            for (IndexTemplateMetaData template : templates) {
                                templateNames.add(template.getName());
                                for (ObjectObjectCursor<String, CompressedXContent> cursor : template.mappings()) {
                                    if (mappings.containsKey(cursor.key)) {
                                        XContentHelper.mergeDefaults(mappings.get(cursor.key), MapperService.parseMapping(cursor.value.string()));
                                    } else {
                                        mappings.put(cursor.key, MapperService.parseMapping(cursor.value.string()));
                                    }
                                }
                                // handle custom
                                for (ObjectObjectCursor<String, Custom> cursor : template.customs()) {
                                    String type = cursor.key;
                                    IndexMetaData.Custom custom = cursor.value;
                                    IndexMetaData.Custom existing = customs.get(type);
                                    if (existing == null) {
                                        customs.put(type, custom);
                                    } else {
                                        IndexMetaData.Custom merged = existing.mergeWith(custom);
                                        customs.put(type, merged);
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

                                    //Allow templatesAliases to be templated by replacing a token with the name of the index that we are applying it to
                                    if (aliasMetaData.alias().contains("{index}")) {
                                        String templatedAlias = aliasMetaData.alias().replace("{index}", request.index());
                                        aliasMetaData = AliasMetaData.newAliasMetaData(aliasMetaData, templatedAlias);
                                    }

                                    aliasValidator.validateAliasMetaData(aliasMetaData, request.index(), currentState.metaData());
                                    templatesAliases.put(aliasMetaData.alias(), aliasMetaData);
                                }
                            }
                            Settings.Builder indexSettingsBuilder = Settings.builder();
                            // apply templates, here, in reverse order, since first ones are better matching
                            for (int i = templates.size() - 1; i >= 0; i--) {
                                indexSettingsBuilder.put(templates.get(i).settings());
                            }
                            // now, put the request settings, so they override templates
                            indexSettingsBuilder.put(request.settings());
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
                                DiscoveryNodes nodes = currentState.nodes();
                                final Version createdVersion = Version.smallest(version, nodes.getSmallestNonClientNodeVersion());
                                indexSettingsBuilder.put(SETTING_VERSION_CREATED, createdVersion);
                            }

                            if (indexSettingsBuilder.get(SETTING_CREATION_DATE) == null) {
                                indexSettingsBuilder.put(SETTING_CREATION_DATE, new DateTime(DateTimeZone.UTC).getMillis());
                            }

                            indexSettingsBuilder.put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID());
                            final Index shrinkFromIndex = request.shrinkFrom();
                            int routingNumShards = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettingsBuilder.build());;
                            if (shrinkFromIndex != null) {
                                prepareShrinkIndexSettings(currentState, mappings.keySet(), indexSettingsBuilder, shrinkFromIndex,
                                    request.index());
                                IndexMetaData sourceMetaData = currentState.metaData().getIndexSafe(shrinkFromIndex);
                                routingNumShards = sourceMetaData.getRoutingNumShards();
                            }

                            Settings actualIndexSettings = indexSettingsBuilder.build();
                            IndexMetaData.Builder tmpImdBuilder = IndexMetaData.builder(request.index())
                                .setRoutingNumShards(routingNumShards);
                            // Set up everything, now locally create the index to see that things are ok, and apply
                            final IndexMetaData tmpImd = tmpImdBuilder.settings(actualIndexSettings).build();
                            // create the index here (on the master) to validate it can be created, as well as adding the mapping
                            final IndexService indexService = indicesService.createIndex(nodeServicesProvider, tmpImd, Collections.emptyList());
                            createdIndex = indexService.index();
                            // now add the mappings
                            MapperService mapperService = indexService.mapperService();
                            try {
                                mapperService.merge(mappings, request.updateAllTypes());
                            } catch (MapperParsingException mpe) {
                                removalReason = "failed on parsing default mapping/mappings on index creation";
                                throw mpe;
                            }

                            final QueryShardContext queryShardContext = indexService.newQueryShardContext();
                            for (Alias alias : request.aliases()) {
                                if (Strings.hasLength(alias.filter())) {
                                    aliasValidator.validateAliasFilter(alias.name(), alias.filter(), queryShardContext);
                                }
                            }
                            for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                                if (aliasMetaData.filter() != null) {
                                    aliasValidator.validateAliasFilter(aliasMetaData.alias(), aliasMetaData.filter().uncompressed(), queryShardContext);
                                }
                            }

                            // now, update the mappings with the actual source
                            Map<String, MappingMetaData> mappingsMetaData = new HashMap<>();
                            for (DocumentMapper mapper : mapperService.docMappers(true)) {
                                MappingMetaData mappingMd = new MappingMetaData(mapper);
                                mappingsMetaData.put(mapper.type(), mappingMd);
                            }

                            final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(request.index())
                                .settings(actualIndexSettings)
                                .setRoutingNumShards(routingNumShards);
                            for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                                indexMetaDataBuilder.putMapping(mappingMd);
                            }

                            for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                                indexMetaDataBuilder.putAlias(aliasMetaData);
                            }
                            for (Alias alias : request.aliases()) {
                                AliasMetaData aliasMetaData = AliasMetaData.builder(alias.name()).filter(alias.filter())
                                        .indexRouting(alias.indexRouting()).searchRouting(alias.searchRouting()).build();
                                indexMetaDataBuilder.putAlias(aliasMetaData);
                            }

                            for (Map.Entry<String, Custom> customEntry : customs.entrySet()) {
                                indexMetaDataBuilder.putCustom(customEntry.getKey(), customEntry.getValue());
                            }

                            indexMetaDataBuilder.state(request.state());

                            final IndexMetaData indexMetaData;
                            try {
                                indexMetaData = indexMetaDataBuilder.build();
                            } catch (Exception e) {
                                removalReason = "failed to build index metadata";
                                throw e;
                            }

                            indexService.getIndexEventListener().beforeIndexAddedToCluster(indexMetaData.getIndex(),
                                    indexMetaData.getSettings());

                            MetaData newMetaData = MetaData.builder(currentState.metaData())
                                    .put(indexMetaData, false)
                                    .build();

                            String maybeShadowIndicator = IndexMetaData.isIndexUsingShadowReplicas(indexMetaData.getSettings()) ? "s" : "";
                            logger.info("[{}] creating index, cause [{}], templates {}, shards [{}]/[{}{}], mappings {}",
                                    request.index(), request.cause(), templateNames, indexMetaData.getNumberOfShards(),
                                    indexMetaData.getNumberOfReplicas(), maybeShadowIndicator, mappings.keySet());

                            ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                            if (!request.blocks().isEmpty()) {
                                for (ClusterBlock block : request.blocks()) {
                                    blocks.addIndexBlock(request.index(), block);
                                }
                            }
                            blocks.updateBlocks(indexMetaData);

                            ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

                            if (request.state() == State.OPEN) {
                                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                                        .addAsNew(updatedState.metaData().index(request.index()));
                                RoutingAllocation.Result routingResult = allocationService.reroute(
                                        ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                                        "index [" + request.index() + "] created");
                                updatedState = ClusterState.builder(updatedState).routingResult(routingResult).build();
                            }
                            removalReason = "cleaning up after validating index on master";
                            return updatedState;
                        } finally {
                            if (createdIndex != null) {
                                // Index was already partially created - need to clean up
                                indicesService.removeIndex(createdIndex, removalReason != null ? removalReason : "failed to create index");
                            }
                        }
                    }
                });
    }

    private List<IndexTemplateMetaData> findTemplates(CreateIndexClusterStateUpdateRequest request, ClusterState state, IndexTemplateFilter indexTemplateFilter) throws IOException {
        List<IndexTemplateMetaData> templates = new ArrayList<>();
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData template = cursor.value;
            if (indexTemplateFilter.apply(request, template)) {
                templates.add(template);
            }
        }

        CollectionUtil.timSort(templates, new Comparator<IndexTemplateMetaData>() {
            @Override
            public int compare(IndexTemplateMetaData o1, IndexTemplateMetaData o2) {
                return o2.order() - o1.order();
            }
        });
        return templates;
    }

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        validateIndexName(request.index(), state);
        validateIndexSettings(request.index(), request.settings());
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
                validationErrors.add("custom path [" + customPath + "] is not a sub-path of path.shared_data [" + env.sharedDataFile() + "]");
            }
        }
        //norelease - this can be removed?
        Integer number_of_primaries = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        Integer number_of_replicas = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, null);
        if (number_of_primaries != null && number_of_primaries <= 0) {
            validationErrors.add("index must have 1 or more primary shards");
        }
        if (number_of_replicas != null && number_of_replicas < 0) {
            validationErrors.add("index must have 0 or more replica shards");
        }
        return validationErrors;
    }

    private static class DefaultIndexTemplateFilter implements IndexTemplateFilter {
        @Override
        public boolean apply(CreateIndexClusterStateUpdateRequest request, IndexTemplateMetaData template) {
            return Regex.simpleMatch(template.template(), request.index());
        }
    }

    /**
     * Validates the settings and mappings for shrinking an index.
     * @return the list of nodes at least one instance of the source index shards are allocated
     */
    static List<String> validateShrinkIndex(ClusterState state, String sourceIndex,
                                        Set<String> targetIndexMappingsTypes, String targetIndexName,
                                        Settings targetIndexSettings) {
        if (state.metaData().hasIndex(targetIndexName)) {
            throw new IndexAlreadyExistsException(state.metaData().index(targetIndexName).getIndex());
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

    static void prepareShrinkIndexSettings(ClusterState currentState, Set<String> mappingKeys, Settings.Builder indexSettingsBuilder, Index shrinkFromIndex, String shrinkIntoName) {
        final IndexMetaData sourceMetaData = currentState.metaData().index(shrinkFromIndex.getName());
        final List<String> nodesToAllocateOn = validateShrinkIndex(currentState, shrinkFromIndex.getName(),
            mappingKeys, shrinkIntoName, indexSettingsBuilder.build());
        final Predicate<String> analysisSimilarityPredicate = (s) -> s.startsWith("index.similarity.")
            || s.startsWith("index.analysis.");
        indexSettingsBuilder
            // we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
            // once we are allocated.
            .put("index.routing.allocation.initial_recovery._id",
                Strings.arrayToCommaDelimitedString(nodesToAllocateOn.toArray()))
            // we only try once and then give up with a shrink index
            .put("index.allocation.max_retries", 1)
            // now copy all similarity / analysis settings - this overrides all settings from the user unless they
            // wanna add extra settings
            .put(sourceMetaData.getSettings().filter(analysisSimilarityPredicate))
            .put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME.getKey(), shrinkFromIndex.getName())
            .put(IndexMetaData.INDEX_SHRINK_SOURCE_UUID.getKey(), shrinkFromIndex.getUUID());
    }

}
