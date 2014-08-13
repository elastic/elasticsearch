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
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * Service responsible for submitting create index requests
 */
public class MetaDataCreateIndexService extends AbstractComponent {

    public final static int MAX_INDEX_NAME_BYTES = 100;

    private final Environment environment;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final MetaDataService metaDataService;
    private final Version version;
    private final String riverIndexName;
    private final AliasValidator aliasValidator;

    @Inject
    public MetaDataCreateIndexService(Settings settings, Environment environment, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                      AllocationService allocationService, MetaDataService metaDataService, Version version, @RiverIndexName String riverIndexName,
                                      AliasValidator aliasValidator) {
        super(settings);
        this.environment = environment;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.metaDataService = metaDataService;
        this.version = version;
        this.riverIndexName = riverIndexName;
        this.aliasValidator = aliasValidator;
    }

    public void createIndex(final CreateIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : request.settings().getAsMap().entrySet()) {
            if (!entry.getKey().startsWith("index.")) {
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        request.settings(updatedSettingsBuilder.build());

        // we lock here, and not within the cluster service callback since we don't want to
        // block the whole cluster state handling
        final Semaphore mdLock = metaDataService.indexMetaDataLock(request.index());

        // quick check to see if we can acquire a lock, otherwise spawn to a thread pool
        if (mdLock.tryAcquire()) {
            createIndex(request, listener, mdLock);
            return;
        }

        threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!mdLock.tryAcquire(request.masterNodeTimeout().nanos(), TimeUnit.NANOSECONDS)) {
                        listener.onFailure(new ProcessClusterEventTimeoutException(request.masterNodeTimeout(), "acquire index lock"));
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    listener.onFailure(e);
                    return;
                }

                createIndex(request, listener, mdLock);
            }
        });
    }

    public void validateIndexName(String index, ClusterState state) throws ElasticsearchException {
        if (state.routingTable().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        if (state.metaData().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        if (!Strings.validFileName(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (index.contains("#")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain '#'");
        }
        if (!index.equals(riverIndexName) && index.charAt(0) == '_') {
            throw new InvalidIndexNameException(new Index(index), index, "must not start with '_'");
        }
        if (!index.toLowerCase(Locale.ROOT).equals(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must be lowercase");
        }
        int byteCount = 0;
        try {
            byteCount = index.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw new InvalidIndexNameException(new Index(index), index,
                    "index name is too long, (" + byteCount +
                    " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (state.metaData().aliases().containsKey(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "already exists as alias");
        }
    }

    private void createIndex(final CreateIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener, final Semaphore mdLock) {
        clusterService.submitStateUpdateTask("create-index [" + request.index() + "], cause [" + request.cause() + "]", Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                mdLock.release();
                super.onAllNodesAcked(t);
            }

            @Override
            public void onAckTimeout() {
                mdLock.release();
                super.onAckTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                mdLock.release();
                super.onFailure(source, t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                boolean indexCreated = false;
                String failureReason = null;
                try {
                    validate(request, currentState);

                    for (Alias alias : request.aliases()) {
                        aliasValidator.validateAlias(alias, request.index(), currentState.metaData());
                    }

                    // we only find a template when its an API call (a new index)
                    // find templates, highest order are better matching
                    List<IndexTemplateMetaData> templates = findTemplates(request, currentState);

                    Map<String, Custom> customs = Maps.newHashMap();

                    // add the request mapping
                    Map<String, Map<String, Object>> mappings = Maps.newHashMap();

                    Map<String, AliasMetaData> templatesAliases = Maps.newHashMap();

                    for (Map.Entry<String, String> entry : request.mappings().entrySet()) {
                        mappings.put(entry.getKey(), parseMapping(entry.getValue()));
                    }

                    for (Map.Entry<String, Custom> entry : request.customs().entrySet()) {
                        customs.put(entry.getKey(), entry.getValue());
                    }

                    // apply templates, merging the mappings into the request mapping if exists
                    for (IndexTemplateMetaData template : templates) {
                        for (ObjectObjectCursor<String, CompressedString> cursor : template.mappings()) {
                            if (mappings.containsKey(cursor.key)) {
                                XContentHelper.mergeDefaults(mappings.get(cursor.key), parseMapping(cursor.value.string()));
                            } else {
                                mappings.put(cursor.key, parseMapping(cursor.value.string()));
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
                                IndexMetaData.Custom merged = IndexMetaData.lookupFactorySafe(type).merge(existing, custom);
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

                    // now add config level mappings
                    File mappingsDir = new File(environment.configFile(), "mappings");
                    if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                        // first index level
                        File indexMappingsDir = new File(mappingsDir, request.index());
                        if (indexMappingsDir.exists() && indexMappingsDir.isDirectory()) {
                            addMappings(mappings, indexMappingsDir);
                        }

                        // second is the _default mapping
                        File defaultMappingsDir = new File(mappingsDir, "_default");
                        if (defaultMappingsDir.exists() && defaultMappingsDir.isDirectory()) {
                            addMappings(mappings, defaultMappingsDir);
                        }
                    }

                    ImmutableSettings.Builder indexSettingsBuilder = settingsBuilder();
                    // apply templates, here, in reverse order, since first ones are better matching
                    for (int i = templates.size() - 1; i >= 0; i--) {
                        indexSettingsBuilder.put(templates.get(i).settings());
                    }
                    // now, put the request settings, so they override templates
                    indexSettingsBuilder.put(request.settings());

                    if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                        if (request.index().equals(riverIndexName)) {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
                        } else {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                        }
                    }
                    if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                        if (request.index().equals(riverIndexName)) {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                        } else {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                        }
                    }

                    if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
                        indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
                    }

                    if (indexSettingsBuilder.get(SETTING_VERSION_CREATED) == null) {
                        DiscoveryNodes nodes = currentState.nodes();
                        final Version createdVersion = Version.smallest(version, nodes.smallestNonClientNodeVersion());
                        indexSettingsBuilder.put(SETTING_VERSION_CREATED, createdVersion);
                    }
                    indexSettingsBuilder.put(SETTING_UUID, Strings.randomBase64UUID());

                    Settings actualIndexSettings = indexSettingsBuilder.build();

                    // Set up everything, now locally create the index to see that things are ok, and apply

                    // create the index here (on the master) to validate it can be created, as well as adding the mapping
                    indicesService.createIndex(request.index(), actualIndexSettings, clusterService.localNode().id());
                    indexCreated = true;
                    // now add the mappings
                    IndexService indexService = indicesService.indexServiceSafe(request.index());
                    MapperService mapperService = indexService.mapperService();
                    // first, add the default mapping
                    if (mappings.containsKey(MapperService.DEFAULT_MAPPING)) {
                        try {
                            mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedString(XContentFactory.jsonBuilder().map(mappings.get(MapperService.DEFAULT_MAPPING)).string()), false);
                        } catch (Exception e) {
                            failureReason = "failed on parsing default mapping on index creation";
                            throw new MapperParsingException("mapping [" + MapperService.DEFAULT_MAPPING + "]", e);
                        }
                    }
                    for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
                        if (entry.getKey().equals(MapperService.DEFAULT_MAPPING)) {
                            continue;
                        }
                        try {
                            // apply the default here, its the first time we parse it
                            mapperService.merge(entry.getKey(), new CompressedString(XContentFactory.jsonBuilder().map(entry.getValue()).string()), true);
                        } catch (Exception e) {
                            failureReason = "failed on parsing mappings on index creation";
                            throw new MapperParsingException("mapping [" + entry.getKey() + "]", e);
                        }
                    }

                    IndexQueryParserService indexQueryParserService = indexService.queryParserService();
                    for (Alias alias : request.aliases()) {
                        if (Strings.hasLength(alias.filter())) {
                            aliasValidator.validateAliasFilter(alias.name(), alias.filter(), indexQueryParserService);
                        }
                    }
                    for (AliasMetaData aliasMetaData : templatesAliases.values()) {
                        if (aliasMetaData.filter() != null) {
                            aliasValidator.validateAliasFilter(aliasMetaData.alias(), aliasMetaData.filter().uncompressed(), indexQueryParserService);
                        }
                    }

                    // now, update the mappings with the actual source
                    Map<String, MappingMetaData> mappingsMetaData = Maps.newHashMap();
                    for (DocumentMapper mapper : mapperService.docMappers(true)) {
                        MappingMetaData mappingMd = new MappingMetaData(mapper);
                        mappingsMetaData.put(mapper.type(), mappingMd);
                    }

                    final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(request.index()).settings(actualIndexSettings);
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
                        failureReason = "failed to build index metadata";
                        throw e;
                    }

                    MetaData newMetaData = MetaData.builder(currentState.metaData())
                            .put(indexMetaData, false)
                            .build();

                    logger.info("[{}] creating index, cause [{}], shards [{}]/[{}], mappings {}", request.index(), request.cause(), indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas(), mappings.keySet());

                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    if (!request.blocks().isEmpty()) {
                        for (ClusterBlock block : request.blocks()) {
                            blocks.addIndexBlock(request.index(), block);
                        }
                    }
                    if (request.state() == State.CLOSE) {
                        blocks.addIndexBlock(request.index(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
                    }

                    ClusterState updatedState = ClusterState.builder(currentState).blocks(blocks).metaData(newMetaData).build();

                    if (request.state() == State.OPEN) {
                        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable())
                                .addAsNew(updatedState.metaData().index(request.index()));
                        RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(routingTableBuilder).build());
                        updatedState = ClusterState.builder(updatedState).routingResult(routingResult).build();
                    }
                    return updatedState;
                } finally {
                    if (indexCreated) {
                        // Index was already partially created - need to clean up
                        indicesService.removeIndex(request.index(), failureReason != null ? failureReason : "failed to create index");
                    }
                }
            }
        });
    }

    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        return XContentFactory.xContent(mappingSource).createParser(mappingSource).mapAndClose();
    }

    private void addMappings(Map<String, Map<String, Object>> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        for (File mappingFile : mappingsFiles) {
            if (mappingFile.isHidden()) {
                continue;
            }
            int lastDotIndex = mappingFile.getName().lastIndexOf('.');
            String mappingType = lastDotIndex != -1 ? mappingFile.getName().substring(0, lastDotIndex) : mappingFile.getName();
            try {
                String mappingSource = Streams.copyToString(new InputStreamReader(new FileInputStream(mappingFile), Charsets.UTF_8));
                if (mappings.containsKey(mappingType)) {
                    XContentHelper.mergeDefaults(mappings.get(mappingType), parseMapping(mappingSource));
                } else {
                    mappings.put(mappingType, parseMapping(mappingSource));
                }
            } catch (Exception e) {
                logger.warn("failed to read / parse mapping [" + mappingType + "] from location [" + mappingFile + "], ignoring...", e);
            }
        }
    }

    private List<IndexTemplateMetaData> findTemplates(CreateIndexClusterStateUpdateRequest request, ClusterState state) {
        List<IndexTemplateMetaData> templates = Lists.newArrayList();
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData template = cursor.value;
            if (Regex.simpleMatch(template.template(), request.index())) {
                templates.add(template);
            }
        }

        // see if we have templates defined under config
        File templatesDir = new File(environment.configFile(), "templates");
        if (templatesDir.exists() && templatesDir.isDirectory()) {
            File[] templatesFiles = templatesDir.listFiles();
            if (templatesFiles != null) {
                for (File templatesFile : templatesFiles) {
                    XContentParser parser = null;
                    try {
                        byte[] templatesData = Streams.copyToByteArray(templatesFile);
                        parser = XContentHelper.createParser(templatesData, 0, templatesData.length);
                        IndexTemplateMetaData template = IndexTemplateMetaData.Builder.fromXContent(parser);
                        if (Regex.simpleMatch(template.template(), request.index())) {
                            templates.add(template);
                        }
                    } catch (Exception e) {
                        logger.warn("[{}] failed to read template [{}] from config", e, request.index(), templatesFile.getAbsolutePath());
                    } finally {
                        Releasables.closeWhileHandlingException(parser);
                    }
                }
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

    private void validate(CreateIndexClusterStateUpdateRequest request, ClusterState state) throws ElasticsearchException {
        validateIndexName(request.index(), state);
    }
}
