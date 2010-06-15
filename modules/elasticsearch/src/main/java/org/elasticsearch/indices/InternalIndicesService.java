/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.CloseableIndexComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.*;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexGatewayModule;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.routing.OperationRoutingModule;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.plugins.IndicesPluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.util.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.collect.Sets.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public class InternalIndicesService extends AbstractLifecycleComponent<IndicesService> implements IndicesService {

    private final IndicesClusterStateService clusterStateService;

    private final InternalIndicesLifecycle indicesLifecycle;

    private final IndicesAnalysisService indicesAnalysisService;

    private final Injector injector;

    private final PluginsService pluginsService;

    private final Map<String, Injector> indicesInjectors = new HashMap<String, Injector>();

    private volatile ImmutableMap<String, IndexService> indices = ImmutableMap.of();

    @Inject public InternalIndicesService(Settings settings, IndicesClusterStateService clusterStateService,
                                          IndicesLifecycle indicesLifecycle, IndicesAnalysisService indicesAnalysisService, Injector injector) {
        super(settings);
        this.clusterStateService = clusterStateService;
        this.indicesLifecycle = (InternalIndicesLifecycle) indicesLifecycle;
        this.indicesAnalysisService = indicesAnalysisService;
        this.injector = injector;

        this.pluginsService = injector.getInstance(PluginsService.class);
    }

    @Override protected void doStart() throws ElasticSearchException {
        clusterStateService.start();
    }

    @Override protected void doStop() throws ElasticSearchException {
        clusterStateService.stop();
        for (String index : indices.keySet()) {
            deleteIndex(index, false);
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
        clusterStateService.close();
        indicesAnalysisService.close();
    }

    @Override public IndicesLifecycle indicesLifecycle() {
        return this.indicesLifecycle;
    }

    /**
     * Returns <tt>true</tt> if changes (adding / removing) indices, shards and so on are allowed.
     */
    public boolean changesAllowed() {
        // we check on stop here since we defined stop when we delete the indices
        return lifecycle.started();
    }

    @Override public UnmodifiableIterator<IndexService> iterator() {
        return indices.values().iterator();
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public Set<String> indices() {
        return newHashSet(indices.keySet());
    }

    public IndexService indexService(String index) {
        return indices.get(index);
    }

    @Override public IndexService indexServiceSafe(String index) throws IndexMissingException {
        IndexService indexService = indexService(index);
        if (indexService == null) {
            throw new IndexMissingException(new Index(index));
        }
        return indexService;
    }

    @Override public GroupShardsIterator searchShards(ClusterState clusterState, String[] indexNames, String queryHint) throws ElasticSearchException {
        if (indexNames == null || indexNames.length == 0) {
            ImmutableMap<String, IndexService> indices = this.indices;
            indexNames = indices.keySet().toArray(new String[indices.keySet().size()]);
        }
        GroupShardsIterator its = new GroupShardsIterator();
        for (String index : indexNames) {
            its.add(indexServiceSafe(index).operationRouting().searchShards(clusterState, queryHint));
        }
        return its;
    }

    public synchronized IndexService createIndex(String sIndexName, Settings settings, String localNodeId) throws ElasticSearchException {
        Index index = new Index(sIndexName);
        if (indicesInjectors.containsKey(index.name())) {
            throw new IndexAlreadyExistsException(index);
        }

        indicesLifecycle.beforeIndexCreated(index);

        logger.debug("Creating Index [{}], shards [{}]/[{}]", new Object[]{sIndexName, settings.get(SETTING_NUMBER_OF_SHARDS), settings.get(SETTING_NUMBER_OF_REPLICAS)});

        Settings indexSettings = settingsBuilder()
                .put("settingsType", "index")
                .put(this.settings)
                .put(settings)
                .classLoader(settings.getClassLoader())
                .globalSettings(settings.getGlobalSettings())
                .build();

        ArrayList<Module> modules = new ArrayList<Module>();
        modules.add(new IndexNameModule(index));
        modules.add(new LocalNodeIdModule(localNodeId));
        modules.add(new IndexSettingsModule(indexSettings));
        modules.add(new IndicesPluginsModule(indexSettings, pluginsService));
        modules.add(new IndexEngineModule(indexSettings));
        modules.add(new AnalysisModule(indexSettings, indicesAnalysisService));
        modules.add(new SimilarityModule(indexSettings));
        modules.add(new IndexCacheModule(indexSettings));
        modules.add(new IndexQueryParserModule(indexSettings));
        modules.add(new MapperServiceModule());
        modules.add(new IndexGatewayModule(indexSettings, injector.getInstance(Gateway.class)));
        modules.add(new OperationRoutingModule(indexSettings));
        modules.add(new IndexModule());

        pluginsService.processModules(modules);

        Injector indexInjector = injector.createChildInjector(modules);

        indicesInjectors.put(index.name(), indexInjector);

        IndexService indexService = indexInjector.getInstance(IndexService.class);

        indicesLifecycle.afterIndexCreated(indexService);

        indices = newMapBuilder(indices).put(index.name(), indexService).immutableMap();

        return indexService;
    }

    @Override public synchronized void cleanIndex(String index) throws ElasticSearchException {
        deleteIndex(index, false);
    }

    @Override public synchronized void deleteIndex(String index) throws ElasticSearchException {
        deleteIndex(index, true);
    }

    private synchronized void deleteIndex(String index, boolean delete) throws ElasticSearchException {
        Injector indexInjector = indicesInjectors.remove(index);
        if (indexInjector == null) {
            if (!delete) {
                return;
            }
            throw new IndexMissingException(new Index(index));
        }
        if (delete) {
            logger.debug("Deleting Index [{}]", index);
        }

        Map<String, IndexService> tmpMap = newHashMap(indices);
        IndexService indexService = tmpMap.remove(index);
        indices = ImmutableMap.copyOf(tmpMap);

        indicesLifecycle.beforeIndexClosed(indexService, delete);

        for (Class<? extends CloseableIndexComponent> closeable : pluginsService.indexServices()) {
            indexInjector.getInstance(closeable).close(delete);
        }

        indexService.close(delete);

        indexInjector.getInstance(FilterCache.class).close();
        indexInjector.getInstance(AnalysisService.class).close();
        indexInjector.getInstance(IndexEngine.class).close();
        indexInjector.getInstance(IndexServiceManagement.class).close();

        indexInjector.getInstance(IndexGateway.class).close(delete);

        Injectors.close(injector);

        indicesLifecycle.afterIndexClosed(indexService.index(), delete);
    }
}