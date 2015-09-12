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

package org.elasticsearch.rest.action;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.action.admin.cluster.health.RestClusterHealthAction;
import org.elasticsearch.rest.action.admin.cluster.node.hotthreads.RestNodesHotThreadsAction;
import org.elasticsearch.rest.action.admin.cluster.node.info.RestNodesInfoAction;
import org.elasticsearch.rest.action.admin.cluster.node.stats.RestNodesStatsAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.delete.RestDeleteRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.get.RestGetRepositoriesAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.put.RestPutRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.repositories.verify.RestVerifyRepositoryAction;
import org.elasticsearch.rest.action.admin.cluster.reroute.RestClusterRerouteAction;
import org.elasticsearch.rest.action.admin.cluster.settings.RestClusterGetSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.settings.RestClusterUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.cluster.shards.RestClusterSearchShardsAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.create.RestCreateSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.delete.RestDeleteSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.get.RestGetSnapshotsAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.restore.RestRestoreSnapshotAction;
import org.elasticsearch.rest.action.admin.cluster.snapshots.status.RestSnapshotsStatusAction;
import org.elasticsearch.rest.action.admin.cluster.state.RestClusterStateAction;
import org.elasticsearch.rest.action.admin.cluster.stats.RestClusterStatsAction;
import org.elasticsearch.rest.action.admin.cluster.tasks.RestPendingClusterTasksAction;
import org.elasticsearch.rest.action.admin.indices.alias.RestIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.delete.RestIndexDeleteAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.get.RestGetAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.get.RestGetIndicesAliasesAction;
import org.elasticsearch.rest.action.admin.indices.alias.head.RestAliasesExistAction;
import org.elasticsearch.rest.action.admin.indices.alias.put.RestIndexPutAliasAction;
import org.elasticsearch.rest.action.admin.indices.analyze.RestAnalyzeAction;
import org.elasticsearch.rest.action.admin.indices.cache.clear.RestClearIndicesCacheAction;
import org.elasticsearch.rest.action.admin.indices.close.RestCloseIndexAction;
import org.elasticsearch.rest.action.admin.indices.create.RestCreateIndexAction;
import org.elasticsearch.rest.action.admin.indices.delete.RestDeleteIndexAction;
import org.elasticsearch.rest.action.admin.indices.exists.indices.RestIndicesExistsAction;
import org.elasticsearch.rest.action.admin.indices.exists.types.RestTypesExistsAction;
import org.elasticsearch.rest.action.admin.indices.flush.RestFlushAction;
import org.elasticsearch.rest.action.admin.indices.flush.RestSyncedFlushAction;
import org.elasticsearch.rest.action.admin.indices.get.RestGetIndicesAction;
import org.elasticsearch.rest.action.admin.indices.mapping.get.RestGetFieldMappingAction;
import org.elasticsearch.rest.action.admin.indices.mapping.get.RestGetMappingAction;
import org.elasticsearch.rest.action.admin.indices.mapping.put.RestPutMappingAction;
import org.elasticsearch.rest.action.admin.indices.open.RestOpenIndexAction;
import org.elasticsearch.rest.action.admin.indices.optimize.RestOptimizeAction;
import org.elasticsearch.rest.action.admin.indices.recovery.RestRecoveryAction;
import org.elasticsearch.rest.action.admin.indices.refresh.RestRefreshAction;
import org.elasticsearch.rest.action.admin.indices.segments.RestIndicesSegmentsAction;
import org.elasticsearch.rest.action.admin.indices.settings.RestGetSettingsAction;
import org.elasticsearch.rest.action.admin.indices.settings.RestUpdateSettingsAction;
import org.elasticsearch.rest.action.admin.indices.shards.RestIndicesShardStoresAction;
import org.elasticsearch.rest.action.admin.indices.stats.RestIndicesStatsAction;
import org.elasticsearch.rest.action.admin.indices.template.delete.RestDeleteIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.get.RestGetIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.head.RestHeadIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.template.put.RestPutIndexTemplateAction;
import org.elasticsearch.rest.action.admin.indices.upgrade.RestUpgradeAction;
import org.elasticsearch.rest.action.admin.indices.validate.query.RestValidateQueryAction;
import org.elasticsearch.rest.action.admin.indices.validate.template.RestRenderSearchTemplateAction;
import org.elasticsearch.rest.action.admin.indices.warmer.delete.RestDeleteWarmerAction;
import org.elasticsearch.rest.action.admin.indices.warmer.get.RestGetWarmerAction;
import org.elasticsearch.rest.action.admin.indices.warmer.put.RestPutWarmerAction;
import org.elasticsearch.rest.action.bulk.RestBulkAction;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestAliasAction;
import org.elasticsearch.rest.action.cat.RestAllocationAction;
import org.elasticsearch.rest.action.cat.RestCatAction;
import org.elasticsearch.rest.action.cat.RestFielddataAction;
import org.elasticsearch.rest.action.cat.RestHealthAction;
import org.elasticsearch.rest.action.cat.RestIndicesAction;
import org.elasticsearch.rest.action.cat.RestMasterAction;
import org.elasticsearch.rest.action.cat.RestNodeAttrsAction;
import org.elasticsearch.rest.action.cat.RestNodesAction;
import org.elasticsearch.rest.action.cat.RestPluginsAction;
import org.elasticsearch.rest.action.cat.RestSegmentsAction;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.rest.action.cat.RestThreadPoolAction;
import org.elasticsearch.rest.action.delete.RestDeleteAction;
import org.elasticsearch.rest.action.explain.RestExplainAction;
import org.elasticsearch.rest.action.fieldstats.RestFieldStatsAction;
import org.elasticsearch.rest.action.get.RestGetAction;
import org.elasticsearch.rest.action.get.RestGetSourceAction;
import org.elasticsearch.rest.action.get.RestHeadAction;
import org.elasticsearch.rest.action.get.RestMultiGetAction;
import org.elasticsearch.rest.action.index.RestIndexAction;
import org.elasticsearch.rest.action.main.RestMainAction;
import org.elasticsearch.rest.action.percolate.RestMultiPercolateAction;
import org.elasticsearch.rest.action.percolate.RestPercolateAction;
import org.elasticsearch.rest.action.script.RestDeleteIndexedScriptAction;
import org.elasticsearch.rest.action.script.RestGetIndexedScriptAction;
import org.elasticsearch.rest.action.script.RestPutIndexedScriptAction;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.rest.action.suggest.RestSuggestAction;
import org.elasticsearch.rest.action.template.RestDeleteSearchTemplateAction;
import org.elasticsearch.rest.action.template.RestGetSearchTemplateAction;
import org.elasticsearch.rest.action.template.RestPutSearchTemplateAction;
import org.elasticsearch.rest.action.termvectors.RestMultiTermVectorsAction;
import org.elasticsearch.rest.action.termvectors.RestTermVectorsAction;
import org.elasticsearch.rest.action.update.RestUpdateAction;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RestActionModule extends AbstractModule {
    private List<Class<? extends BaseRestHandler>> restPluginsActions = new ArrayList<>();

    public RestActionModule(List<Class<? extends BaseRestHandler>> restPluginsActions) {
        this.restPluginsActions = restPluginsActions;
    }

    @Override
    protected void configure() {
        for (Class<? extends BaseRestHandler> restAction : restPluginsActions) {
            bind(restAction).asEagerSingleton();
        }

        bind(RestMainAction.class).asEagerSingleton();

        bind(RestNodesInfoAction.class).asEagerSingleton();
        bind(RestNodesStatsAction.class).asEagerSingleton();
        bind(RestNodesHotThreadsAction.class).asEagerSingleton();
        bind(RestClusterStatsAction.class).asEagerSingleton();
        bind(RestClusterStateAction.class).asEagerSingleton();
        bind(RestClusterHealthAction.class).asEagerSingleton();
        bind(RestClusterUpdateSettingsAction.class).asEagerSingleton();
        bind(RestClusterGetSettingsAction.class).asEagerSingleton();
        bind(RestClusterRerouteAction.class).asEagerSingleton();
        bind(RestClusterSearchShardsAction.class).asEagerSingleton();
        bind(RestPendingClusterTasksAction.class).asEagerSingleton();
        bind(RestPutRepositoryAction.class).asEagerSingleton();
        bind(RestGetRepositoriesAction.class).asEagerSingleton();
        bind(RestDeleteRepositoryAction.class).asEagerSingleton();
        bind(RestVerifyRepositoryAction.class).asEagerSingleton();
        bind(RestGetSnapshotsAction.class).asEagerSingleton();
        bind(RestCreateSnapshotAction.class).asEagerSingleton();
        bind(RestRestoreSnapshotAction.class).asEagerSingleton();
        bind(RestDeleteSnapshotAction.class).asEagerSingleton();
        bind(RestSnapshotsStatusAction.class).asEagerSingleton();

        bind(RestIndicesExistsAction.class).asEagerSingleton();
        bind(RestTypesExistsAction.class).asEagerSingleton();
        bind(RestGetIndicesAction.class).asEagerSingleton();
        bind(RestIndicesStatsAction.class).asEagerSingleton();
        bind(RestIndicesSegmentsAction.class).asEagerSingleton();
        bind(RestIndicesShardStoresAction.class).asEagerSingleton();
        bind(RestGetAliasesAction.class).asEagerSingleton();
        bind(RestAliasesExistAction.class).asEagerSingleton();
        bind(RestIndexDeleteAliasesAction.class).asEagerSingleton();
        bind(RestIndexPutAliasAction.class).asEagerSingleton();
        bind(RestIndicesAliasesAction.class).asEagerSingleton();
        bind(RestGetIndicesAliasesAction.class).asEagerSingleton();
        bind(RestCreateIndexAction.class).asEagerSingleton();
        bind(RestDeleteIndexAction.class).asEagerSingleton();
        bind(RestCloseIndexAction.class).asEagerSingleton();
        bind(RestOpenIndexAction.class).asEagerSingleton();

        bind(RestUpdateSettingsAction.class).asEagerSingleton();
        bind(RestGetSettingsAction.class).asEagerSingleton();

        bind(RestAnalyzeAction.class).asEagerSingleton();
        bind(RestGetIndexTemplateAction.class).asEagerSingleton();
        bind(RestPutIndexTemplateAction.class).asEagerSingleton();
        bind(RestDeleteIndexTemplateAction.class).asEagerSingleton();
        bind(RestHeadIndexTemplateAction.class).asEagerSingleton();

        bind(RestPutWarmerAction.class).asEagerSingleton();
        bind(RestDeleteWarmerAction.class).asEagerSingleton();
        bind(RestGetWarmerAction.class).asEagerSingleton();

        bind(RestPutMappingAction.class).asEagerSingleton();
        bind(RestGetMappingAction.class).asEagerSingleton();
        bind(RestGetFieldMappingAction.class).asEagerSingleton();

        bind(RestRefreshAction.class).asEagerSingleton();
        bind(RestFlushAction.class).asEagerSingleton();
        bind(RestSyncedFlushAction.class).asEagerSingleton();
        bind(RestOptimizeAction.class).asEagerSingleton();
        bind(RestUpgradeAction.class).asEagerSingleton();
        bind(RestClearIndicesCacheAction.class).asEagerSingleton();

        bind(RestIndexAction.class).asEagerSingleton();
        bind(RestGetAction.class).asEagerSingleton();
        bind(RestGetSourceAction.class).asEagerSingleton();
        bind(RestHeadAction.class).asEagerSingleton();
        bind(RestMultiGetAction.class).asEagerSingleton();
        bind(RestDeleteAction.class).asEagerSingleton();
        bind(org.elasticsearch.rest.action.count.RestCountAction.class).asEagerSingleton();
        bind(RestSuggestAction.class).asEagerSingleton();
        bind(RestTermVectorsAction.class).asEagerSingleton();
        bind(RestMultiTermVectorsAction.class).asEagerSingleton();
        bind(RestBulkAction.class).asEagerSingleton();
        bind(RestUpdateAction.class).asEagerSingleton();
        bind(RestPercolateAction.class).asEagerSingleton();
        bind(RestMultiPercolateAction.class).asEagerSingleton();

        bind(RestSearchAction.class).asEagerSingleton();
        bind(RestSearchScrollAction.class).asEagerSingleton();
        bind(RestClearScrollAction.class).asEagerSingleton();
        bind(RestMultiSearchAction.class).asEagerSingleton();
        bind(RestRenderSearchTemplateAction.class).asEagerSingleton();

        bind(RestValidateQueryAction.class).asEagerSingleton();

        bind(RestExplainAction.class).asEagerSingleton();

        bind(RestRecoveryAction.class).asEagerSingleton();

        // Templates API
        bind(RestGetSearchTemplateAction.class).asEagerSingleton();
        bind(RestPutSearchTemplateAction.class).asEagerSingleton();
        bind(RestDeleteSearchTemplateAction.class).asEagerSingleton();

        // Scripts API
        bind(RestGetIndexedScriptAction.class).asEagerSingleton();
        bind(RestPutIndexedScriptAction.class).asEagerSingleton();
        bind(RestDeleteIndexedScriptAction.class).asEagerSingleton();


        bind(RestFieldStatsAction.class).asEagerSingleton();

        // cat API
        Multibinder<AbstractCatAction> catActionMultibinder = Multibinder.newSetBinder(binder(), AbstractCatAction.class);
        catActionMultibinder.addBinding().to(RestAllocationAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestShardsAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestMasterAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestNodesAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestIndicesAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestSegmentsAction.class).asEagerSingleton();
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        catActionMultibinder.addBinding().to(org.elasticsearch.rest.action.cat.RestCountAction.class).asEagerSingleton();
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        catActionMultibinder.addBinding().to(org.elasticsearch.rest.action.cat.RestRecoveryAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestHealthAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(org.elasticsearch.rest.action.cat.RestPendingClusterTasksAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestAliasAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestThreadPoolAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestPluginsAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestFielddataAction.class).asEagerSingleton();
        catActionMultibinder.addBinding().to(RestNodeAttrsAction.class).asEagerSingleton();
        // no abstract cat action
        bind(RestCatAction.class).asEagerSingleton();
    }
}
