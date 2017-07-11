/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeInfoAction;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class Upgrade implements ActionPlugin {

    public static final Version UPGRADE_INTRODUCED = Version.V_5_5_0; // TODO: Probably will need to change this to 5.6.0

    private final Settings settings;
    private final List<BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> upgradeCheckFactories;

    public Upgrade(Settings settings) {
        this.settings = settings;
        this.upgradeCheckFactories = new ArrayList<>();
        upgradeCheckFactories.add(getWatcherUpgradeCheckFactory(settings));
    }

    public Collection<Object> createComponents(InternalClient internalClient, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry) {
        List<IndexUpgradeCheck> upgradeChecks = new ArrayList<>(upgradeCheckFactories.size());
        for (BiFunction<InternalClient, ClusterService, IndexUpgradeCheck> checkFactory : upgradeCheckFactories) {
            upgradeChecks.add(checkFactory.apply(internalClient, clusterService));
        }
        return Collections.singletonList(new IndexUpgradeService(settings, Collections.unmodifiableList(upgradeChecks)));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(IndexUpgradeInfoAction.INSTANCE, IndexUpgradeInfoAction.TransportAction.class),
                new ActionHandler<>(IndexUpgradeAction.INSTANCE, IndexUpgradeAction.TransportAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestIndexUpgradeInfoAction(settings, restController),
                new RestIndexUpgradeAction(settings, restController)
        );
    }

    static BiFunction<InternalClient, ClusterService, IndexUpgradeCheck> getWatcherUpgradeCheckFactory(Settings settings) {
        return (internalClient, clusterService) ->
                new IndexUpgradeCheck<Boolean>("watcher",
                        settings,
                        indexMetaData -> {
                            if (".watches".equals(indexMetaData.getIndex().getName()) ||
                                    indexMetaData.getAliases().containsKey(".watches")) {
                                if (indexMetaData.getMappings().size() == 1 && indexMetaData.getMappings().containsKey("doc")) {
                                    return UpgradeActionRequired.UP_TO_DATE;
                                } else {
                                    return UpgradeActionRequired.UPGRADE;
                                }
                            } else {
                                return UpgradeActionRequired.NOT_APPLICABLE;
                            }
                        }, internalClient,
                        clusterService,
                        new String[]{"watch"},
                        new Script(ScriptType.INLINE, "painless", "ctx._type = \"doc\";\n" +
                                "if (ctx._source.containsKey(\"_status\") && !ctx._source.containsKey(\"status\")  ) {\n" +
                                "  ctx._source.status = ctx._source.remove(\"_status\");\n" +
                                "}",
                                new HashMap<>()),
                        booleanActionListener -> preWatcherUpgrade(internalClient, booleanActionListener),
                        (shouldStartWatcher, listener) -> postWatcherUpgrade(internalClient, shouldStartWatcher, listener)
                );
    }

    private static void preWatcherUpgrade(Client client, ActionListener<Boolean> listener) {
        new WatcherClient(client).watcherStats(new WatcherStatsRequest(), ActionListener.wrap(
                stats -> {
                    if (stats.watcherMetaData().manuallyStopped()) {
                        // don't start the watcher after upgrade
                        listener.onResponse(false);
                    } else {
                        // stop the watcher
                        new WatcherClient(client).watcherService(new WatcherServiceRequest().stop(), ActionListener.wrap(
                                stopResponse -> {
                                    if (stopResponse.isAcknowledged()) {
                                        listener.onResponse(true);
                                    } else {
                                        listener.onFailure(new IllegalStateException("unable to stop watcher service"));
                                    }
                                }, listener::onFailure
                        ));
                    }
                }, listener::onFailure));
    }

    private static void postWatcherUpgrade(Client client, Boolean shouldStartWatcher, ActionListener<TransportResponse.Empty> listener) {
        // if you are confused that these steps are numbered reversed, we are creating the action listeners first
        // but only calling the deletion at the end of the method (inception style)
        // step 3, after successful deletion of triggered watch index: start watcher
        ActionListener<DeleteIndexResponse> deleteTriggeredWatchIndexResponse = ActionListener.wrap(deleteIndexResponse ->
                startWatcherIfNeeded(shouldStartWatcher, client, listener), e -> {
            if (e instanceof IndexNotFoundException) {
                startWatcherIfNeeded(shouldStartWatcher, client, listener);
            } else {
                listener.onFailure(e);
            }
        });

        // step 2, after acknowledged delete triggered watches template: delete triggered watches index
        ActionListener<DeleteIndexTemplateResponse> triggeredWatchIndexTemplateListener = ActionListener.wrap(r -> {
            if (r.isAcknowledged()) {
                client.admin().indices().prepareDelete(".triggered_watches").execute(deleteTriggeredWatchIndexResponse);
            } else {
                listener.onFailure(new ElasticsearchException("Deleting triggered_watches template not acknowledged"));
            }

        }, listener::onFailure);

        // step 1, after acknowledged watches template deletion: delete triggered_watches template
        ActionListener<DeleteIndexTemplateResponse> watchIndexTemplateListener = ActionListener.wrap(r -> {
            if (r.isAcknowledged()) {
                client.admin().indices().prepareDeleteTemplate("triggered_watches").execute(triggeredWatchIndexTemplateListener);
            } else {
                listener.onFailure(new ElasticsearchException("Deleting watches template not acknowledged"));
            }
        }, listener::onFailure);
        client.admin().indices().prepareDeleteTemplate("watches").execute(watchIndexTemplateListener);
    }

    private static void startWatcherIfNeeded(Boolean shouldStartWatcher, Client client, ActionListener<TransportResponse.Empty> listener) {
        if (shouldStartWatcher) {
            // Start the watcher service
            new WatcherClient(client).watcherService(new WatcherServiceRequest().start(), ActionListener.wrap(
                    stopResponse -> listener.onResponse(TransportResponse.Empty.INSTANCE), listener::onFailure
            ));
        } else {
            listener.onResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}
