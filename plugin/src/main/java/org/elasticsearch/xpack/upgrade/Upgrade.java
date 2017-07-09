/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class Upgrade implements ActionPlugin {

    public static final Version UPGRADE_INTRODUCED = Version.V_5_5_0; // TODO: Probably will need to change this to 5.6.0

    private final Settings settings;
    private final List<BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> upgradeCheckFactories;
    private final Set<String> extraParameters;

    public Upgrade(Settings settings) {
        this.settings = settings;
        this.extraParameters = new HashSet<>();
        this.upgradeCheckFactories = new ArrayList<>();
        for (Tuple<Collection<String>, BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> checkFactory : Arrays.asList(
                getKibanaUpgradeCheckFactory(settings),
                getWatcherUpgradeCheckFactory(settings))) {
            extraParameters.addAll(checkFactory.v1());
            upgradeCheckFactories.add(checkFactory.v2());
        }
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
                new RestIndexUpgradeInfoAction(settings, restController, extraParameters),
                new RestIndexUpgradeAction(settings, restController, extraParameters)
        );
    }

    static Tuple<Collection<String>, BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> getKibanaUpgradeCheckFactory(
            Settings settings) {
        return new Tuple<>(
                Collections.singletonList("kibana_indices"),
                (internalClient, clusterService) ->
                        new IndexUpgradeCheck<Void>("kibana",
                                settings,
                                (indexMetaData, params) -> {
                                    String indexName = indexMetaData.getIndex().getName();
                                    String kibanaIndicesMasks = params.getOrDefault("kibana_indices", ".kibana");
                                    String[] kibanaIndices = Strings.delimitedListToStringArray(kibanaIndicesMasks, ",");
                                    if (Regex.simpleMatch(kibanaIndices, indexName)) {
                                        return UpgradeActionRequired.UPGRADE;
                                    } else {
                                        return UpgradeActionRequired.NOT_APPLICABLE;
                                    }
                                }, internalClient,
                                clusterService,
                                Strings.EMPTY_ARRAY,
                                new Script(ScriptType.INLINE, "painless", "ctx._id = ctx._type + \"-\" + ctx._id;\n" +
                                        "ctx._source = [ ctx._type : ctx._source ];\n" +
                                        "ctx._source.type = ctx._type;\n" +
                                        "ctx._type = \"doc\";",
                                        new HashMap<>())));
    }

    static Tuple<Collection<String>, BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> getWatcherUpgradeCheckFactory(
            Settings settings) {
        return new Tuple<>(
                Collections.emptyList(),
                (internalClient, clusterService) ->
                        new IndexUpgradeCheck<Boolean>("watcher",
                                settings,
                                (indexMetaData, params) -> {
                                    if (".watches".equals(indexMetaData.getIndex().getName()) ||
                                            indexMetaData.getAliases().containsKey(".watches")) {
                                        if (indexMetaData.getMappings().size() == 1 && indexMetaData.getMappings().containsKey("doc") ) {
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
                        ));
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
        client.admin().indices().prepareDelete(".triggered_watches").execute(ActionListener.wrap(deleteIndexResponse -> {
                    startWatcherIfNeeded(shouldStartWatcher, client, listener);
                }, e -> {
                    if (e instanceof IndexNotFoundException) {
                        startWatcherIfNeeded(shouldStartWatcher, client, listener);
                    } else {
                        listener.onFailure(e);
                    }
                }
        ));
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
