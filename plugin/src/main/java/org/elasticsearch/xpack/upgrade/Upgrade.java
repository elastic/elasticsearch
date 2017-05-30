/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeInfoAction;

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
                getGenericCheckFactory(settings))) {
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

    static Tuple<Collection<String>, BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> getGenericCheckFactory(
            Settings settings) {
        return new Tuple<>(
                Collections.emptyList(),
                (internalClient, clusterService) ->
                        new IndexUpgradeCheck("generic", settings,
                                indexAndParams -> indexAndParams.v1().getCreationVersion().before(Version.V_5_0_0_alpha1),
                                UpgradeActionRequired.REINDEX,
                                UpgradeActionRequired.UP_TO_DATE));
    }

    static Tuple<Collection<String>, BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> getKibanaUpgradeCheckFactory(
            Settings settings) {
        return new Tuple<>(
                Collections.singletonList("kibana_indices"),
                (internalClient, clusterService) ->
                        new IndexUpgradeCheck("kibana",
                                settings,
                                internalClient,
                                clusterService,
                                indexAndParams -> {
                                    String indexName = indexAndParams.v1().getIndex().getName();
                                    String kibanaIndicesMasks = indexAndParams.v2().getOrDefault("kibana_indices", ".kibana");
                                    String[] kibanaIndices = Strings.delimitedListToStringArray(kibanaIndicesMasks, ",");
                                    return Regex.simpleMatch(kibanaIndices, indexName);
                                },
                                Strings.EMPTY_ARRAY,
                                new Script(ScriptType.INLINE, "painless", "ctx._id = ctx._type + \"-\" + ctx._id;\n" +
                                        "ctx._source = [ ctx._type : ctx._source ];\n" +
                                        "ctx._source.type = ctx._type;\n" +
                                        "ctx._type = \"doc\";",
                                        new HashMap<>())));
    }
}
