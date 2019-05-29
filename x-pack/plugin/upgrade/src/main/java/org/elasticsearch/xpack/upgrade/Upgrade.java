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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Allocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.template.TemplateUtils;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.upgrade.actions.TransportIndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.actions.TransportIndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeInfoAction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.clientWithOrigin;
import static org.elasticsearch.xpack.core.security.authc.esnative.NativeUserStoreField.INDEX_TYPE;
import static org.elasticsearch.xpack.core.security.authc.esnative.NativeUserStoreField.RESERVED_USER_TYPE;

public class Upgrade extends Plugin implements ActionPlugin {

    public static final Version UPGRADE_INTRODUCED = Version.V_5_6_0;

    // this is the required index.format setting for 6.0 services (watcher and security) to start up
    // this index setting is set by the upgrade API or automatically when a 6.0 index template is created
    private static final int EXPECTED_INDEX_FORMAT_VERSION = 6;

    private static final String SECURITY_INDEX_NAME = ".security";

    private final Settings settings;
    private final List<BiFunction<Client, ClusterService, IndexUpgradeCheck>> upgradeCheckFactories;

    public Upgrade(Settings settings) {
        this.settings = settings;
        this.upgradeCheckFactories = new ArrayList<>();
        upgradeCheckFactories.add(getWatchesIndexUpgradeCheckFactory(settings));
        upgradeCheckFactories.add(getTriggeredWatchesIndexUpgradeCheckFactory(settings));
        upgradeCheckFactories.add(getSecurityUpgradeCheckFactory(settings));
    }


    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        List<IndexUpgradeCheck> upgradeChecks = new ArrayList<>(upgradeCheckFactories.size());
        for (BiFunction<Client, ClusterService, IndexUpgradeCheck> checkFactory : upgradeCheckFactories) {
            upgradeChecks.add(checkFactory.apply(client, clusterService));
        }
        return Collections.singletonList(new IndexUpgradeService(settings, Collections.unmodifiableList(upgradeChecks)));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(IndexUpgradeInfoAction.INSTANCE, TransportIndexUpgradeInfoAction.class),
                new ActionHandler<>(IndexUpgradeAction.INSTANCE, TransportIndexUpgradeAction.class)
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

    /**
     * Checks the format of an internal index and returns true if the index is up to date or false if upgrade is required
     */
    public static boolean checkInternalIndexFormat(IndexMetaData indexMetaData) {
        return indexMetaData.getSettings().getAsInt(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), 0) == EXPECTED_INDEX_FORMAT_VERSION;
    }

    static BiFunction<Client, ClusterService, IndexUpgradeCheck> getSecurityUpgradeCheckFactory(Settings settings) {
        return (client, clusterService) -> {
            final Client clientWithOrigin = clientWithOrigin(client, SECURITY_ORIGIN);
            return new IndexUpgradeCheck<Void>("security",
                    indexMetaData -> {
                        if (".security".equals(indexMetaData.getIndex().getName())
                                || indexMetaData.getAliases().containsKey(".security")) {

                            if (checkInternalIndexFormat(indexMetaData)) {
                                return checkIndexNeedsReindex(indexMetaData);
                            } else {
                                return UpgradeActionRequired.UPGRADE;
                            }
                        } else {
                            return UpgradeActionRequired.NOT_APPLICABLE;
                        }
                    },
                    clientWithOrigin,
                    clusterService,
                    new String[]{"user", "reserved-user", "role", "doc"},
                    new Script(ScriptType.INLINE, "painless",
                            "ctx._source.type = ctx._type;\n" +
                                    "if (!ctx._type.equals(\"doc\")) {\n" +
                                    "   ctx._id = ctx._type + \"-\" + ctx._id;\n" +
                                    "   ctx._type = \"doc\";" +
                                    "}\n",
                            new HashMap<>()),
                    (cs, listener) -> {
                        if (isClusterRoutingAllocationEnabled(cs) == false) {
                            listener.onFailure(new ElasticsearchException(
                                    "pre-upgrade check failed, please enable cluster routing allocation using setting [{}]",
                                    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
                        } else {
                            listener.onResponse(null);
                        }
                    },
                    (success, listener) -> postSecurityUpgrade(clientWithOrigin, listener));
        };
    }

    private static void postSecurityUpgrade(Client client, ActionListener<TransportResponse.Empty> listener) {
        // update passwords to the new style, if they are in the old default password mechanism
        client.prepareSearch(SECURITY_INDEX_NAME)
              .setQuery(QueryBuilders.termQuery(User.Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE))
              .setFetchSource(true)
              .execute(ActionListener.wrap(searchResponse -> {
                  assert searchResponse.getHits().getTotalHits() <= 10 :
                     "there are more than 10 reserved users we need to change this to retrieve them all!";
                  Set<String> toConvert = new HashSet<>();
                  for (SearchHit searchHit : searchResponse.getHits()) {
                      Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                      if (hasOldStyleDefaultPassword(sourceMap)) {
                          toConvert.add(searchHit.getId());
                      }
                  }

                  if (toConvert.isEmpty()) {
                      listener.onResponse(TransportResponse.Empty.INSTANCE);
                  } else {
                      final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                      for (final String id : toConvert) {
                          final UpdateRequest updateRequest = new UpdateRequest(SECURITY_INDEX_NAME,
                                INDEX_TYPE, RESERVED_USER_TYPE + "-" + id);
                          updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                       .doc(User.Fields.PASSWORD.getPreferredName(), "",
                                            User.Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE);
                          bulkRequestBuilder.add(updateRequest);
                      }
                      bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                          @Override
                          public void onResponse(BulkResponse bulkItemResponses) {
                              if (bulkItemResponses.hasFailures()) {
                                  final String msg = "failed to update old style reserved user passwords: " +
                                                     bulkItemResponses.buildFailureMessage();
                                  listener.onFailure(new ElasticsearchException(msg));
                              } else {
                                  listener.onResponse(TransportResponse.Empty.INSTANCE);
                              }
                          }

                          @Override
                          public void onFailure(Exception e) {
                              listener.onFailure(e);
                          }
                      });
                  }
              }, listener::onFailure));
    }

    /**
     * Determines whether the supplied source as a {@link Map} has its password explicitly set to be the default password
     */
    private static boolean hasOldStyleDefaultPassword(Map<String, Object> userSource) {
        // TODO we should store the hash as something other than a string... bytes?
        final String passwordHash = (String) userSource.get(User.Fields.PASSWORD.getPreferredName());
        if (passwordHash == null) {
            throw new IllegalStateException("passwordHash should never be null");
        } else if (passwordHash.isEmpty()) {
            // we know empty is the new style
            return false;
        }

        try (SecureString secureString = new SecureString(passwordHash.toCharArray())) {
            return Hasher.BCRYPT.verify(new SecureString("".toCharArray()), secureString.getChars());
        }
    }

    static BiFunction<Client, ClusterService, IndexUpgradeCheck> getWatchesIndexUpgradeCheckFactory(Settings settings) {
        return (client, clusterService) -> {
            final Client clientWithOrigin = clientWithOrigin(client, WATCHER_ORIGIN);
            return new IndexUpgradeCheck<Boolean>("watches",
                    indexMetaData -> {
                        if (indexOrAliasExists(indexMetaData, ".watches")) {
                            if (checkInternalIndexFormat(indexMetaData)) {
                                return checkIndexNeedsReindex(indexMetaData);
                            } else {
                                return UpgradeActionRequired.UPGRADE;
                            }
                        } else {
                            return UpgradeActionRequired.NOT_APPLICABLE;
                        }
                    }, clientWithOrigin,
                    clusterService,
                    new String[]{"watch"},
                    new Script(ScriptType.INLINE, "painless", "ctx._type = \"doc\";\n" +
                            "if (ctx._source.containsKey(\"_status\") && !ctx._source.containsKey(\"status\")  ) {\n" +
                            "  ctx._source.status = ctx._source.remove(\"_status\");\n" +
                            "}",
                            new HashMap<>()),
                    (cs, booleanActionListener) -> preWatchesIndexUpgrade(clientWithOrigin, cs, booleanActionListener),
                    (shouldStartWatcher, listener) -> postWatchesIndexUpgrade(clientWithOrigin, shouldStartWatcher, listener)
            );
        };
    }

    static BiFunction<Client, ClusterService, IndexUpgradeCheck> getTriggeredWatchesIndexUpgradeCheckFactory(Settings settings) {
        return (client, clusterService) -> {
            final Client clientWithOrigin = clientWithOrigin(client, WATCHER_ORIGIN);
            return new IndexUpgradeCheck<Boolean>("triggered-watches",
                    indexMetaData -> {
                        if (indexOrAliasExists(indexMetaData, TriggeredWatchStoreField.INDEX_NAME)) {
                            if (checkInternalIndexFormat(indexMetaData)) {
                                return checkIndexNeedsReindex(indexMetaData);
                            } else {
                                return UpgradeActionRequired.UPGRADE;
                            }
                        } else {
                            return UpgradeActionRequired.NOT_APPLICABLE;
                        }
                    }, clientWithOrigin,
                    clusterService,
                    new String[]{"triggered-watch"},
                    new Script(ScriptType.INLINE, "painless", "ctx._type = \"doc\";\n", new HashMap<>()),
                    (cs, booleanActionListener) -> preTriggeredWatchesIndexUpgrade(clientWithOrigin, cs, booleanActionListener),
                    (shouldStartWatcher, listener) -> postWatchesIndexUpgrade(clientWithOrigin, shouldStartWatcher, listener)
            );
        };
    }

    private static UpgradeActionRequired checkIndexNeedsReindex(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0)) {
            return UpgradeActionRequired.REINDEX;
        } else {
            return UpgradeActionRequired.UP_TO_DATE;
        }
    }

    private static boolean indexOrAliasExists(IndexMetaData indexMetaData, String name) {
        return name.equals(indexMetaData.getIndex().getName()) || indexMetaData.getAliases().containsKey(name);
    }

    static void preTriggeredWatchesIndexUpgrade(Client client, ClusterState cs, ActionListener<Boolean> listener) {
        if (isClusterRoutingAllocationEnabled(cs) == false) {
            listener.onFailure(new ElasticsearchException(
                    "pre-upgrade check failed, please enable cluster routing allocation using setting [{}]",
                    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
        }
        new WatcherClient(client).prepareWatcherStats().execute(ActionListener.wrap(
                stats -> {
                    if (stats.watcherMetaData().manuallyStopped()) {
                        preTriggeredWatchesIndexUpgrade(client, listener, false);
                    } else {
                        new WatcherClient(client).prepareWatchService().stop().execute(ActionListener.wrap(
                                watcherServiceResponse -> {
                                    if (watcherServiceResponse.isAcknowledged()) {
                                        preTriggeredWatchesIndexUpgrade(client, listener, true);
                                    } else {
                                        listener.onFailure(new IllegalStateException("unable to stop watcher service"));
                                    }

                                },
                                listener::onFailure));
                    }
                },
                listener::onFailure));
    }

    static boolean isClusterRoutingAllocationEnabled(ClusterState cs) {
        Allocation clusterRoutingAllocation = EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING
                .get(cs.getMetaData().settings());
        if (Allocation.NONE == clusterRoutingAllocation) {
            return false;
        }
        return true;
    }

    private static void preTriggeredWatchesIndexUpgrade(final Client client, final ActionListener<Boolean> listener,
                                                        final boolean restart) {
        final String legacyTriggeredWatchesTemplateName = "triggered_watches";

        ActionListener<AcknowledgedResponse> returnToCallerListener =
                deleteIndexTemplateListener(legacyTriggeredWatchesTemplateName, listener, () -> listener.onResponse(restart));

        // step 2, after put new .triggered_watches template: delete triggered_watches index template, then return to caller
        ActionListener<AcknowledgedResponse> putTriggeredWatchesListener =
                putIndexTemplateListener(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME, listener,
                        () -> client.admin().indices().prepareDeleteTemplate(legacyTriggeredWatchesTemplateName)
                                .execute(returnToCallerListener));

        // step 1, put new .triggered_watches template
        final byte[] triggeredWatchesTemplate = TemplateUtils.loadTemplate("/triggered-watches.json",
                WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
                Pattern.quote("${xpack.watcher.template.version}")).getBytes(StandardCharsets.UTF_8);

        client.admin().indices().preparePutTemplate(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME)
                .setSource(triggeredWatchesTemplate, XContentType.JSON).execute(putTriggeredWatchesListener);
    }

    static void preWatchesIndexUpgrade(Client client, ClusterState cs, ActionListener<Boolean> listener) {
        if (isClusterRoutingAllocationEnabled(cs) == false) {
            listener.onFailure(new ElasticsearchException(
                    "pre-upgrade check failed, please enable cluster routing allocation using setting [{}]",
                    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()));
        }

        new WatcherClient(client).prepareWatcherStats().execute(ActionListener.wrap(
                    stats -> {
                        if (stats.watcherMetaData().manuallyStopped()) {
                            preWatchesIndexUpgrade(client, listener, false);
                        } else {
                            new WatcherClient(client).prepareWatchService().stop().execute(ActionListener.wrap(
                                    watcherServiceResponse -> {
                                        if (watcherServiceResponse.isAcknowledged()) {
                                            preWatchesIndexUpgrade(client, listener, true);
                                        } else {
                                            listener.onFailure(new IllegalStateException("unable to stop watcher service"));
                                        }

                                    },
                                    listener::onFailure));
                        }
                    },
                    listener::onFailure));
    }

    private static void preWatchesIndexUpgrade(final Client client, final ActionListener<Boolean> listener, final boolean restart) {
        final String legacyWatchesTemplateName = "watches";
        ActionListener<AcknowledgedResponse> returnToCallerListener =
                deleteIndexTemplateListener(legacyWatchesTemplateName, listener, () -> listener.onResponse(restart));

        // step 3, after put new .watches template: delete watches index template, then return to caller
        ActionListener<AcknowledgedResponse> putTriggeredWatchesListener =
                putIndexTemplateListener(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME, listener,
                        () -> client.admin().indices().prepareDeleteTemplate(legacyWatchesTemplateName)
                                .execute(returnToCallerListener));

        // step 2, after delete watch history templates: put new .watches template
        final byte[] watchesTemplate = TemplateUtils.loadTemplate("/watches.json",
                WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION,
                Pattern.quote("${xpack.watcher.template.version}")).getBytes(StandardCharsets.UTF_8);

        ActionListener<AcknowledgedResponse> deleteWatchHistoryTemplatesListener = deleteIndexTemplateListener("watch_history_*",
                listener,
                () -> client.admin().indices().preparePutTemplate(WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME)
                        .setSource(watchesTemplate, XContentType.JSON)
                        .execute(putTriggeredWatchesListener));

        // step 1, delete watch history index templates
        client.admin().indices().prepareDeleteTemplate("watch_history_*").execute(deleteWatchHistoryTemplatesListener);
    }

    static void postWatchesIndexUpgrade(Client client, Boolean shouldStartWatcher,
                                                ActionListener<TransportResponse.Empty> listener) {
        if (shouldStartWatcher) {
            WatcherClient watcherClient = new WatcherClient(client);
            watcherClient.prepareWatcherStats().execute(waitingStatsListener(0, listener, watcherClient));
        } else {
            listener.onResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    /*
     * With 6.x the watcher stats are possibly delayed. This means, even if you start watcher it might take a moment,
     * until all nodes have really started up watcher. In order to cater for this, we have to introduce a polling mechanism
     * for the stats.
     *
     * The reason for this is, that we may shut down watcher, then do the upgrade things, like deleting and recreating an index
     * and then watcher is still marked as STOPPING instead of being STOPPED, because the upgrade itself was too fast.
     *
     * To counter this, this action listener polls the stats with an increasing wait delay by 150ms up to 10 times.
     */
    private static ActionListener<WatcherStatsResponse> waitingStatsListener(int currentCount,
                                                                             ActionListener<TransportResponse.Empty> listener,
                                                                             WatcherClient watcherClient) {
        return ActionListener.wrap(r -> {
            boolean watcherStoppedOnAllNodes = r.getNodes().stream()
                    .map(WatcherStatsResponse.Node::getWatcherState)
                    .allMatch(s -> s == WatcherState.STOPPED);
            if (watcherStoppedOnAllNodes == false) {
                if (currentCount >= 10) {
                    listener.onFailure(new ElasticsearchException("watcher did not stop properly, so cannot start up again"));
                } else {
                    Thread.sleep(currentCount * 150);
                    watcherClient.prepareWatcherStats().execute(waitingStatsListener(currentCount + 1, listener, watcherClient));
                }
            } else {
                watcherClient.watcherService(new WatcherServiceRequest().start(), ActionListener.wrap(
                        serviceResponse -> listener.onResponse(TransportResponse.Empty.INSTANCE), listener::onFailure
                ));
            }

        }, listener::onFailure);
    }

    private static ActionListener<AcknowledgedResponse> putIndexTemplateListener(String name, ActionListener<Boolean> listener,
                                                                                 Runnable runnable) {
        return ActionListener.wrap(
                r -> {
                    if (r.isAcknowledged()) {
                        runnable.run();
                    } else {
                        listener.onFailure(new ElasticsearchException("Putting [{}] template was not acknowledged", name));
                    }
                },
                listener::onFailure);
    }

    private static ActionListener<AcknowledgedResponse> deleteIndexTemplateListener(String name, ActionListener<Boolean> listener,
                                                                                    Runnable runnable) {
        return ActionListener.wrap(
                r -> {
                    if (r.isAcknowledged()) {
                        runnable.run();
                    } else {
                        listener.onFailure(new ElasticsearchException("Deleting [{}] template was not acknowledged", name));
                    }
                },
                // if the index template we tried to delete is gone already, no need to worry
                e -> {
                    if (e instanceof IndexTemplateMissingException) {
                        runnable.run();
                    } else {
                        listener.onFailure(e);
                    }
                });
    }
}
