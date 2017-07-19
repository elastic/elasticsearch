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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.user.User;
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
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.INDEX_TYPE;
import static org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.RESERVED_USER_TYPE;

public class Upgrade implements ActionPlugin {

    public static final Version UPGRADE_INTRODUCED = Version.V_5_6_0;

    // this is the required index.format setting for 6.0 services (watcher and security) to start up
    // this index setting is set by the upgrade API or automatically when a 6.0 index template is created
    private static final int EXPECTED_INDEX_FORMAT_VERSION = 6;

    private final Settings settings;
    private final List<BiFunction<InternalClient, ClusterService, IndexUpgradeCheck>> upgradeCheckFactories;

    public Upgrade(Settings settings) {
        this.settings = settings;
        this.upgradeCheckFactories = new ArrayList<>();
        upgradeCheckFactories.add(getWatcherUpgradeCheckFactory(settings));
        upgradeCheckFactories.add(getSecurityUpgradeCheckFactory(settings));
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

    /**
     * Checks the format of an internal index and returns true if the index is up to date or false if upgrade is required
     */
    public static boolean checkInternalIndexFormat(IndexMetaData indexMetaData) {
        return indexMetaData.getSettings().getAsInt(IndexMetaData.INDEX_FORMAT_SETTING.getKey(), 0) == EXPECTED_INDEX_FORMAT_VERSION;
    }

    static BiFunction<InternalClient, ClusterService, IndexUpgradeCheck> getSecurityUpgradeCheckFactory(Settings settings) {
        return (internalClient, clusterService) ->
               new IndexUpgradeCheck<Void>("security",
                    settings,
                    indexMetaData -> {
                        if (".security".equals(indexMetaData.getIndex().getName())
                                || indexMetaData.getAliases().containsKey(".security")) {

                            if (checkInternalIndexFormat(indexMetaData)) {
                                return UpgradeActionRequired.UP_TO_DATE;
                            } else {
                                return UpgradeActionRequired.UPGRADE;
                            }
                        } else {
                            return UpgradeActionRequired.NOT_APPLICABLE;
                        }
                    },
                    internalClient,
                    clusterService,
                    new String[] { "user", "reserved-user", "role", "doc" },
                    new Script(ScriptType.INLINE, "painless",
                        "ctx._source.type = ctx._type;\n" +
                        "if (!ctx._type.equals(\"doc\")) {\n" +
                        "   ctx._id = ctx._type + \"-\" + ctx._id;\n" +
                        "   ctx._type = \"doc\";" +
                        "}\n",
                        new HashMap<>()),
                        listener -> listener.onResponse(null),
                        (success, listener) -> postSecurityUpgrade(internalClient, listener));
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
            return Hasher.BCRYPT.verify(ReservedRealm.EMPTY_PASSWORD_TEXT, secureString.getChars());
        }
    }

    static BiFunction<InternalClient, ClusterService, IndexUpgradeCheck> getWatcherUpgradeCheckFactory(Settings settings) {
        return (internalClient, clusterService) ->
                new IndexUpgradeCheck<Boolean>("watcher",
                        settings,
                        indexMetaData -> {
                            if (".watches".equals(indexMetaData.getIndex().getName()) ||
                                    indexMetaData.getAliases().containsKey(".watches")) {
                                if (checkInternalIndexFormat(indexMetaData)) {
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
        ActionListener<DeleteIndexTemplateResponse> triggeredWatchIndexTemplateListener = deleteIndexTemplateListener("triggered_watches",
                listener, () -> listener.onResponse(true));

        ActionListener<DeleteIndexTemplateResponse> watchIndexTemplateListener = deleteIndexTemplateListener("watches", listener,
                () -> client.admin().indices().prepareDeleteTemplate("triggered_watches").execute(triggeredWatchIndexTemplateListener));

        new WatcherClient(client).watcherStats(new WatcherStatsRequest(), ActionListener.wrap(
                stats -> {
                    if (stats.watcherMetaData().manuallyStopped()) {
                        // don't start watcher after upgrade
                        listener.onResponse(false);
                    } else {
                        // stop watcher
                        new WatcherClient(client).watcherService(new WatcherServiceRequest().stop(), ActionListener.wrap(
                                stopResponse -> {
                                    if (stopResponse.isAcknowledged()) {
                                        // delete old templates before indexing
                                        client.admin().indices().prepareDeleteTemplate("watches").execute(watchIndexTemplateListener);
                                    } else {
                                        listener.onFailure(new IllegalStateException("unable to stop watcher service"));
                                    }
                                }, listener::onFailure
                        ));
                    }
                }, listener::onFailure));
    }

    private static void postWatcherUpgrade(Client client, Boolean shouldStartWatcher, ActionListener<TransportResponse.Empty> listener) {
        ActionListener<DeleteIndexResponse> deleteTriggeredWatchIndexResponse = ActionListener.wrap(deleteIndexResponse ->
                startWatcherIfNeeded(shouldStartWatcher, client, listener), e -> {
            if (e instanceof IndexNotFoundException) {
                startWatcherIfNeeded(shouldStartWatcher, client, listener);
            } else {
                listener.onFailure(e);
            }
        });

        client.admin().indices().prepareDelete(".triggered_watches").execute(deleteTriggeredWatchIndexResponse);
    }

    private static ActionListener<DeleteIndexTemplateResponse> deleteIndexTemplateListener(String name, ActionListener<Boolean> listener,
                                                                                           Runnable runnable) {
        return ActionListener.wrap(r -> {
            if (r.isAcknowledged()) {
                runnable.run();
            } else {
                listener.onFailure(new ElasticsearchException("Deleting [{}] template was not acknowledged", name));
            }
        }, listener::onFailure);
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
