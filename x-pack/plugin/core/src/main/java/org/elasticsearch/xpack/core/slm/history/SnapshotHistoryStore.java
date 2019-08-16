/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.slm.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;

/**
 * Records Snapshot Lifecycle Management actions as represented by {@link SnapshotHistoryItem} into an index
 * for the purposes of querying and alerting.
 */
public class SnapshotHistoryStore {
    private static final Logger logger = LogManager.getLogger(SnapshotHistoryStore.class);

    public static final String SLM_HISTORY_INDEX_PREFIX = ".slm-history-" + INDEX_TEMPLATE_VERSION + "-";
    public static final String SLM_HISTORY_ALIAS = ".slm-history-" + INDEX_TEMPLATE_VERSION;

    private final Client client;
    private final ClusterService clusterService;
    private final boolean slmHistoryEnabled;

    public SnapshotHistoryStore(Settings nodeSettings, Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        slmHistoryEnabled = SLM_HISTORY_INDEX_ENABLED_SETTING.get(nodeSettings);
    }

    /**
     * Attempts to asynchronously index a snapshot lifecycle management history entry
     *
     * @param item The entry to index
     */
    public void putAsync(SnapshotHistoryItem item) {
        if (slmHistoryEnabled == false) {
            logger.trace("not recording snapshot history item because [{}] is [false]: [{}]",
                SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), item);
            return;
        }
        logger.trace("about to index snapshot history item in index [{}]: [{}]", SLM_HISTORY_ALIAS, item);
        bootstrap(client, clusterService, () -> {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                item.toXContent(builder, ToXContent.EMPTY_PARAMS);
                IndexRequest request = new IndexRequest(SLM_HISTORY_ALIAS)
                    .source(builder);
                client.index(request, ActionListener.wrap(indexResponse -> {
                    logger.debug("successfully indexed snapshot history item with id [{}] in index [{}]: [{}]",
                        indexResponse.getId(), SLM_HISTORY_ALIAS, item);
                }, exception -> {
                    logger.error(new ParameterizedMessage("failed to index snapshot history item in index [{}]: [{}]",
                        SLM_HISTORY_ALIAS, item), exception);
                }));
            } catch (IOException exception) {
                logger.error(new ParameterizedMessage("failed to index snapshot history item in index [{}]: [{}]",
                    SLM_HISTORY_ALIAS, item), exception);
            }
        });
    }

    private static void bootstrap(Client client, ClusterService clusterService, Runnable onBootstrapped) {
        ClusterState state = clusterService.state();
        boolean aliasExists = state.metaData().hasAlias(SLM_HISTORY_ALIAS);

        if (aliasExists) {
            // Assume everything is fine
            onBootstrapped.run();
            return;
        }

        if (state.metaData().hasIndex(SLM_HISTORY_ALIAS)) {
            logger.error("SLM history alias already exists as concrete index");
            return;
        }

        client.admin().indices().prepareCreate(SLM_HISTORY_INDEX_PREFIX + "000001")
            .setWaitForActiveShards(1)
            .addAlias(new Alias(SLM_HISTORY_ALIAS)
                .writeIndex(true))
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    onBootstrapped.run();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to bootstrap SLM history index", e);
                }
            });
    }
}
