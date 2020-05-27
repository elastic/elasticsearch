/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
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
        ensureHistoryIndex(client, clusterService.state(), ActionListener.wrap(createdIndex -> {
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
        }, ex -> logger.error(new ParameterizedMessage("failed to ensure SLM history index exists, not indexing history item [{}]",
            item), ex)));
    }

    /**
     * Checks if the SLM history index exists, and if not, creates it.
     *
     * @param client  The client to use to create the index if needed
     * @param state   The current cluster state, to determine if the alias exists
     * @param andThen Called after the index has been created. `onResponse` called with `true` if the index was created,
     *                `false` if it already existed.
     */
    static void ensureHistoryIndex(Client client, ClusterState state, ActionListener<Boolean> andThen) {
        final String initialHistoryIndexName = SLM_HISTORY_INDEX_PREFIX + "000001";
        final IndexAbstraction slmHistory = state.metadata().getIndicesLookup().get(SLM_HISTORY_ALIAS);
        final IndexAbstraction initialHistoryIndex = state.metadata().getIndicesLookup().get(initialHistoryIndexName);

        if (slmHistory == null && initialHistoryIndex == null) {
            // No alias or index exists with the expected names, so create the index with appropriate alias
            client.admin().indices().prepareCreate(initialHistoryIndexName)
                .setWaitForActiveShards(1)
                .addAlias(new Alias(SLM_HISTORY_ALIAS)
                    .writeIndex(true)
                    .isHidden(true))
                .execute(new ActionListener<>() {
                    @Override
                    public void onResponse(CreateIndexResponse response) {
                        andThen.onResponse(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException) {
                            // The index didn't exist before we made the call, there was probably a race - just ignore this
                            logger.debug("index [{}] was created after checking for its existence, likely due to a concurrent call",
                                initialHistoryIndexName);
                            andThen.onResponse(false);
                        } else {
                            andThen.onFailure(e);
                        }
                    }
                });
        } else if (slmHistory == null) {
            // alias does not exist but initial index does, something is broken
            andThen.onFailure(new IllegalStateException("SLM history index [" + initialHistoryIndexName +
                "] already exists but does not have alias [" + SLM_HISTORY_ALIAS + "]"));
        } else if (slmHistory.getType() == IndexAbstraction.Type.ALIAS) {
            if (slmHistory.getWriteIndex() != null) {
                // The alias exists and has a write index, so we're good
                andThen.onResponse(false);
            } else {
                // The alias does not have a write index, so we can't index into it
                andThen.onFailure(new IllegalStateException("SLM history alias [" + SLM_HISTORY_ALIAS + "does not have a write index"));
            }
        } else if (slmHistory.getType() != IndexAbstraction.Type.ALIAS) {
            // This is not an alias, error out
            andThen.onFailure(new IllegalStateException("SLM history alias [" + SLM_HISTORY_ALIAS +
                "] already exists as " + slmHistory.getType().getDisplayName()));
        } else {
            logger.error("unexpected IndexOrAlias for [{}]: [{}]", SLM_HISTORY_ALIAS, slmHistory);
            // (slmHistory.isAlias() == true) but (slmHistory instanceof Alias == false)?
            assert false : SLM_HISTORY_ALIAS + " cannot be both an alias and not an alias simultaneously";
        }
    }
}
