/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ensures the system ES|QL views exist and are up to date.
 */
public final class SystemViews implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemViews.class);

    /**
     * The system views keyed by view name with the backing ES|QL query as the value.
     */
    static final Map<String, String> VIEWS = Map.of(".ml-anomalies", """
        FROM .ml-anomalies-*, *:.ml-anomalies-*
        | WHERE result_type IN ("bucket", "record", "influencer")
        | EVAL score = COALESCE(anomaly_score, record_score, influencer_score),
               initial_score = COALESCE(initial_anomaly_score, initial_record_score, initial_influencer_score)
        | KEEP timestamp,
               event.ingested,
               job_id,
               result_type,
               is_interim,
               score,
               initial_score,
               bucket_span,
               function,
               function_description,
               field_name,
               by_field_name,
               by_field_value,
               over_field_name,
               over_field_value,
               partition_field_name,
               partition_field_value,
               influencer_field_name,
               influencer_field_value,
               detector_index,
               actual,
               typical,
               event_count,
               processing_time_ms
        """);

    /**
     * System views that are new in this version of Elasticsearch.
     * If any of these already exist in the cluster, the node will fail to start,
     * and the user must delete them and restart the node to recreate them.
     */
    private static final Set<String> NEW_SYSTEM_VIEWS = Set.of(".ml-anomalies");

    static boolean isSystemView(String name) {
        return VIEWS.containsKey(name);
    }

    private final ThreadPool threadPool;
    private final ViewService viewService;

    private final AtomicBoolean creationInProgress = new AtomicBoolean(false);

    public SystemViews(ClusterService clusterService, ThreadPool threadPool, ViewService viewService) {
        this.threadPool = threadPool;
        this.viewService = viewService;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }
        // Fail fast (synchronously) if a "new" system view already exists with a different definition: it must not be
        // silently overwritten. This runs on the cluster applier thread so the error surfaces immediately rather than as
        // an uncaught exception from the async creation task.
        failIfConflictingNewSystemViewExists();
        if (allViewsUpToDate()) {
            return;
        }
        if (creationInProgress.compareAndSet(false, true) == false) {
            return;
        }
        threadPool.generic().execute(this::createViews);
    }

    /**
     * Throws if a {@link #NEW_SYSTEM_VIEWS new} system view already exists with a query that differs from the managed
     * definition (e.g. a user-defined view with the same name). Such a view must be deleted and the node restarted for
     * the system view to be (re)created; we never overwrite it automatically.
     */
    private void failIfConflictingNewSystemViewExists() {
        for (String name : NEW_SYSTEM_VIEWS) {
            String existing = existingQuery(name);
            if (existing != null && VIEWS.get(name).equals(existing) == false) {
                throw new IllegalStateException(
                    "ES|QL system view [" + name + "] already exists. Please delete it and restart the node to recreate it."
                );
            }
        }
    }

    private boolean allViewsUpToDate() {
        for (Map.Entry<String, String> entry : VIEWS.entrySet()) {
            if (entry.getValue().equals(existingQuery(entry.getKey())) == false) {
                return false;
            }
        }
        return true;
    }

    private String existingQuery(String name) {
        View existingView = viewService.get(Metadata.DEFAULT_PROJECT_ID, name);
        return existingView != null ? existingView.query() : null;
    }

    private void createViews() {
        try (var refs = new RefCountingRunnable(() -> creationInProgress.set(false))) {
            for (Map.Entry<String, String> entry : VIEWS.entrySet()) {
                String name = entry.getKey();
                String query = entry.getValue();
                if (query.equals(existingQuery(name))) {
                    continue;
                }
                Releasable ref = refs.acquire();
                PutViewAction.Request request = new PutViewAction.Request(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    new View(name, query)
                );
                viewService.putView(
                    Metadata.DEFAULT_PROJECT_ID,
                    request,
                    ActionListener.runAfter(
                        ActionListener.wrap(
                            acknowledged -> logger.info("created/updated ES|QL system view [{}]", name),
                            e -> logger.warn(() -> "failed to create ES|QL system view [" + name + "]", e)
                        ),
                        ref::close
                    )
                );
            }
        }
    }
}
