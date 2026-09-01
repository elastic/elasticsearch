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
        if (allViewsUpToDate()) {
            return;
        }
        if (creationInProgress.compareAndSet(false, true) == false) {
            return;
        }
        threadPool.generic().execute(this::createViews);
    }

    private boolean allViewsUpToDate() {
        for (Map.Entry<String, String> entry : VIEWS.entrySet()) {
            if (isViewUpToDate(entry.getKey(), entry.getValue()) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean isViewUpToDate(String name, String query) {
        View existingView = viewService.get(Metadata.DEFAULT_PROJECT_ID, name);
        return existingView != null && query.equals(existingView.query());
    }

    private void createViews() {
        try (var refs = new RefCountingRunnable(() -> creationInProgress.set(false))) {
            for (Map.Entry<String, String> entry : VIEWS.entrySet()) {
                String name = entry.getKey();
                String query = entry.getValue();
                if (isViewUpToDate(name, query)) {
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
                            acknowledged -> logger.info("created ES|QL system view [{}]", name),
                            e -> logger.warn(() -> "failed to create ES|QL system view [" + name + "]", e)
                        ),
                        ref::close
                    )
                );
            }
        }
    }
}
