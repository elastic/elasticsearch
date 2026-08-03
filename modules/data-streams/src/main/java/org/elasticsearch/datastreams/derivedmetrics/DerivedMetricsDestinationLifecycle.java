/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Gives each derived metrics destination a lifecycle, once, when it first appears.
 *
 * <p>Destinations are auto-created by the first document written to them, from one shared index template. A per-source, per-interval
 * retention cannot live in that template, so it is applied to the destination data stream after it exists.
 *
 * <p>This applies the lifecycle <em>once</em> and never revisits it. The configuration on the source stream is not continuously
 * reconciled, so a lifecycle changed by hand on a destination stays changed. The trade is the other way round too: changing
 * {@code derived_metrics.destinations} does not retroactively alter destinations that already exist. Removing a destination's lifecycle
 * entirely makes it eligible again, since "has no lifecycle" is the only signal that it was never set up.
 */
public class DerivedMetricsDestinationLifecycle implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsDestinationLifecycle.class);

    /**
     * Used when neither the destination nor the cluster declares a retention, so that a derived metrics destination is never unbounded
     * by accident.
     */
    public static final TimeValue FALLBACK_RETENTION = TimeValue.timeValueDays(30);

    private final Client client;
    private final ClusterService clusterService;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public DerivedMetricsDestinationLifecycle(
        Client client,
        ClusterService clusterService,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        this.client = new OriginSettingClient(client, DataStreamDerivedMetrics.DERIVED_METRICS_ORIGIN);
        this.clusterService = clusterService;
        this.globalRetentionSettings = globalRetentionSettings;
    }

    public void init() {
        clusterService.addListener(this);
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || event.localNodeMaster() == false) {
            return;
        }
        for (ProjectMetadata project : event.state().metadata().projects().values()) {
            for (DataStream dataStream : project.dataStreams().values()) {
                if (DerivedMetricsDestination.isDestination(dataStream.getName()) == false || dataStream.getDataLifecycle() != null) {
                    continue;
                }
                DataStreamLifecycle lifecycle = resolveLifecycle(project, dataStream.getName());
                if (lifecycle != null && inFlight.add(dataStream.getName())) {
                    apply(project.id(), dataStream.getName(), lifecycle);
                }
            }
        }
    }

    /**
     * Works back from a destination name to the configuration that produced it. Returns null when no source claims the destination,
     * which happens for a stream whose source has been deleted; those are left alone to expire on whatever they already have.
     */
    @Nullable
    private DataStreamLifecycle resolveLifecycle(ProjectMetadata project, String destination) {
        String withoutPrefix = destination.substring(DerivedMetricsDestination.DESTINATION_PREFIX.length());
        int separator = withoutPrefix.lastIndexOf('-');
        if (separator < 0) {
            return null;
        }
        String sourceName = withoutPrefix.substring(0, separator);
        String interval = withoutPrefix.substring(separator + 1);

        DataStream source = project.dataStreams().get(sourceName);
        if (source == null) {
            return null;
        }
        DataStreamDerivedMetrics config = source.getDataStreamOptions().derivedMetrics();
        if (config == null) {
            return null;
        }
        TimeValue parsed;
        try {
            parsed = TimeValue.parseTimeValue(interval, "interval");
        } catch (IllegalArgumentException e) {
            return null;
        }
        DataStreamDerivedMetrics.Destination destination0 = config.destinationFor(parsed);
        if (destination0 != null && destination0.lifecycle() != null) {
            return destination0.lifecycle().toDataStreamLifecycle();
        }
        TimeValue retention = globalRetentionSettings.getDefaultRetention();
        return DataStreamLifecycle.dataLifecycleBuilder().dataRetention(retention == null ? FALLBACK_RETENTION : retention).build();
    }

    private void apply(ProjectId project, String destination, DataStreamLifecycle lifecycle) {
        PutDataStreamLifecycleAction.Request request = new PutDataStreamLifecycleAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30),
            new String[] { destination },
            lifecycle
        );
        client.projectClient(project).execute(PutDataStreamLifecycleAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                inFlight.remove(destination);
                logger.info("applied the derived metrics lifecycle to [{}]: {}", destination, lifecycle);
            }

            @Override
            public void onFailure(Exception e) {
                inFlight.remove(destination);
                logger.warn(() -> "failed to apply the derived metrics lifecycle to [" + destination + "]", e);
            }
        });
    }
}
