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
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps the managed index template that backs derived metrics destinations installed, in every project.
 *
 * <p>Destinations are created on demand by the first metric document written to them, which only works if a matching template already
 * exists. The template is therefore installed by the elected master as soon as cluster state is recovered, and reinstalled whenever the
 * version stored in it falls behind the version this node ships.
 */
public class DerivedMetricsTemplateRegistry implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DerivedMetricsTemplateRegistry.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Set<ProjectId> inFlight = ConcurrentHashMap.newKeySet();

    public DerivedMetricsTemplateRegistry(Client client, ClusterService clusterService) {
        this.client = new OriginSettingClient(client, DataStreamDerivedMetrics.DERIVED_METRICS_ORIGIN);
        this.clusterService = clusterService;
    }

    public void init() {
        clusterService.addListener(this);
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }
        for (ProjectMetadata project : event.state().metadata().projects().values()) {
            if (needsInstall(project) && inFlight.add(project.id())) {
                install(project.id());
            }
        }
    }

    private static boolean needsInstall(ProjectMetadata project) {
        ComposableIndexTemplate existing = project.templatesV2().get(DerivedMetricsDestination.TEMPLATE_NAME);
        if (existing == null) {
            return true;
        }
        Long version = existing.version();
        return version == null || version < DerivedMetricsDestination.TEMPLATE_VERSION;
    }

    private void install(ProjectId project) {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            DerivedMetricsDestination.TEMPLATE_NAME
        );
        request.indexTemplate(DerivedMetricsDestination.template());
        request.masterNodeTimeout(TimeValue.timeValueSeconds(30));
        client.projectClient(project).execute(TransportPutComposableIndexTemplateAction.TYPE, request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                inFlight.remove(project);
                logger.info(
                    "installed the derived metrics index template [{}] in project [{}]",
                    DerivedMetricsDestination.TEMPLATE_NAME,
                    project
                );
            }

            @Override
            public void onFailure(Exception e) {
                inFlight.remove(project);
                logger.warn(
                    () -> "failed to install the derived metrics index template ["
                        + DerivedMetricsDestination.TEMPLATE_NAME
                        + "] in project ["
                        + project
                        + "]",
                    e
                );
            }
        });
    }
}
