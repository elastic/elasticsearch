/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

/**
 * Reports basic availability and enablement for the {@code columnar} index mode.
 * <p>
 * The {@code enabled} flag reflects {@code cluster.columnar.enabled}, the cluster-level gate
 * that prevents creation of any strict-columnar index when {@code false}. Note that
 * {@code cluster.logsdb_columnar.enabled} is a testing-only knob (gated on snapshot builds)
 * and is intentionally not used here.
 */
public class ColumnarInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final ClusterService clusterService;

    @Inject
    public ColumnarInfoTransportAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.COLUMNAR.name(), transportService, actionFilters);
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.COLUMNAR;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return clusterService.getClusterSettings().get(LogsDBPlugin.CLUSTER_COLUMNAR_ENABLED);
    }
}
