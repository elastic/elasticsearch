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

public class LogsDBInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final ClusterService clusterService;

    @Inject
    public LogsDBInfoTransportAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.LOGSDB.name(), transportService, actionFilters);
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.LOGSDB;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return LogsDBPlugin.CLUSTER_LOGSDB_ENABLED.get(clusterService.getSettings());
    }
}
