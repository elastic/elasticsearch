/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.ActionType;

public class MonitoringBulkAction extends ActionType<MonitoringBulkResponse> {

    public static final MonitoringBulkAction INSTANCE = new MonitoringBulkAction();
    public static final String NAME = "cluster:admin/xpack/monitoring/bulk";

    private MonitoringBulkAction() {
        super(NAME, MonitoringBulkResponse::new);
    }
}
