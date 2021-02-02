/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.ActionType;

public class MonitoringMigrateAlertsAction extends ActionType<MonitoringMigrateAlertsResponse> {

    public static final MonitoringMigrateAlertsAction INSTANCE = new MonitoringMigrateAlertsAction();
    public static final String NAME = "cluster:admin/xpack/monitoring/migrate/alerts";

    public MonitoringMigrateAlertsAction() {
        super(NAME, MonitoringMigrateAlertsResponse::new);
    }
}
