/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;


import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public interface AlertActionFactory {

    AlertAction createAction(XContentParser parser) throws IOException;

    public boolean doAction(AlertAction action, Alert alert, AlertsService.AlertRun alertRun);

}
