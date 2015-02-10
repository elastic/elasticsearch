/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.history.FiredAlert;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class AlertContext {

    private final String runId;
    private final Alert alert;
    private final DateTime fireTime;
    private final DateTime scheduledTime;

    private Payload payload;
    private Trigger.Result triggerResult;
    private Throttler.Result throttleResult;
    private Transform.Result transformResult;
    private Map<String, Action.Result> actionsResults = new HashMap<>();

    public AlertContext(Alert alert, DateTime fireTime, DateTime scheduledTime) {
        this.runId = FiredAlert.firedAlertId(alert, scheduledTime);
        this.alert = alert;
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
    }

    public String runId() {
        return runId;
    }

    public Alert alert() {
        return alert;
    }

    public DateTime fireTime() {
        return fireTime;
    }

    public DateTime scheduledTime() {
        return scheduledTime;
    }

    public Payload payload() {
        return payload;
    }

    public void triggerResult(Trigger.Result triggerResult) {
        this.triggerResult = triggerResult;
        this.payload = triggerResult.payload();
    }

    public Trigger.Result triggerResult() {
        return triggerResult;
    }

    public void throttleResult(Throttler.Result throttleResult) {
        this.throttleResult = throttleResult;
    }

    public Throttler.Result throttleResult() {
        return throttleResult;
    }

    public void transformResult(Transform.Result transformResult) {
        this.transformResult = transformResult;
        this.payload = transformResult.payload();
    }

    public Transform.Result transformResult() {
        return transformResult;
    }

    public void addActionResult(Action.Result result) {
        actionsResults.put(result.type(), result);
    }

    public Map<String, Action.Result> actionsResults() {
        return actionsResults;
    }

}
