/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.common.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ExecutionContext {

    private final String id;
    private final Alert alert;
    private final DateTime executionTime;
    private final DateTime fireTime;
    private final DateTime scheduledTime;

    private Input.Result inputResult;
    private Condition.Result conditionResult;
    private Throttler.Result throttleResult;
    private Transform.Result transformResult;
    private Map<String, Action.Result> actionsResults = new HashMap<>();

    private Payload payload;

    public ExecutionContext(String id, Alert alert, DateTime executionTime, DateTime fireTime, DateTime scheduledTime) {
        this.id = id;
        this.alert = alert;
        this.executionTime = executionTime;
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
    }

    public String id() {
        return id;
    }

    public Alert alert() {
        return alert;
    }

    public DateTime executionTime() {
        return executionTime;
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

    public void onInputResult(Input.Result inputResult) {
        this.inputResult = inputResult;
        this.payload = inputResult.payload();
    }

    public Input.Result inputResult() {
        return inputResult;
    }

    public void onConditionResult(Condition.Result conditionResult) {
        alert.status().onCheck(conditionResult.met(), executionTime);
        this.conditionResult = conditionResult;
    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public void onThrottleResult(Throttler.Result throttleResult) {
        this.throttleResult = throttleResult;
        if (throttleResult.throttle()) {
            alert.status().onThrottle(executionTime, throttleResult.reason());
        } else {
            alert.status().onExecution(executionTime);
        }
    }

    public Throttler.Result throttleResult() {
        return throttleResult;
    }

    public void onTransformResult(Transform.Result transformResult) {
        this.transformResult = transformResult;
        this.payload = transformResult.payload();
    }

    public Transform.Result transformResult() {
        return transformResult;
    }

    public void onActionResult(Action.Result result) {
        actionsResults.put(result.type(), result);
    }

    public Map<String, Action.Result> actionsResults() {
        return actionsResults;
    }

    public AlertExecution finish() {
        return new AlertExecution(this);
    }

}
