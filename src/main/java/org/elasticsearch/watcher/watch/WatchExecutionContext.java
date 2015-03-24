/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.common.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class WatchExecutionContext {

    private final String id;
    private final Watch watch;
    private final DateTime executionTime;
    private final DateTime fireTime;
    private final DateTime scheduledTime;

    private Input.Result inputResult;
    private Condition.Result conditionResult;
    private Throttler.Result throttleResult;
    private Transform.Result transformResult;
    private Map<String, Action.Result> actionsResults = new HashMap<>();

    private Payload payload;

    public WatchExecutionContext(String id, Watch watch, DateTime executionTime, DateTime fireTime, DateTime scheduledTime) {
        this.id = id;
        this.watch = watch;
        this.executionTime = executionTime;
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
    }

    public String id() {
        return id;
    }

    public Watch watch() {
        return watch;
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
        watch.status().onCheck(conditionResult.met(), executionTime);
        this.conditionResult = conditionResult;
    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public void onThrottleResult(Throttler.Result throttleResult) {
        this.throttleResult = throttleResult;
        if (throttleResult.throttle()) {
            watch.status().onThrottle(executionTime, throttleResult.reason());
        } else {
            watch.status().onExecution(executionTime);
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

    public WatchExecution finish() {
        return new WatchExecution(this);
    }

}
