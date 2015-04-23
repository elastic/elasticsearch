/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecution;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class WatchExecutionContext {

    private final Wid id;
    private final Watch watch;
    private final DateTime executionTime;
    private final TriggerEvent triggerEvent;

    private Input.Result inputResult;
    private Condition.Result conditionResult;
    private Throttler.Result throttleResult;
    private Transform.Result transformResult;
    private Map<String, ActionWrapper.Result> actionsResults = new HashMap<>();

    private Payload payload;

    public WatchExecutionContext(Watch watch, DateTime executionTime, TriggerEvent triggerEvent) {
        this.watch = watch;
        this.executionTime = executionTime;
        this.triggerEvent = triggerEvent;
        this.id = new Wid(watch.id(), watch.nonce(), executionTime);
    }

    /**
     * @return true if this action should be simulated
     */
    public abstract boolean simulateAction(String actionId);

    /**
     * @return true if this execution should be recorded in the .watch_history index
     */
    public abstract boolean recordExecution();

    public Wid id() {
        return id;
    }

    public Watch watch() {
        return watch;
    }

    public DateTime executionTime() {
        return executionTime;
    }

    public TriggerEvent triggerEvent() {
        return triggerEvent;
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
        this.conditionResult = conditionResult;
        if (recordExecution()) {
            watch.status().onCheck(conditionResult.met(), executionTime);
        }

    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public void onThrottleResult(Throttler.Result throttleResult) {
        this.throttleResult = throttleResult;
        if (recordExecution()) {
            if (throttleResult.throttle()) {
                watch.status().onThrottle(executionTime, throttleResult.reason());
            } else {
                watch.status().onExecution(executionTime);
            }
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

    public void onActionResult(ActionWrapper.Result result) {
        actionsResults.put(result.id(), result);
    }

    public ExecutableActions.Results actionsResults() {
        return new ExecutableActions.Results(actionsResults);
    }

    public WatchExecution finish() {
        return new WatchExecution(this);
    }
}
