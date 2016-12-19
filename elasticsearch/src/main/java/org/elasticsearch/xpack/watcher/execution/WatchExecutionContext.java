/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.condition.Condition;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.input.Input;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public abstract class WatchExecutionContext {

    private final Wid id;
    private final Watch watch;
    private final DateTime executionTime;
    private final TriggerEvent triggerEvent;
    private final TimeValue defaultThrottlePeriod;

    private ExecutionPhase phase = ExecutionPhase.AWAITS_EXECUTION;
    private long startTimestamp;

    private Payload payload;
    private Map<String, Object> vars = new HashMap<>();

    private Input.Result inputResult;
    private Condition.Result conditionResult;
    private Transform.Result transformResult;
    private ConcurrentMap<String, ActionWrapper.Result> actionsResults = ConcurrentCollections.newConcurrentMap();

    public WatchExecutionContext(Watch watch, DateTime executionTime, TriggerEvent triggerEvent, TimeValue defaultThrottlePeriod) {
        this.id = new Wid(watch.id(), executionTime);
        this.watch = watch;
        this.executionTime = executionTime;
        this.triggerEvent = triggerEvent;
        this.defaultThrottlePeriod = defaultThrottlePeriod;
    }

    /**
     * @return true if the watch associated with this context is known to watcher (i.e. it's stored
     *              in watcher. This plays a key role in how we handle execution. For example, if
     *              the watch is known, but then the watch is not there (perhaps deleted in between)
     *              we abort execution. It also plays a part (along with {@link #recordExecution()}
     *              in the decision of whether the watch record should be stored and if the watch
     *              status should be updated.
     */
    public abstract boolean knownWatch();

    /**
     * @return true if this action should be simulated
     */
    public abstract boolean simulateAction(String actionId);

    public abstract boolean skipThrottling(String actionId);

    /**
     * @return true if this execution should be recorded in the .watcher-history index
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

    /**
     * @return The default throttle period in the system.
     */
    public TimeValue defaultThrottlePeriod() {
        return defaultThrottlePeriod;
    }

    public boolean overrideRecordOnConflict() {
        return false;
    }

    public TriggerEvent triggerEvent() {
        return triggerEvent;
    }

    public Payload payload() {
        return payload;
    }

    public Map<String, Object> vars() {
        return vars;
    }

    public ExecutionPhase executionPhase() {
        return phase;
    }

    public void start() {
        assert phase == ExecutionPhase.AWAITS_EXECUTION;
        startTimestamp = System.currentTimeMillis();
        phase = ExecutionPhase.STARTED;
    }

    public void beforeInput() {
        assert phase == ExecutionPhase.STARTED;
        phase = ExecutionPhase.INPUT;
    }

    public void onInputResult(Input.Result inputResult) {
        assert !phase.sealed();
        this.inputResult = inputResult;
        if (inputResult.status() == Input.Result.Status.SUCCESS) {
            this.payload = inputResult.payload();
        }
    }

    public Input.Result inputResult() {
        return inputResult;
    }

    public void beforeCondition() {
        assert phase == ExecutionPhase.INPUT;
        phase = ExecutionPhase.CONDITION;
    }

    public void onConditionResult(Condition.Result conditionResult) {
        assert !phase.sealed();
        this.conditionResult = conditionResult;
        if (recordExecution()) {
            watch.status().onCheck(conditionResult.met(), executionTime);
        }
    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public void beforeWatchTransform() {
        assert phase == ExecutionPhase.CONDITION;
        this.phase = ExecutionPhase.WATCH_TRANSFORM;
    }

    public void onWatchTransformResult(Transform.Result result) {
        assert !phase.sealed();
        this.transformResult = result;
        if (result.status() == Transform.Result.Status.SUCCESS) {
            this.payload = result.payload();
        }
    }

    public Transform.Result transformResult() {
        return transformResult;
    }

    public void beforeActions() {
        assert phase == ExecutionPhase.CONDITION || phase == ExecutionPhase.WATCH_TRANSFORM;
        phase = ExecutionPhase.ACTIONS;
    }

    public void onActionResult(ActionWrapper.Result result) {
        assert !phase.sealed();
        actionsResults.put(result.id(), result);
        if (recordExecution()) {
            watch.status().onActionResult(result.id(), executionTime, result.action());
        }
    }

    public Map<String, ActionWrapper.Result> actionsResults() {
        return Collections.unmodifiableMap(actionsResults);
    }

    public WatchRecord abortBeforeExecution(ExecutionState state, String message) {
        assert !phase.sealed();
        phase = ExecutionPhase.ABORTED;
        return new WatchRecord.MessageWatchRecord(id, triggerEvent, state, message);
    }

    public WatchRecord abortFailedExecution(String message) {
        assert !phase.sealed();
        phase = ExecutionPhase.ABORTED;
        long executionFinishMs = System.currentTimeMillis();
        WatchExecutionResult result = new WatchExecutionResult(this, executionFinishMs - startTimestamp);
        return new WatchRecord.MessageWatchRecord(this, result, message);
    }

    public WatchRecord abortFailedExecution(Exception e) {
        assert !phase.sealed();
        phase = ExecutionPhase.ABORTED;
        long executionFinishMs = System.currentTimeMillis();
        WatchExecutionResult result = new WatchExecutionResult(this, executionFinishMs - startTimestamp);
        return new WatchRecord.ExceptionWatchRecord(this, result, e);
    }

    public WatchRecord finish() {
        assert !phase.sealed();
        phase = ExecutionPhase.FINISHED;
        long executionFinishMs = System.currentTimeMillis();
        WatchExecutionResult result = new WatchExecutionResult(this, executionFinishMs - startTimestamp);
        return new WatchRecord.MessageWatchRecord(this, result);
    }

    public WatchExecutionSnapshot createSnapshot(Thread executionThread) {
        return new WatchExecutionSnapshot(this, executionThread.getStackTrace());
    }

}
