/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class ManualExecutionContext extends WatchExecutionContext {

    private final Map<String, ActionExecutionMode> actionModes;
    private final boolean recordExecution;
    private final boolean knownWatch;

    ManualExecutionContext(
        Watch watch,
        boolean knownWatch,
        ZonedDateTime executionTime,
        ManualTriggerEvent triggerEvent,
        TimeValue defaultThrottlePeriod,
        Input.Result inputResult,
        Condition.Result conditionResult,
        Map<String, ActionExecutionMode> actionModes,
        boolean recordExecution
    ) throws Exception {

        super(watch.id(), executionTime, triggerEvent, defaultThrottlePeriod);

        this.actionModes = actionModes;
        this.recordExecution = recordExecution;
        this.knownWatch = knownWatch;

        // set the watch early to ensure calls to watch() below succeed.
        super.ensureWatchExists(() -> watch);

        if (inputResult != null) {
            onInputResult(inputResult);
        }
        if (conditionResult != null) {
            onConditionResult(conditionResult);
        }
        ActionExecutionMode allMode = actionModes.get(Builder.ALL);
        if (allMode == null || allMode == ActionExecutionMode.SKIP) {
            boolean throttleAll = allMode == ActionExecutionMode.SKIP;
            for (ActionWrapper action : watch.actions()) {
                if (throttleAll) {
                    onActionResult(
                        new ActionWrapperResult(action.id(), new Action.Result.Throttled(action.action().type(), "manually skipped"))
                    );
                } else {
                    ActionExecutionMode mode = actionModes.get(action.id());
                    if (mode == ActionExecutionMode.SKIP) {
                        onActionResult(
                            new ActionWrapperResult(action.id(), new Action.Result.Throttled(action.action().type(), "manually skipped"))
                        );
                    }
                }
            }
        }
    }

    @Override
    public boolean knownWatch() {
        return knownWatch;
    }

    @Override
    public final boolean simulateAction(String actionId) {
        ActionExecutionMode mode = actionModes.get(Builder.ALL);
        if (mode == null) {
            mode = actionModes.get(actionId);
        }
        return mode != null && mode.simulate();
    }

    @Override
    public boolean skipThrottling(String actionId) {
        ActionExecutionMode mode = actionModes.get(Builder.ALL);
        if (mode == null) {
            mode = actionModes.get(actionId);
        }
        return mode != null && mode.force();
    }

    @Override
    public boolean shouldBeExecuted() {
        // we always want to execute a manually triggered watch as the user has triggered this via an
        // external API call
        return true;
    }

    @Override
    public final boolean recordExecution() {
        return recordExecution;
    }

    public static Builder builder(Watch watch, boolean knownWatch, ManualTriggerEvent event, TimeValue defaultThrottlePeriod) {
        return new Builder(watch, knownWatch, event, defaultThrottlePeriod);
    }

    public static class Builder {

        static final String ALL = "_all";

        private final Watch watch;
        private final boolean knownWatch;
        private final ManualTriggerEvent triggerEvent;
        private final TimeValue defaultThrottlePeriod;
        protected ZonedDateTime executionTime;
        private boolean recordExecution = false;
        private Map<String, ActionExecutionMode> actionModes = new HashMap<>();
        private Input.Result inputResult;
        private Condition.Result conditionResult;

        private Builder(Watch watch, boolean knownWatch, ManualTriggerEvent triggerEvent, TimeValue defaultThrottlePeriod) {
            this.watch = watch;
            this.knownWatch = knownWatch;
            assert triggerEvent != null;
            this.triggerEvent = triggerEvent;
            this.defaultThrottlePeriod = defaultThrottlePeriod;
        }

        public Builder executionTime(ZonedDateTime executionTime) {
            this.executionTime = executionTime;
            return this;
        }

        public Builder recordExecution(boolean recordExecution) {
            this.recordExecution = recordExecution;
            return this;
        }

        public Builder allActionsMode(ActionExecutionMode mode) {
            return actionMode(ALL, mode);
        }

        public Builder actionMode(String id, ActionExecutionMode mode) {
            if (actionModes == null) {
                throw new IllegalStateException("ManualExecutionContext has already been built!");
            }
            if (ALL.equals(id)) {
                actionModes = new HashMap<>();
            }
            actionModes.put(id, mode);
            return this;
        }

        public Builder withInput(Input.Result inputResult) {
            this.inputResult = inputResult;
            return this;
        }

        public Builder withCondition(Condition.Result conditionResult) {
            this.conditionResult = conditionResult;
            return this;
        }

        public ManualExecutionContext build() throws Exception {
            if (executionTime == null) {
                executionTime = ZonedDateTime.now(ZoneOffset.UTC);
            }
            ManualExecutionContext context = new ManualExecutionContext(
                watch,
                knownWatch,
                executionTime,
                triggerEvent,
                defaultThrottlePeriod,
                inputResult,
                conditionResult,
                unmodifiableMap(actionModes),
                recordExecution
            );
            actionModes = null;
            return context;
        }
    }
}
