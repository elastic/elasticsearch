/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.base.Predicates;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.watcher.watch.Watch;

import java.util.HashMap;
import java.util.Set;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;

/**
 */
public class ManualExecutionContext extends WatchExecutionContext {

    private final Predicate<String> simulateActionPredicate;
    private final boolean recordExecution;

    ManualExecutionContext(Watch watch, DateTime executionTime, ManualTriggerEvent triggerEvent,
                           Input.Result inputResult, Condition.Result conditionResult,
                           Throttler.Result throttlerResult, Predicate<String> simulateActionPredicate,
                           boolean recordExecution) {
        super(watch, executionTime, triggerEvent);
        if (inputResult != null) {
            onInputResult(inputResult);
        }
        if (conditionResult != null) {
            onConditionResult(conditionResult);
        }
        if (throttlerResult != null) {
            onThrottleResult(throttlerResult);
        }
        this.simulateActionPredicate = simulateActionPredicate;
        this.recordExecution = recordExecution;
    }

    @Override
    public final boolean simulateAction(String actionId) {
        return simulateActionPredicate.apply(actionId);
    }

    @Override
    public final boolean recordExecution() {
        return recordExecution;
    }

    public static Builder builder(Watch watch) {
        return new Builder(watch);
    }


    public static class Builder {

        private final Watch watch;
        protected DateTime executionTime;
        private boolean recordExecution = false;
        private Predicate<String> simulateActionPredicate = Predicates.alwaysFalse();
        private Input.Result inputResult;
        private Condition.Result conditionResult;
        private Throttler.Result throttlerResult;
        private ManualTriggerEvent triggerEvent;

        private Builder(Watch watch) {
            this.watch = watch;
        }

        public Builder executionTime(DateTime executionTime) {
            this.executionTime = executionTime;
            return this;
        }

        public Builder recordExecution(boolean recordExecution) {
            this.recordExecution = recordExecution;
            return this;
        }

        public Builder simulateAllActions() {
            simulateActionPredicate = Predicates.alwaysTrue();
            return this;
        }

        public Builder simulateActions(String... ids) {
            simulateActionPredicate = Predicates.or(simulateActionPredicate, new IdsPredicate(ids));
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

        public Builder withThrottle(Throttler.Result throttlerResult) {
            this.throttlerResult = throttlerResult;
            return this;
        }

        public Builder triggerEvent(ManualTriggerEvent triggerEvent) {
            this.triggerEvent = triggerEvent;
            return this;
        }


        public ManualExecutionContext build() {
            if (executionTime == null) {
                executionTime = DateTime.now(UTC);
            }
            if (triggerEvent == null) {
                triggerEvent = new ManualTriggerEvent(watch.id(), executionTime, new HashMap<String, Object>());
            }
            return new ManualExecutionContext(watch, executionTime, triggerEvent, inputResult, conditionResult, throttlerResult, simulateActionPredicate, recordExecution);
        }
    }

    static class IdsPredicate implements Predicate<String> {

        private final Set<String> ids;

        private Set<String> ids() {
            return ids;
        }

        IdsPredicate(String... ids) {
            this.ids = ImmutableSet.copyOf(ids);
        }

        @Override
        public boolean apply(String id) {
            return ids.contains(id);
        }
    }
}
