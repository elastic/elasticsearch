/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;

public class TriggerWatchStats {

    public final boolean metadata;
    public final String triggerType;
    public final String scheduleType;
    public final String inputType;
    public final String conditionType;
    public final String transformType;
    public final ActionStats[] actions;

    private TriggerWatchStats(boolean metadata, String triggerType, String scheduleType, String inputType,
                              String conditionType, String transformType, ActionStats[] actions) {
        this.metadata = metadata;
        this.triggerType = triggerType;
        this.scheduleType = scheduleType;
        this.inputType = inputType;
        this.conditionType = conditionType;
        this.transformType = transformType;
        this.actions = actions;
    }

    public static final class ActionStats {
        public final String actionType;
        public final String transformType;
        public final String conditionType;

        public ActionStats(String actionType, String transformType, String conditionType) {
            this.actionType = actionType;
            this.transformType = transformType;
            this.conditionType = conditionType;
        }
    }

    public static TriggerWatchStats create(Watch watch) {
        final boolean metadata = watch.metadata() != null && watch.metadata().isEmpty() == false;
        final String triggerType = watch.trigger().type();
        String scheduleTriggerType = null;
        if (ScheduleTrigger.TYPE.equals(watch.trigger().type())) {
            ScheduleTrigger scheduleTrigger = (ScheduleTrigger) watch.trigger();
            scheduleTriggerType = scheduleTrigger.getSchedule().type();
        }
        final String inputType = watch.input().type();
        final String conditionType = watch.condition().type();
        final String transformType = watch.transform() != null ? watch.transform().type() : null;

        final ActionStats[] actionStats = new ActionStats[watch.actions().size()];
        int i = 0;
        for (ActionWrapper actionWrapper : watch.actions()) {
            String transform = actionWrapper.transform() != null ? actionWrapper.transform().type() : null;
            String condition = actionWrapper.condition() != null ? actionWrapper.condition().type() : null;
            String type = actionWrapper.action().type();
            actionStats[i++] = new ActionStats(type, transform, condition);
        }

        return new TriggerWatchStats(metadata, triggerType, scheduleTriggerType, inputType,
                                     conditionType, transformType, actionStats);
    }
}
