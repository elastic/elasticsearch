/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.trigger.AbstractTriggerEngine;

import java.io.IOException;

/**
 *
 */
public abstract class ScheduleTriggerEngine extends AbstractTriggerEngine<ScheduleTrigger, ScheduleTriggerEvent> {

    public static final String TYPE = ScheduleTrigger.TYPE;

    protected final ScheduleRegistry scheduleRegistry;

    public ScheduleTriggerEngine(Settings settings, ScheduleRegistry scheduleRegistry) {
        super(settings);
        this.scheduleRegistry = scheduleRegistry;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public ScheduleTrigger parseTrigger(String context, XContentParser parser) throws IOException {
        Schedule schedule = scheduleRegistry.parse(context, parser);
        return new ScheduleTrigger(schedule);
    }

    @Override
    public ScheduleTriggerEvent parseTriggerEvent(String context, XContentParser parser) throws IOException {
        return ScheduleTriggerEvent.parse(context, parser);
    }
}
