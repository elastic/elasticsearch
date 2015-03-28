/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.trigger.AbstractTriggerEngine;

/**
 *
 */
public abstract class ScheduleTriggerEngine extends AbstractTriggerEngine<ScheduleTrigger, ScheduleTriggerEvent> {

    public static final String TYPE = ScheduleTrigger.TYPE;

    public ScheduleTriggerEngine(Settings settings) {
        super(settings);
    }

    @Override
    public String type() {
        return TYPE;
    }
}
