/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.TriggerService;

/**
 */
public class AsyncTriggerListener implements TriggerEngine.Listener {

    private final ExecutionService executionService;

    @Inject
    public AsyncTriggerListener(ExecutionService executionService, TriggerService triggerService) {
        this.executionService = executionService;
        triggerService.register(this);
    }

    @Override
    public void triggered(Iterable<TriggerEvent> events) {
        executionService.processEventsAsync(events);
    }

}
