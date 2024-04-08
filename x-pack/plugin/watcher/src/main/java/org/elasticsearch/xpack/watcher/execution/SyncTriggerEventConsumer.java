/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;

import java.util.function.Consumer;

import static java.util.stream.StreamSupport.stream;
import static org.elasticsearch.core.Strings.format;

public class SyncTriggerEventConsumer implements Consumer<Iterable<TriggerEvent>> {
    private static final Logger logger = LogManager.getLogger(SyncTriggerEventConsumer.class);

    private final ExecutionService executionService;

    public SyncTriggerEventConsumer(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @Override
    public void accept(Iterable<TriggerEvent> events) {
        try {
            executionService.processEventsSync(events);
        } catch (Exception e) {
            logger.error(
                () -> format(
                    "failed to process triggered events [%s]",
                    (Object) stream(events.spliterator(), false).toArray(TriggerEvent[]::new)
                ),
                e
            );
        }
    }
}
