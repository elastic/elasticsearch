/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;

import java.util.function.Consumer;

import static java.util.stream.StreamSupport.stream;

public class AsyncTriggerEventConsumer implements Consumer<Iterable<TriggerEvent>> {

    private final Logger logger;
    private final ExecutionService executionService;

    public AsyncTriggerEventConsumer(Settings settings, ExecutionService executionService) {
        this.logger = Loggers.getLogger(SyncTriggerEventConsumer.class, settings);
        this.executionService = executionService;
    }

    @Override
    public void accept(Iterable<TriggerEvent> events) {
        try {
            executionService.processEventsAsync(events);
        } catch (Exception e) {
            logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to process triggered events [{}]",
                            (Object) stream(events.spliterator(), false).toArray(size ->
                                    new TriggerEvent[size])),
                    e);
        }
    }
}
