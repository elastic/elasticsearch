/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

public interface TriggerEngine<T extends Trigger, E extends TriggerEvent> {

    String type();

    /**
     * It's the responsibility of the trigger engine implementation to select the appropriate jobs
     * from the given list of jobs
     */
    void start(Collection<Watch> jobs);

    void stop();

    void register(Consumer<Iterable<TriggerEvent>> consumer);

    void add(Watch job);

    /**
     * Get into a pause state, implies clearing out existing jobs
     */
    void pauseExecution();

    /**
     * Removes the job associated with the given name from this trigger engine.
     *
     * @param jobId   The name of the job to remove
     * @return          {@code true} if the job existed and removed, {@code false} otherwise.
     */
    boolean remove(String jobId);

    E simulateEvent(String jobId, @Nullable Map<String, Object> data, TriggerService service);

    T parseTrigger(String context, XContentParser parser) throws IOException;

    E parseTriggerEvent(TriggerService service, String watchId, String context, XContentParser parser) throws IOException;

}
