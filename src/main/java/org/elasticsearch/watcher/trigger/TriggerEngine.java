/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;

/**
 *
 */
public interface TriggerEngine<T extends Trigger, E extends TriggerEvent> {

    String type();

    /**
     * It's the responsibility of the trigger engine implementation to select the appropriate jobs
     * from the given list of jobs
     */
    void start(Collection<Job> jobs);

    void stop();

    void register(Listener listener);

    void add(Job job);

    /**
     * Removes the job associated with the given name from this trigger engine.
     *
     * @param jobId   The name of the job to remove
     * @return          {@code true} if the job existed and removed, {@code false} otherwise.
     */
    boolean remove(String jobId);

    T parseTrigger(String context, XContentParser parser) throws IOException;

    E parseTriggerEvent(String context, XContentParser parser) throws IOException;

    interface Listener {

        void triggered(Iterable<TriggerEvent> events);

    }

    interface Job {

        String id();

        Trigger trigger();
    }


}
