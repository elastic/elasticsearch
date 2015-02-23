/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler;

import org.elasticsearch.alerts.scheduler.schedule.Schedule;
import org.elasticsearch.common.joda.time.DateTime;

import java.util.Collection;

/**
 *
 */
public interface Scheduler {

    /**
     * Starts the scheduler and schedules the specified jobs before returning.
     */
    void start(Collection<? extends Job> jobs);

    /**
     * Stops the scheduler.
     */
    void stop();

    /**
     * Adds and schedules the give job
     */
    void add(Job job);

    /**
     * Removes the scheduled job that is associated with the given name
     */
    boolean remove(String jobName);

    void addListener(Listener listener);

    public static interface Listener {

        void fire(String jobName, DateTime scheduledFireTime, DateTime fireTime);

    }

    public static interface Job {

        String name();

        Schedule schedule();

    }
}
