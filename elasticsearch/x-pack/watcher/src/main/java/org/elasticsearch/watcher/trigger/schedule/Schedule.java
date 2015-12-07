/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public interface Schedule extends ToXContent {

    String type();

    /**
     * Returns the next scheduled time after the given time, according to this schedule. If the given schedule
     * cannot resolve the next scheduled time, then {@code -1} is returned. It really depends on the type of
     * schedule to determine when {@code -1} is returned. Some schedules (e.g. IntervalSchedule) will never return
     * {@code -1} as they can always compute the next scheduled time. {@code Cron} based schedules are good example
     * of schedules that may return {@code -1}, for example, when the schedule only points to times that are all
     * before the given time (in which case, there is no next scheduled time for the given time).
     *
     * Example:
     *
     *      cron    0 0 0 * 1 ? 2013        (only points to days in January 2013)
     *
     *      time    2015-01-01 12:00:00     (this time is in 2015)
     *
     */
    long nextScheduledTimeAfter(long startTime, long time);

    interface Parser<S extends Schedule> {

        String type();

        S parse(XContentParser parser) throws IOException;
    }
}
