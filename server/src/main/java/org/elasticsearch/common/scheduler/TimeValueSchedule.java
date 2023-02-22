/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.scheduler;

import org.elasticsearch.common.scheduler.SchedulerEngine.Schedule;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

/**
 * {@link Schedule} implementation wrapping a {@link TimeValue} interval that'll compute the
 * next scheduled execution time according to the configured interval.
 */
public class TimeValueSchedule implements Schedule {

    private TimeValue interval;

    public TimeValueSchedule(TimeValue interval) {
        if (interval.millis() <= 0) {
            throw new IllegalArgumentException("interval must be greater than 0 milliseconds");
        }
        this.interval = interval;
    }

    public TimeValue getInterval() {
        return interval;
    }

    @Override
    public long nextScheduledTimeAfter(long startTime, long time) {
        assert time >= startTime;
        if (startTime == time) {
            time++;
        }
        long delta = time - startTime;
        return startTime + (delta / interval.millis() + 1) * interval.millis();
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TimeValueSchedule other = (TimeValueSchedule) obj;
        return Objects.equals(interval, other.interval);
    }

}
