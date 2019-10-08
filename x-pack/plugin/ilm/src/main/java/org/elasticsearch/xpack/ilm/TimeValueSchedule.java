/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine.Schedule;

import java.util.Objects;

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
