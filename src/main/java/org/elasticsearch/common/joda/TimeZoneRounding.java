/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;

/**
 */
public abstract class TimeZoneRounding {

    public abstract long calc(long utcMillis);

    public static Builder builder(DateTimeField field) {
        return new Builder(field);
    }

    public static Builder builder(TimeValue interval) {
        return new Builder(interval);
    }

    public static class Builder {

        private DateTimeField field;
        private long interval = -1;

        private DateTimeZone preTz = DateTimeZone.UTC;
        private DateTimeZone postTz = DateTimeZone.UTC;

        private float factor = 1.0f;

        private long preOffset;
        private long postOffset;

        private boolean preZoneAdjustLargeInterval = false;

        public Builder(DateTimeField field) {
            this.field = field;
            this.interval = -1;
        }

        public Builder(TimeValue interval) {
            this.field = null;
            this.interval = interval.millis();
        }

        public Builder preZone(DateTimeZone preTz) {
            this.preTz = preTz;
            return this;
        }

        public Builder preZoneAdjustLargeInterval(boolean preZoneAdjustLargeInterval) {
            this.preZoneAdjustLargeInterval = preZoneAdjustLargeInterval;
            return this;
        }

        public Builder postZone(DateTimeZone postTz) {
            this.postTz = postTz;
            return this;
        }

        public Builder preOffset(long preOffset) {
            this.preOffset = preOffset;
            return this;
        }

        public Builder postOffset(long postOffset) {
            this.postOffset = postOffset;
            return this;
        }

        public Builder factor(float factor) {
            this.factor = factor;
            return this;
        }

        public TimeZoneRounding build() {
            TimeZoneRounding timeZoneRounding;
            if (field != null) {
                if (preTz.equals(DateTimeZone.UTC) && postTz.equals(DateTimeZone.UTC)) {
                    timeZoneRounding = new UTCTimeZoneRoundingFloor(field);
                } else if (preZoneAdjustLargeInterval || field.getDurationField().getUnitMillis() < DateTimeConstants.MILLIS_PER_HOUR * 12) {
                    timeZoneRounding = new TimeTimeZoneRoundingFloor(field, preTz, postTz);
                } else {
                    timeZoneRounding = new DayTimeZoneRoundingFloor(field, preTz, postTz);
                }
            } else {
                if (preTz.equals(DateTimeZone.UTC) && postTz.equals(DateTimeZone.UTC)) {
                    timeZoneRounding = new UTCIntervalTimeZoneRounding(interval);
                } else if (preZoneAdjustLargeInterval || interval < DateTimeConstants.MILLIS_PER_HOUR * 12) {
                    timeZoneRounding = new TimeIntervalTimeZoneRounding(interval, preTz, postTz);
                } else {
                    timeZoneRounding = new DayIntervalTimeZoneRounding(interval, preTz, postTz);
                }
            }
            if (preOffset != 0 || postOffset != 0) {
                timeZoneRounding = new PrePostTimeZoneRounding(timeZoneRounding, preOffset, postOffset);
            }
            if (factor != 1.0f) {
                timeZoneRounding = new FactorTimeZoneRounding(timeZoneRounding, factor);
            }
            return timeZoneRounding;
        }
    }

    static class TimeTimeZoneRoundingFloor extends TimeZoneRounding {

        private final DateTimeField field;
        private final DateTimeZone preTz;
        private final DateTimeZone postTz;

        TimeTimeZoneRoundingFloor(DateTimeField field, DateTimeZone preTz, DateTimeZone postTz) {
            this.field = field;
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public long calc(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            time = field.roundFloor(time);
            // now, time is still in local, move it to UTC (or the adjustLargeInterval flag is set)
            time = time - preTz.getOffset(time);
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
        }
    }

    static class UTCTimeZoneRoundingFloor extends TimeZoneRounding {

        private final DateTimeField field;

        UTCTimeZoneRoundingFloor(DateTimeField field) {
            this.field = field;
        }

        @Override
        public long calc(long utcMillis) {
            return field.roundFloor(utcMillis);
        }
    }

    static class DayTimeZoneRoundingFloor extends TimeZoneRounding {
        private final DateTimeField field;
        private final DateTimeZone preTz;
        private final DateTimeZone postTz;

        DayTimeZoneRoundingFloor(DateTimeField field, DateTimeZone preTz, DateTimeZone postTz) {
            this.field = field;
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public long calc(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            time = field.roundFloor(time);
            // after rounding, since its day level (and above), its actually UTC!
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
        }
    }

    static class UTCIntervalTimeZoneRounding extends TimeZoneRounding {

        private final long interval;

        UTCIntervalTimeZoneRounding(long interval) {
            this.interval = interval;
        }

        @Override
        public long calc(long utcMillis) {
            return ((utcMillis / interval) * interval);
        }
    }


    static class TimeIntervalTimeZoneRounding extends TimeZoneRounding {

        private final long interval;
        private final DateTimeZone preTz;
        private final DateTimeZone postTz;

        TimeIntervalTimeZoneRounding(long interval, DateTimeZone preTz, DateTimeZone postTz) {
            this.interval = interval;
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public long calc(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            time = ((time / interval) * interval);
            // now, time is still in local, move it to UTC
            time = time - preTz.getOffset(time);
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
        }
    }

    static class DayIntervalTimeZoneRounding extends TimeZoneRounding {

        private final long interval;
        private final DateTimeZone preTz;
        private final DateTimeZone postTz;

        DayIntervalTimeZoneRounding(long interval, DateTimeZone preTz, DateTimeZone postTz) {
            this.interval = interval;
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public long calc(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            time = ((time / interval) * interval);
            // after rounding, since its day level (and above), its actually UTC!
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
        }
    }

    static class FactorTimeZoneRounding extends TimeZoneRounding {

        private final TimeZoneRounding timeZoneRounding;

        private final float factor;

        FactorTimeZoneRounding(TimeZoneRounding timeZoneRounding, float factor) {
            this.timeZoneRounding = timeZoneRounding;
            this.factor = factor;
        }

        @Override
        public long calc(long utcMillis) {
            return timeZoneRounding.calc((long) (factor * utcMillis));
        }
    }

    static class PrePostTimeZoneRounding extends TimeZoneRounding {

        private final TimeZoneRounding timeZoneRounding;

        private final long preOffset;
        private final long postOffset;

        PrePostTimeZoneRounding(TimeZoneRounding timeZoneRounding, long preOffset, long postOffset) {
            this.timeZoneRounding = timeZoneRounding;
            this.preOffset = preOffset;
            this.postOffset = postOffset;
        }

        @Override
        public long calc(long utcMillis) {
            return postOffset + timeZoneRounding.calc(utcMillis + preOffset);
        }
    }
}
