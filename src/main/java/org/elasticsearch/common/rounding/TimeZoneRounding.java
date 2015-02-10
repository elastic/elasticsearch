/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.rounding;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;

import java.io.IOException;

/**
 */
public abstract class TimeZoneRounding extends Rounding {

    public static Builder builder(DateTimeUnit unit) {
        return new Builder(unit);
    }

    public static Builder builder(TimeValue interval) {
        return new Builder(interval);
    }

    public static class Builder {

        private DateTimeUnit unit;
        private long interval = -1;

        private DateTimeZone timeZone = DateTimeZone.UTC;

        private float factor = 1.0f;

        private long offset;

        public Builder(DateTimeUnit unit) {
            this.unit = unit;
            this.interval = -1;
        }

        public Builder(TimeValue interval) {
            this.unit = null;
            this.interval = interval.millis();
        }

        public Builder timeZone(DateTimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder factor(float factor) {
            this.factor = factor;
            return this;
        }

        public Rounding build() {
            Rounding timeZoneRounding;
            if (unit != null) {
                if (timeZone.equals(DateTimeZone.UTC)) {
                    timeZoneRounding = new UTCTimeZoneRoundingFloor(unit);
                } else if (unit.field().getDurationField().getUnitMillis() < DateTimeConstants.MILLIS_PER_HOUR * 12) {
                    timeZoneRounding = new TimeTimeZoneRoundingFloor(unit, timeZone);
                } else {
                    timeZoneRounding = new DayTimeZoneRoundingFloor(unit, timeZone);
                }
            } else {
                if (timeZone.equals(DateTimeZone.UTC)) {
                    timeZoneRounding = new UTCIntervalTimeZoneRounding(interval);
                } else if (interval < DateTimeConstants.MILLIS_PER_HOUR * 12) {
                    timeZoneRounding = new TimeIntervalTimeZoneRounding(interval, timeZone);
                } else {
                    timeZoneRounding = new DayIntervalTimeZoneRounding(interval, timeZone);
                }
            }
            if (offset != 0) {
                timeZoneRounding = new OffsetRounding(timeZoneRounding, offset);
            }
            if (factor != 1.0f) {
                timeZoneRounding = new FactorRounding(timeZoneRounding, factor);
            }
            return timeZoneRounding;
        }
    }

    static class TimeTimeZoneRoundingFloor extends TimeZoneRounding {

        static final byte ID = 1;

        private DateTimeUnit unit;
        private DateTimeField field;
        private DurationField durationField;
        private DateTimeZone timeZone;

        TimeTimeZoneRoundingFloor() { // for serialization
        }

        TimeTimeZoneRoundingFloor(DateTimeUnit unit, DateTimeZone timeZone) {
            this.unit = unit;
            field = unit.field();
            durationField = field.getDurationField();
            this.timeZone = timeZone;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long timeLocal = timeZone.convertUTCToLocal(utcMillis);
            return field.roundFloor(timeLocal);
        }

        @Override
        public long valueForKey(long time) {
            assert field.roundFloor(time) == time;
            return timeZone.convertLocalToUTC(time, true);
        }

        @Override
        public long nextRoundingValue(long time) {
            long timeUtc = timeZone.convertUTCToLocal(time);
            long nextInLocalTime = durationField.add(timeUtc, 1);
            return timeZone.convertLocalToUTC(nextInLocalTime, true);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            unit = DateTimeUnit.resolve(in.readByte());
            field = unit.field();
            durationField = field.getDurationField();
            timeZone = DateTimeZone.forID(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(unit.id());
            out.writeString(timeZone.getID());
        }
    }

    static class UTCTimeZoneRoundingFloor extends TimeZoneRounding {

        final static byte ID = 2;

        private DateTimeUnit unit;
        private DateTimeField field;
        private DurationField durationField;

        UTCTimeZoneRoundingFloor() { // for serialization
        }

        UTCTimeZoneRoundingFloor(DateTimeUnit unit) {
            this.unit = unit;
            field = unit.field();
            durationField = field.getDurationField();
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            return field.roundFloor(utcMillis);
        }

        @Override
        public long valueForKey(long key) {
            return key;
        }

        @Override
        public long nextRoundingValue(long value) {
            return durationField.add(value, 1);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            unit = DateTimeUnit.resolve(in.readByte());
            field = unit.field();
            durationField = field.getDurationField();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(unit.id());
        }
    }

    static class DayTimeZoneRoundingFloor extends TimeZoneRounding {

        final static byte ID = 3;

        private DateTimeUnit unit;
        private DateTimeField field;
        private DurationField durationField;
        private DateTimeZone timeZone;

        DayTimeZoneRoundingFloor() { // for serialization
        }

        DayTimeZoneRoundingFloor(DateTimeUnit unit, DateTimeZone timeZone) {
            this.unit = unit;
            field = unit.field();
            durationField = field.getDurationField();
            this.timeZone = timeZone;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long timeLocal = timeZone.convertUTCToLocal(utcMillis);
            return field.roundFloor(timeLocal);
        }

        @Override
        public long valueForKey(long time) {
            assert field.roundFloor(time) == time;
            return timeZone.convertLocalToUTC(time, true);
        }

        @Override
        public long nextRoundingValue(long time) {
            long timeUtc = timeZone.convertUTCToLocal(time);
            long nextInLocalTime = durationField.add(timeUtc, 1);
            return timeZone.convertLocalToUTC(nextInLocalTime, true);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            unit = DateTimeUnit.resolve(in.readByte());
            field = unit.field();
            durationField = field.getDurationField();
            timeZone = DateTimeZone.forID(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(unit.id());
            out.writeString(timeZone.getID());
        }
    }

    static class UTCIntervalTimeZoneRounding extends TimeZoneRounding {

        final static byte ID = 4;

        private long interval;

        UTCIntervalTimeZoneRounding() { // for serialization
        }

        UTCIntervalTimeZoneRounding(long interval) {
            this.interval = interval;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            return Rounding.Interval.roundKey(utcMillis, interval);
        }

        @Override
        public long valueForKey(long key) {
            return Rounding.Interval.roundValue(key, interval);
        }

        @Override
        public long nextRoundingValue(long value) {
            return value + interval;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            interval = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
        }
    }


    static class TimeIntervalTimeZoneRounding extends TimeZoneRounding {

        final static byte ID = 5;

        private long interval;
        private DateTimeZone timeZone;

        TimeIntervalTimeZoneRounding() { // for serialization
        }

        TimeIntervalTimeZoneRounding(long interval, DateTimeZone timeZone) {
            this.interval = interval;
            this.timeZone = timeZone;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long timeLocal = timeZone.convertUTCToLocal(utcMillis);
            return Rounding.Interval.roundKey(timeLocal, interval);
        }

        @Override
        public long valueForKey(long key) {
            long localTime = Rounding.Interval.roundValue(key, interval);
            return timeZone.convertLocalToUTC(localTime, true);
        }

        @Override
        public long nextRoundingValue(long value) {
            long timeLocal = timeZone.convertUTCToLocal(value);
            long next = timeLocal + interval;
            return timeZone.convertLocalToUTC(next, true);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            interval = in.readVLong();
            timeZone = DateTimeZone.forID(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
            out.writeString(timeZone.getID());
        }
    }

    static class DayIntervalTimeZoneRounding extends TimeZoneRounding {

        final static byte ID = 6;

        private long interval;
        private DateTimeZone timeZone;

        DayIntervalTimeZoneRounding() { // for serialization
        }

        DayIntervalTimeZoneRounding(long interval, DateTimeZone timeZone) {
            this.interval = interval;
            this.timeZone = timeZone;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long time = utcMillis + timeZone.getOffset(utcMillis);
            return Rounding.Interval.roundKey(time, interval);
        }

        @Override
        public long valueForKey(long key) {
            long localTime = Rounding.Interval.roundValue(key, interval);
            return timeZone.convertLocalToUTC(localTime, true);
        }

        @Override
        public long nextRoundingValue(long value) {
            long timeLocal = timeZone.convertUTCToLocal(value);
            long next = timeLocal + interval;
            return timeZone.convertLocalToUTC(next, true);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            interval = in.readVLong();
            timeZone = DateTimeZone.forID(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
            out.writeString(timeZone.getID());
        }
    }
}
