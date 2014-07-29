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

        private DateTimeZone preTz = DateTimeZone.UTC;
        private DateTimeZone postTz = DateTimeZone.UTC;

        private float factor = 1.0f;

        private long preOffset;
        private long postOffset;

        private boolean preZoneAdjustLargeInterval = false;

        public Builder(DateTimeUnit unit) {
            this.unit = unit;
            this.interval = -1;
        }

        public Builder(TimeValue interval) {
            this.unit = null;
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
            if (unit != null) {
                if (preTz.equals(DateTimeZone.UTC) && postTz.equals(DateTimeZone.UTC)) {
                    timeZoneRounding = new UTCTimeZoneRoundingFloor(unit);
                } else if (preZoneAdjustLargeInterval || unit.field().getDurationField().getUnitMillis() < DateTimeConstants.MILLIS_PER_HOUR * 12) {
                    timeZoneRounding = new TimeTimeZoneRoundingFloor(unit, preTz, postTz);
                } else {
                    timeZoneRounding = new DayTimeZoneRoundingFloor(unit, preTz, postTz);
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

        static final byte ID = 1;

        private DateTimeUnit unit;
        private DateTimeField field;
        private DurationField durationField;
        private DateTimeZone preTz;
        private DateTimeZone postTz;

        TimeTimeZoneRoundingFloor() { // for serialization
        }

        TimeTimeZoneRoundingFloor(DateTimeUnit unit, DateTimeZone preTz, DateTimeZone postTz) {
            this.unit = unit;
            field = unit.field();
            durationField = field.getDurationField();
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            return field.roundFloor(time);
        }

        @Override
        public long valueForKey(long time) {
            // now, time is still in local, move it to UTC (or the adjustLargeInterval flag is set)
            time = time - preTz.getOffset(time);
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
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
            preTz = DateTimeZone.forID(in.readSharedString());
            postTz = DateTimeZone.forID(in.readSharedString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(unit.id());
            out.writeSharedString(preTz.getID());
            out.writeSharedString(postTz.getID());
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
        private DateTimeZone preTz;
        private DateTimeZone postTz;

        DayTimeZoneRoundingFloor() { // for serialization
        }

        DayTimeZoneRoundingFloor(DateTimeUnit unit, DateTimeZone preTz, DateTimeZone postTz) {
            this.unit = unit;
            field = unit.field();
            durationField = field.getDurationField();
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            return field.roundFloor(time);
        }

        @Override
        public long valueForKey(long time) {
            // after rounding, since its day level (and above), its actually UTC!
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
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
            preTz = DateTimeZone.forID(in.readSharedString());
            postTz = DateTimeZone.forID(in.readSharedString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(unit.id());
            out.writeSharedString(preTz.getID());
            out.writeSharedString(postTz.getID());
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
        private DateTimeZone preTz;
        private DateTimeZone postTz;

        TimeIntervalTimeZoneRounding() { // for serialization
        }

        TimeIntervalTimeZoneRounding(long interval, DateTimeZone preTz, DateTimeZone postTz) {
            this.interval = interval;
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            return Rounding.Interval.roundKey(time, interval);
        }

        @Override
        public long valueForKey(long key) {
            long time = Rounding.Interval.roundValue(key, interval);
            // now, time is still in local, move it to UTC
            time = time - preTz.getOffset(time);
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
        }

        @Override
        public long nextRoundingValue(long value) {
            return value + interval;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            interval = in.readVLong();
            preTz = DateTimeZone.forID(in.readSharedString());
            postTz = DateTimeZone.forID(in.readSharedString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
            out.writeSharedString(preTz.getID());
            out.writeSharedString(postTz.getID());
        }
    }

    static class DayIntervalTimeZoneRounding extends TimeZoneRounding {

        final static byte ID = 6;

        private long interval;
        private DateTimeZone preTz;
        private DateTimeZone postTz;

        DayIntervalTimeZoneRounding() { // for serialization
        }

        DayIntervalTimeZoneRounding(long interval, DateTimeZone preTz, DateTimeZone postTz) {
            this.interval = interval;
            this.preTz = preTz;
            this.postTz = postTz;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            long time = utcMillis + preTz.getOffset(utcMillis);
            return Rounding.Interval.roundKey(time, interval);
        }

        @Override
        public long valueForKey(long key) {
            long time = Rounding.Interval.roundValue(key, interval);
            // after rounding, since its day level (and above), its actually UTC!
            // now apply post Tz
            time = time + postTz.getOffset(time);
            return time;
        }

        @Override
        public long nextRoundingValue(long value) {
            return value + interval;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            interval = in.readVLong();
            preTz = DateTimeZone.forID(in.readSharedString());
            postTz = DateTimeZone.forID(in.readSharedString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
            out.writeSharedString(preTz.getID());
            out.writeSharedString(postTz.getID());
        }
    }

    static class FactorTimeZoneRounding extends TimeZoneRounding {

        final static byte ID = 7;

        private TimeZoneRounding timeZoneRounding;

        private float factor;

        FactorTimeZoneRounding() { // for serialization
        }

        FactorTimeZoneRounding(TimeZoneRounding timeZoneRounding, float factor) {
            this.timeZoneRounding = timeZoneRounding;
            this.factor = factor;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            return timeZoneRounding.roundKey((long) (factor * utcMillis));
        }

        @Override
        public long valueForKey(long key) {
            return timeZoneRounding.valueForKey(key);
        }

        @Override
        public long nextRoundingValue(long value) {
            return timeZoneRounding.nextRoundingValue(value);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            timeZoneRounding = (TimeZoneRounding) Rounding.Streams.read(in);
            factor = in.readFloat();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Rounding.Streams.write(timeZoneRounding, out);
            out.writeFloat(factor);
        }
    }

    static class PrePostTimeZoneRounding extends TimeZoneRounding {

        final static byte ID = 8;

        private TimeZoneRounding timeZoneRounding;

        private long preOffset;
        private long postOffset;

        PrePostTimeZoneRounding() { // for serialization
        }

        PrePostTimeZoneRounding(TimeZoneRounding timeZoneRounding, long preOffset, long postOffset) {
            this.timeZoneRounding = timeZoneRounding;
            this.preOffset = preOffset;
            this.postOffset = postOffset;
        }

        @Override
        public byte id() {
            return ID;
        }

        @Override
        public long roundKey(long utcMillis) {
            return timeZoneRounding.roundKey(utcMillis + preOffset);
        }

        @Override
        public long valueForKey(long key) {
            return postOffset + timeZoneRounding.valueForKey(key);
        }

        @Override
        public long nextRoundingValue(long value) {
            return postOffset + timeZoneRounding.nextRoundingValue(value - postOffset);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            timeZoneRounding = (TimeZoneRounding) Rounding.Streams.read(in);
            preOffset = in.readVLong();
            postOffset = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Rounding.Streams.write(timeZoneRounding, out);
            out.writeVLong(preOffset);
            out.writeVLong(postOffset);
        }
    }
}
