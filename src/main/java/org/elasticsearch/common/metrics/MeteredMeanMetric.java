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

package org.elasticsearch.common.metrics;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MeteredMeanMetric extends MeanMetric {
    private static final TimeValue INTERVAL = TimeValue.timeValueSeconds(5);
    private static final double M1_ALPHA = 1 - Math.exp(-5 / 60.0);
    private static final double M5_ALPHA = 1 - Math.exp(-5 / 60.0 / 5);
    private static final double M15_ALPHA = 1 - Math.exp(-5 / 60.0 / 15);
    private static final double M1H_ALPHA = 1 - Math.exp(-5 / 60.0 / 60);
    private static final double M1D_ALPHA = 1 - Math.exp(-5 / 60.0 / 60 / 24);
    private static final double M1W_ALPHA = 1 - Math.exp(-5 / 60.0 / 60 / 24 / 7);

    private final EWMA m1Rate = new EWMA(M1_ALPHA, INTERVAL.seconds(), TimeUnit.SECONDS);
    private final EWMA m5Rate = new EWMA(M5_ALPHA, INTERVAL.seconds(), TimeUnit.SECONDS);
    private final EWMA m15Rate = new EWMA(M15_ALPHA, INTERVAL.seconds(), TimeUnit.SECONDS);
    private final EWMA m1hRate = new EWMA(M1H_ALPHA, INTERVAL.seconds(), TimeUnit.SECONDS);
    private final EWMA m1dRate = new EWMA(M1D_ALPHA, INTERVAL.seconds(), TimeUnit.SECONDS);
    private final EWMA m1wRate = new EWMA(M1W_ALPHA, INTERVAL.seconds(), TimeUnit.SECONDS);
    private final ScheduledFuture<?> future;

    public MeteredMeanMetric(@Nullable ThreadPool threadPool) {
        if (threadPool == null) {
            // Disabling snapshotting, making this metric not useful.
            future = null;
            return;
        }
        this.future = threadPool.scheduleAtFixedRate(INTERVAL, ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                tick();
            }
        });
    }
    
    private void tick() {
        m1Rate.tick();
        m5Rate.tick();
        m15Rate.tick();
        m1hRate.tick();
        m1dRate.tick();
        m1wRate.tick();
    }
    
    public double oneMinuteRate() {
        return m1Rate.rate(TimeUnit.SECONDS);
    }
    
    public double fiveMinuteRate() {
        return m5Rate.rate(TimeUnit.SECONDS);
    }
    
    public double fifteenMinuteRate() {
        return m15Rate.rate(TimeUnit.SECONDS);
    }
    
    public double oneHourRate() {
        return m1hRate.rate(TimeUnit.SECONDS);
    }
    
    public double oneDayRate() {
        return m1dRate.rate(TimeUnit.SECONDS);
    }
    
    public double oneWeekRate() {
        return m1wRate.rate(TimeUnit.SECONDS);
    }
    
    public TimeSnapshot timeSnapshot() {
        return new TimeSnapshot(
                (long)oneMinuteRate(),
                (long)fiveMinuteRate(),
                (long)fifteenMinuteRate(),
                (long)oneHourRate(),
                (long)oneDayRate(),
                (long)oneWeekRate());
    }
    
    @Override
    public void inc(long n) {
        super.inc(n);
        m1Rate.update(n);
        m5Rate.update(n);
        m15Rate.update(n);
        m1hRate.update(n);
        m1dRate.update(n);
        m1wRate.update(n);
    }
    
    @Override
    public void dec(long n) {
        super.dec(n);
        m1Rate.update(-n);
        m5Rate.update(-n);
        m15Rate.update(-n);
        m1hRate.update(-n);
        m1dRate.update(-n);
        m1wRate.update(-n);
    }

    public void stop() {
        if (future != null) {
            future.cancel(false);
        }
    }
    
    /**
     * Assuming the rates are in microseconds, this returns a Streamable-able, ToXContent-able response.
     */
    public static class TimeSnapshot implements Streamable, ToXContent {
        private long m1Rate, m5Rate, m15Rate, m1hRate, m1dRate, m1wRate;
        
        public TimeSnapshot() {}
        
        public TimeSnapshot(long m1Rate, long m5Rate, long m15Rate, long m1hRate, long m1dRate, long m1wRate) {
            this.m1Rate = m1Rate;
            this.m5Rate = m5Rate;
            this.m15Rate = m15Rate;
            this.m1hRate = m1hRate;
            this.m1dRate = m1dRate;
            this.m1wRate = m1wRate;
        }
        
        public static TimeSnapshot add(TimeSnapshot lhs, TimeSnapshot rhs) {
            if (lhs == null) {
                return new TimeSnapshot(rhs.m1Rate, rhs.m5Rate, rhs.m15Rate, rhs.m1hRate, rhs.m1dRate, rhs.m1wRate);
            }
            lhs.add(rhs);
            return lhs;
        }
        
        private void add(TimeSnapshot snapshot) {
            m1Rate += snapshot.m1Rate;
            m5Rate += snapshot.m5Rate;
            m15Rate += snapshot.m15Rate;
            m1hRate += snapshot.m1hRate;
            m1dRate += snapshot.m1dRate;
            m1wRate += snapshot.m1wRate;
        }

        public long get1MinuteRate() {
            return m1Rate;
        }

        public long get5MinuteRate() {
            return m5Rate;
        }

        public long get15MinuteRate() {
            return m15Rate;
        }

        public long get1HourRate() {
            return m1hRate;
        }

        public long get1DayRate() {
            return m1dRate;
        }

        public long get1WeekRate() {
            return m1wRate;
        }

        public static TimeSnapshot readOptional(StreamInput in) throws IOException {
            if (in.getVersion().before(Version.V_1_0_0_RC1)) {
                return null;
            }
            if (in.readBoolean()) {
                TimeSnapshot snapshot = new TimeSnapshot();
                snapshot.readFrom(in);
                return snapshot;
            }
            return null;
        }
        
        @Override
        public void readFrom(StreamInput in) throws IOException {
            m1Rate = in.readVLong();
            m5Rate = in.readVLong();
            m15Rate = in.readVLong();
            m1hRate = in.readVLong();
            m1dRate = in.readVLong();
            m1wRate = in.readVLong();
        }
        
        public static void writeOptional(StreamOutput out, TimeSnapshot snapshot) throws IOException {
            if (out.getVersion().before(Version.V_1_0_0_RC1)) {
                return;
            }
            if (snapshot == null) {
                out.writeBoolean(false);
                return;
            }
            out.writeBoolean(true);
            snapshot.writeTo(out);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(m1Rate);
            out.writeVLong(m5Rate);
            out.writeVLong(m15Rate);
            out.writeVLong(m1hRate);
            out.writeVLong(m1dRate);
            out.writeVLong(m1wRate);
        }
        
        public static void toXContentOptional(XContentBuilder builder, Params params,
                XContentBuilderString name, TimeSnapshot snapshot) throws IOException {
            if (snapshot == null) {
                return;
            }
            builder.field(name);
            snapshot.toXContent(builder, params);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.timeValueField(Fields.MILLISECONDS_ONE_MINUTE, Fields.ONE_MINUTE, new TimeValue(m1Rate, TimeUnit.NANOSECONDS));
            builder.timeValueField(Fields.MILLISECONDS_FIVE_MINUTE, Fields.FIVE_MINUTE, new TimeValue(m5Rate, TimeUnit.NANOSECONDS));
            builder.timeValueField(Fields.MILLISECONDS_FIFTEEN_MINUTE, Fields.FIFTEEN_MINUTE, new TimeValue(m15Rate, TimeUnit.NANOSECONDS));
            builder.timeValueField(Fields.MILLISECONDS_ONE_HOUR, Fields.ONE_HOUR, new TimeValue(m1hRate, TimeUnit.NANOSECONDS));
            builder.timeValueField(Fields.MILLISECONS_ONE_DAY, Fields.ONE_DAY, new TimeValue(m1dRate, TimeUnit.NANOSECONDS));
            builder.timeValueField(Fields.MILLISECONDS_ONE_WEEK, Fields.ONE_WEEK, new TimeValue(m1wRate, TimeUnit.NANOSECONDS));
            builder.endObject();
            return builder;
        }
        
        static final class Fields {
            static final XContentBuilderString MILLISECONDS_ONE_MINUTE = new XContentBuilderString("milliseconds_per_second_one_minute_average");
            static final XContentBuilderString ONE_MINUTE = new XContentBuilderString("per_second_one_minute_average");
            static final XContentBuilderString MILLISECONDS_FIVE_MINUTE = new XContentBuilderString("milliseconds_per_second_five_minute_average");
            static final XContentBuilderString FIVE_MINUTE = new XContentBuilderString("per_second_five_minute_average");
            static final XContentBuilderString MILLISECONDS_FIFTEEN_MINUTE = new XContentBuilderString("milliseconds_per_second_fifteen_minute_average");
            static final XContentBuilderString FIFTEEN_MINUTE = new XContentBuilderString("per_second_fifteen_minute_average");
            static final XContentBuilderString MILLISECONDS_ONE_HOUR = new XContentBuilderString("milliseconds_per_second_one_hour_average");
            static final XContentBuilderString ONE_HOUR = new XContentBuilderString("per_second_one_hour_average");
            static final XContentBuilderString MILLISECONS_ONE_DAY = new XContentBuilderString("milliseconds_per_second_one_day_average");
            static final XContentBuilderString ONE_DAY = new XContentBuilderString("per_second_one_day_average");
            static final XContentBuilderString MILLISECONDS_ONE_WEEK = new XContentBuilderString("milliseoncds_per_second_one_week_average");
            static final XContentBuilderString ONE_WEEK = new XContentBuilderString("per_second_one_week_average");
        }
    }
}
