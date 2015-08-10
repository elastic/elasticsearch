package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class InternalProfileBreakdown implements ProfileBreakdown, Streamable, ToXContent {

    public enum TimingType {
        REWRITE(0), WEIGHT(1), SCORE(2), COST(3), NORMALIZE(4), BUILD_SCORER(5);

        private int type;

        TimingType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    private long[] timings;
    private long[] scratch;

    public InternalProfileBreakdown() {
        timings = new long[6];
        scratch = new long[6];
    }

    public void startTime(TimingType timing) {
        scratch[timing.getType()] = System.nanoTime();
    }

    public long stopAndRecordTime(TimingType timing) {
        long time = System.nanoTime();

        time = time - scratch[timing.getType()];

        timings[timing.getType()] += time;
        scratch[timing.getType()] = 0L;
        return time;
    }

    public void setTime(TimingType type, long time) {
        timings[type.getType()] = time;
    }

    public long getTime(TimingType type) {
        return timings[type.getType()];
    }

    public long getTotalTime() {
        long time = 0;
        for (TimingType type : TimingType.values()) {
            time += timings[type.getType()];
        }
        return time;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timings = in.readLongArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLongArray(timings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (TimingType type : TimingType.values()) {
            builder = builder.field(type.toString(), timings[type.getType()]);
        }
        return builder;
    }
}
