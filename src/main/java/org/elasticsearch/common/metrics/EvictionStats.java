package org.elasticsearch.common.metrics;


import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class EvictionStats implements Streamable, ToXContent {

    private long evictions;
    private double evictionsOneMinuteRate;
    private double evictionsFiveMinuteRate;
    private double evictionsFifteenMinuteRate;

    public EvictionStats() {
        this.evictions = 0;
        this.evictionsOneMinuteRate = 0;
        this.evictionsFiveMinuteRate = 0;
        this.evictionsFifteenMinuteRate = 0;
    }

    public EvictionStats(long evictions, double oneMin, double fiveMin, double fifteenMin) {
        this.evictions = evictions;
        this.evictionsOneMinuteRate = oneMin;
        this.evictionsFiveMinuteRate = fiveMin;
        this.evictionsFifteenMinuteRate = fifteenMin;
    }

    public EvictionStats(MeterMetric evictionMeter) {
        this.evictions = evictionMeter.count();
        this.evictionsOneMinuteRate = evictionMeter.oneMinuteRate();
        this.evictionsFiveMinuteRate = evictionMeter.fiveMinuteRate();
        this.evictionsFifteenMinuteRate = evictionMeter.fifteenMinuteRate();
    }

    public void add(EvictionStats other) {
        this.evictions += other.getEvictions();
        this.evictionsOneMinuteRate += other.getEvictionsOneMinuteRate();
        this.evictionsFiveMinuteRate += other.getEvictionsFiveMinuteRate();
        this.evictionsFifteenMinuteRate += other.getEvictionsFifteenMinuteRate();
    }

    public long getEvictions() {
        return evictions;
    }

    public double getEvictionsOneMinuteRate() {
        return evictionsOneMinuteRate;
    }

    public double getEvictionsFiveMinuteRate() {
        return evictionsFiveMinuteRate;
    }

    public double getEvictionsFifteenMinuteRate() {
        return evictionsFifteenMinuteRate;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        evictions = in.readVLong();
        evictionsOneMinuteRate = in.readDouble();
        evictionsFiveMinuteRate = in.readDouble();
        evictionsFifteenMinuteRate = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(evictions);
        out.writeDouble(evictionsOneMinuteRate);
        out.writeDouble(evictionsFiveMinuteRate);
        out.writeDouble(evictionsFifteenMinuteRate);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.EVICTIONS, getEvictions());

        builder.startObject(Fields.RATES);
            builder.field(Fields.ONE_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsOneMinuteRate(), "")));
            builder.field(Fields.FIVE_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsFiveMinuteRate(),"")));
            builder.field(Fields.FIFTEEN_MIN, Double.valueOf(Strings.format1Decimals(getEvictionsFifteenMinuteRate(),"")));
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final XContentBuilderString EVICTIONS = new XContentBuilderString("evictions");
        static final XContentBuilderString RATES = new XContentBuilderString("evictions_per_sec");
        static final XContentBuilderString ONE_MIN = new XContentBuilderString("1m");
        static final XContentBuilderString FIVE_MIN = new XContentBuilderString("5m");
        static final XContentBuilderString FIFTEEN_MIN = new XContentBuilderString("15m");
    }
}
