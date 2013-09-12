package org.elasticsearch.index.percolator.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class PercolateStats implements Streamable, ToXContent {

    private long percolateCount;
    private long percolateTimeInMillis;
    private long current;

    public PercolateStats() {
    }

    public PercolateStats(long percolateCount, long percolateTimeInMillis, long current) {
        this.percolateCount = percolateCount;
        this.percolateTimeInMillis = percolateTimeInMillis;
        this.current = current;
    }

    public long getCount() {
        return percolateCount;
    }

    public long getTimeInMillis() {
        return percolateTimeInMillis;
    }

    public TimeValue getTime() {
        return new TimeValue(getTimeInMillis());
    }

    public long getCurrent() {
        return current;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PERCOLATE);
        builder.field(Fields.TOTAL, percolateCount);
        builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, percolateTimeInMillis);
        builder.field(Fields.CURRENT, current);
        builder.endObject();
        return builder;
    }

    public void add(PercolateStats percolate) {
        if (percolate == null) {
            return;
        }

        percolateCount += percolate.getCount();
        percolateTimeInMillis += percolate.getTimeInMillis();
        current += percolate.getCurrent();
    }

    static final class Fields {
        static final XContentBuilderString PERCOLATE = new XContentBuilderString("percolate");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TIME = new XContentBuilderString("getTime");
        static final XContentBuilderString TIME_IN_MILLIS = new XContentBuilderString("time_in_millis");
        static final XContentBuilderString CURRENT = new XContentBuilderString("current");
    }

    public static PercolateStats readPercolateStats(StreamInput in) throws IOException {
        PercolateStats stats = new PercolateStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        percolateCount = in.readVLong();
        percolateTimeInMillis = in.readVLong();
        current = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(percolateCount);
        out.writeVLong(percolateTimeInMillis);
        out.writeVLong(current);
    }
}
