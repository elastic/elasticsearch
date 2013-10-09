package org.elasticsearch.index.percolator.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Exposes percolator related statistics.
 */
public class PercolateStats implements Streamable, ToXContent {

    private long percolateCount;
    private long percolateTimeInMillis;
    private long current;
    private long memorySizeInBytes;
    private long numQueries;

    /**
     * Noop constructor for serialazation purposes.
     */
    public PercolateStats() {
    }

    PercolateStats(long percolateCount, long percolateTimeInMillis, long current, long memorySizeInBytes, long numQueries) {
        this.percolateCount = percolateCount;
        this.percolateTimeInMillis = percolateTimeInMillis;
        this.current = current;
        this.memorySizeInBytes = memorySizeInBytes;
        this.numQueries = numQueries;
    }

    /**
     * @return The number of times the percolate api has been invoked.
     */
    public long getCount() {
        return percolateCount;
    }

    /**
     * @return The total amount of time spend in the percolate api
     */
    public long getTimeInMillis() {
        return percolateTimeInMillis;
    }

    /**
     * @return The total amount of time spend in the percolate api
     */
    public TimeValue getTime() {
        return new TimeValue(getTimeInMillis());
    }

    /**
     * @return The total amount of active percolate api invocations.
     */
    public long getCurrent() {
        return current;
    }

    /**
     * @return The total number of loaded percolate queries.
     */
    public long getNumQueries() {
        return numQueries;
    }

    /**
     * @return The total size the loaded queries take in memory.
     */
    public long getMemorySizeInBytes() {
        return memorySizeInBytes;
    }

    /**
     * @return The total size the loaded queries take in memory.
     */
    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PERCOLATE);
        builder.field(Fields.TOTAL, percolateCount);
        builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, percolateTimeInMillis);
        builder.field(Fields.CURRENT, current);
        builder.field(Fields.MEMORY_SIZE_IN_BYTES, memorySizeInBytes);
        builder.field(Fields.MEMORY_SIZE, getMemorySize());
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
        memorySizeInBytes += percolate.getMemorySizeInBytes();
        numQueries += percolate.getNumQueries();
    }

    static final class Fields {
        static final XContentBuilderString PERCOLATE = new XContentBuilderString("percolate");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TIME = new XContentBuilderString("getTime");
        static final XContentBuilderString TIME_IN_MILLIS = new XContentBuilderString("time_in_millis");
        static final XContentBuilderString CURRENT = new XContentBuilderString("current");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
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
        memorySizeInBytes = in.readVLong();
        numQueries = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(percolateCount);
        out.writeVLong(percolateTimeInMillis);
        out.writeVLong(current);
        out.writeVLong(memorySizeInBytes);
        out.writeVLong(numQueries);
    }
}
