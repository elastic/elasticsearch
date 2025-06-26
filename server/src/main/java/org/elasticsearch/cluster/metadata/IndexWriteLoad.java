/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static org.elasticsearch.TransportVersions.INDEX_METADATA_INCLUDES_RECENT_WRITE_LOAD;
import static org.elasticsearch.TransportVersions.INDEX_STATS_AND_METADATA_INCLUDE_PEAK_WRITE_LOAD;

public class IndexWriteLoad implements Writeable, ToXContentFragment {
    public static final ParseField SHARDS_WRITE_LOAD_FIELD = new ParseField("loads");
    public static final ParseField SHARDS_UPTIME_IN_MILLIS = new ParseField("uptimes");
    public static final ParseField SHARDS_RECENT_WRITE_LOAD_FIELD = new ParseField("recent_loads");
    public static final ParseField SHARDS_PEAK_WRITE_LOAD_FIELD = new ParseField("peak_loads");
    private static final Double UNKNOWN_LOAD = -1.0;
    private static final long UNKNOWN_UPTIME = -1;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexWriteLoad, Void> PARSER = new ConstructingObjectParser<>(
        "index_write_load_parser",
        false,
        (args, unused) -> IndexWriteLoad.create(
            (List<Double>) args[0],
            (List<Long>) args[1],
            (List<Double>) args[2],
            (List<Double>) args[3]
        )
    );

    static {
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), SHARDS_WRITE_LOAD_FIELD);
        PARSER.declareLongArray(ConstructingObjectParser.constructorArg(), SHARDS_UPTIME_IN_MILLIS);
        // The recent and peak write load fields are optional so that we can parse XContent built by older versions which did not have them:
        PARSER.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), SHARDS_RECENT_WRITE_LOAD_FIELD);
        PARSER.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), SHARDS_PEAK_WRITE_LOAD_FIELD);
    }

    private static IndexWriteLoad create(
        List<Double> shardsWriteLoad,
        List<Long> shardsUptimeInMillis,
        @Nullable List<Double> shardsRecentWriteLoad,
        @Nullable List<Double> shardsPeakWriteLoad
    ) {
        if (shardsWriteLoad.size() != shardsUptimeInMillis.size()) {
            assert false : "IndexWriteLoad.create() was called with non-matched lengths for shardWriteLoad and shardUptimeInMillis";
            throw new IllegalArgumentException(
                "The same number of shard write loads and shard uptimes should be provided, but "
                    + shardsWriteLoad
                    + " "
                    + shardsUptimeInMillis
                    + " were provided"
            );
        }

        if (shardsWriteLoad.isEmpty()) {
            assert false : "IndexWriteLoad.create() was called with empty shardsRecentWriteLoad";
            ;
            throw new IllegalArgumentException("At least one shard write load and uptime should be provided, but none was provided");
        }

        if (shardsRecentWriteLoad != null && shardsRecentWriteLoad.size() != shardsUptimeInMillis.size()) {
            assert false : "IndexWriteLoad.create() was called with non-matched lengths for shardsRecentWriteLoad and shardUptimeInMillis";
            throw new IllegalArgumentException(
                "The same number of shard recent write loads and shard uptimes should be provided, but "
                    + shardsRecentWriteLoad
                    + " "
                    + shardsUptimeInMillis
                    + " were provided"
            );
        }

        if (shardsPeakWriteLoad != null && shardsPeakWriteLoad.size() != shardsUptimeInMillis.size()) {
            assert false : "IndexWriteLoad.create() was called with non-matched lengths for shardsPeakWriteLoad and shardUptimeInMillis";
            throw new IllegalArgumentException(
                "The same number of shard peak write loads and shard uptimes should be provided, but "
                    + shardsPeakWriteLoad
                    + " "
                    + shardsUptimeInMillis
                    + " were provided"
            );
        }

        return new IndexWriteLoad(
            shardsWriteLoad.stream().mapToDouble(shardLoad -> shardLoad).toArray(),
            shardsUptimeInMillis.stream().mapToLong(shardUptime -> shardUptime).toArray(),
            shardsRecentWriteLoad != null ? shardsRecentWriteLoad.stream().mapToDouble(shardLoad -> shardLoad).toArray() : null,
            shardsPeakWriteLoad != null ? shardsPeakWriteLoad.stream().mapToDouble(shardLoad -> shardLoad).toArray() : null
        );
    }

    private final double[] shardWriteLoad;
    private final long[] shardUptimeInMillis;
    private final double[] shardRecentWriteLoad;
    private final double[] shardPeakWriteLoad;

    private IndexWriteLoad(
        double[] shardWriteLoad,
        long[] shardUptimeInMillis,
        @Nullable double[] shardRecentWriteLoad,
        @Nullable double[] shardPeakWriteLoad
    ) {
        assert shardWriteLoad.length == shardUptimeInMillis.length
            : "IndexWriteLoad constructor was called with non-matched lengths for shardWriteLoad and shardUptimeInMillis";
        this.shardWriteLoad = shardWriteLoad;
        this.shardUptimeInMillis = shardUptimeInMillis;
        if (shardRecentWriteLoad != null) {
            assert shardRecentWriteLoad.length == shardUptimeInMillis.length
                : "IndexWriteLoad constructor was called with non-matched lengths for shardRecentWriteLoad and shardUptimeInMillis";
            this.shardRecentWriteLoad = shardRecentWriteLoad;
        } else {
            this.shardRecentWriteLoad = new double[shardUptimeInMillis.length];
            Arrays.fill(this.shardRecentWriteLoad, UNKNOWN_LOAD);
        }
        if (shardPeakWriteLoad != null) {
            assert shardPeakWriteLoad.length == shardUptimeInMillis.length
                : "IndexWriteLoad constructor was called with non-matched lengths for shardPeakWriteLoad and shardUptimeInMillis";
            this.shardPeakWriteLoad = shardPeakWriteLoad;
        } else {
            this.shardPeakWriteLoad = new double[shardUptimeInMillis.length];
            Arrays.fill(this.shardPeakWriteLoad, UNKNOWN_LOAD);
        }
    }

    public IndexWriteLoad(StreamInput in) throws IOException {
        this(
            in.readDoubleArray(),
            in.readLongArray(),
            in.getTransportVersion().onOrAfter(INDEX_METADATA_INCLUDES_RECENT_WRITE_LOAD) ? in.readDoubleArray() : null,
            in.getTransportVersion().onOrAfter(INDEX_STATS_AND_METADATA_INCLUDE_PEAK_WRITE_LOAD) ? in.readDoubleArray() : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(shardWriteLoad);
        out.writeLongArray(shardUptimeInMillis);
        if (out.getTransportVersion().onOrAfter(INDEX_METADATA_INCLUDES_RECENT_WRITE_LOAD)) {
            out.writeDoubleArray(shardRecentWriteLoad);
        }
        if (out.getTransportVersion().onOrAfter(INDEX_STATS_AND_METADATA_INCLUDE_PEAK_WRITE_LOAD)) {
            out.writeDoubleArray(shardPeakWriteLoad);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SHARDS_WRITE_LOAD_FIELD.getPreferredName(), shardWriteLoad);
        builder.field(SHARDS_UPTIME_IN_MILLIS.getPreferredName(), shardUptimeInMillis);
        builder.field(SHARDS_RECENT_WRITE_LOAD_FIELD.getPreferredName(), shardRecentWriteLoad);
        builder.field(SHARDS_PEAK_WRITE_LOAD_FIELD.getPreferredName(), shardPeakWriteLoad);
        return builder;
    }

    public static IndexWriteLoad fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public OptionalDouble getWriteLoadForShard(int shardId) {
        assertShardInBounds(shardId);

        double load = shardWriteLoad[shardId];
        return load != UNKNOWN_LOAD ? OptionalDouble.of(load) : OptionalDouble.empty();
    }

    public OptionalDouble getRecentWriteLoadForShard(int shardId) {
        assertShardInBounds(shardId);

        double load = shardRecentWriteLoad[shardId];
        return load != UNKNOWN_LOAD ? OptionalDouble.of(load) : OptionalDouble.empty();
    }

    public OptionalDouble getPeakWriteLoadForShard(int shardId) {
        assertShardInBounds(shardId);

        double load = shardPeakWriteLoad[shardId];
        return load != UNKNOWN_LOAD ? OptionalDouble.of(load) : OptionalDouble.empty();
    }

    public OptionalLong getUptimeInMillisForShard(int shardId) {
        assertShardInBounds(shardId);

        long uptime = shardUptimeInMillis[shardId];
        return uptime != UNKNOWN_UPTIME ? OptionalLong.of(uptime) : OptionalLong.empty();
    }

    public int numberOfShards() {
        return shardWriteLoad.length;
    }

    private void assertShardInBounds(int shardId) {
        assert shardId >= 0 : "Unexpected shard id " + shardId;
        assert shardId < shardWriteLoad.length : "Unexpected shard id " + shardId + ", expected < " + shardWriteLoad.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexWriteLoad that = (IndexWriteLoad) o;
        return Arrays.equals(shardWriteLoad, that.shardWriteLoad)
            && Arrays.equals(shardUptimeInMillis, that.shardUptimeInMillis)
            && Arrays.equals(shardRecentWriteLoad, that.shardRecentWriteLoad)
            && Arrays.equals(shardPeakWriteLoad, that.shardPeakWriteLoad);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(shardWriteLoad);
        result = 31 * result + Arrays.hashCode(shardUptimeInMillis);
        result = 31 * result + Arrays.hashCode(shardRecentWriteLoad);
        result = 31 * result + Arrays.hashCode(shardPeakWriteLoad);
        return result;
    }

    public static Builder builder(int numShards) {
        assert numShards > 0 : "A positive number of shards should be provided";
        return new Builder(numShards);
    }

    public static class Builder {
        private final double[] shardWriteLoad;
        private final long[] uptimeInMillis;
        private final double[] shardRecentWriteLoad;
        private final double[] shardPeakWriteLoad;

        private Builder(int numShards) {
            this.shardWriteLoad = new double[numShards];
            this.uptimeInMillis = new long[numShards];
            this.shardRecentWriteLoad = new double[numShards];
            this.shardPeakWriteLoad = new double[numShards];
            Arrays.fill(shardWriteLoad, UNKNOWN_LOAD);
            Arrays.fill(uptimeInMillis, UNKNOWN_UPTIME);
            Arrays.fill(shardRecentWriteLoad, UNKNOWN_LOAD);
            Arrays.fill(shardPeakWriteLoad, UNKNOWN_LOAD);
        }

        public Builder withShardWriteLoad(int shardId, double load, double recentLoad, double peakLoad, long uptimeInMillis) {
            if (shardId >= this.shardWriteLoad.length) {
                throw new IllegalArgumentException();
            }

            this.shardWriteLoad[shardId] = load;
            this.uptimeInMillis[shardId] = uptimeInMillis;
            this.shardRecentWriteLoad[shardId] = recentLoad;
            this.shardPeakWriteLoad[shardId] = peakLoad;

            return this;
        }

        public IndexWriteLoad build() {
            return new IndexWriteLoad(shardWriteLoad, uptimeInMillis, shardRecentWriteLoad, shardPeakWriteLoad);
        }
    }
}
