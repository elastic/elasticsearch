/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
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

public class IndexWriteLoadForecast implements Writeable, ToXContentFragment {
    public static final ParseField SHARDS_WRITE_LOAD_FIELD = new ParseField("loads");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexWriteLoadForecast, Void> PARSER = new ConstructingObjectParser<>(
        "index_write_load_forecast_parser",
        false,
        (args, unused) -> new IndexWriteLoadForecast(((List<Double>) args[0]).stream().mapToDouble(value -> value).toArray())
    );

    static {
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), SHARDS_WRITE_LOAD_FIELD);
    }

    private final double[] forecastedShardWriteLoad;

    private IndexWriteLoadForecast(double[] forecastedShardWriteLoad) {
        this.forecastedShardWriteLoad = forecastedShardWriteLoad;
    }

    public IndexWriteLoadForecast(StreamInput in) throws IOException {
        this(in.readDoubleArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(forecastedShardWriteLoad);
    }

    public static ClusterState maybeIncludeWriteLoadForecast(
        IndexAbstraction.DataStream dataStream,
        ClusterState clusterState,
        TimeValue maxIndexAge,
        TimeValue minShardUptime
    ) {
        final IndexMetadata writeIndex = clusterState.metadata().getIndexSafe(dataStream.getWriteIndex());
        if (IndexSettings.FORECAST_WRITE_LOAD_SETTING.get(writeIndex.getSettings()) == false) {
            return clusterState;
        }

        final int numberOfShards = writeIndex.getNumberOfShards();
        final double[] totalWriteLoad = new double[numberOfShards];
        final double[] maxWriteLoad = new double[numberOfShards];
        final int[] numberOfIndicesTookIntoAccount = new int[numberOfShards];

        for (Index index : dataStream.getIndices()) {
            final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(index);
            final long indexAge = System.currentTimeMillis() - indexMetadata.getCreationDate();
            if (index.equals(dataStream.getWriteIndex()) || indexAge > maxIndexAge.millis()) {
                continue;
            }

            final IndexWriteLoad writeLoad = indexMetadata.getWriteLoad();
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                OptionalDouble writeLoadForShard = writeLoad.getWriteLoadForShard(shardId);
                OptionalLong uptimeInMillisForShard = writeLoad.getUptimeInMillisForShard(shardId);
                if (writeLoadForShard.isPresent()) {
                    assert uptimeInMillisForShard.isPresent();
                    double shardWriteLoad = writeLoadForShard.getAsDouble();
                    long shardUptimeInMillis = uptimeInMillisForShard.getAsLong();
                    if (shardUptimeInMillis > minShardUptime.millis()) {
                        totalWriteLoad[shardId] += shardWriteLoad;
                        maxWriteLoad[shardId] = Math.max(shardWriteLoad, maxWriteLoad[shardId]);
                        numberOfIndicesTookIntoAccount[shardId] += 1;
                    }
                }
            }
        }

        IndexWriteLoadForecast.Builder projectedWriteLoad = IndexWriteLoadForecast.builder(numberOfShards);
        boolean modified = false;
        for (int shardId = 0; shardId < totalWriteLoad.length; shardId++) {
            int shardIndicesTookIntoAccount = numberOfIndicesTookIntoAccount[shardId];
            if (shardIndicesTookIntoAccount > 0) {
                modified = true;
                double normalizedShardLoad = (totalWriteLoad[shardId] / shardIndicesTookIntoAccount) / maxWriteLoad[shardId];
                projectedWriteLoad.withForecastedWriteLoad(shardId, normalizedShardLoad);
            }
        }

        if (modified == false) {
            return clusterState;
        }

        IndexWriteLoadForecast indexWriteLoadForecast = projectedWriteLoad.build();

        final IndexMetadata updatedWriteIndexMetadata = IndexMetadata.builder(writeIndex)
            .indexWriteLoadForecast(indexWriteLoadForecast)
            .build();

        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(updatedWriteIndexMetadata, false))
            .build();
    }

    public static IndexWriteLoadForecast fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(SHARDS_WRITE_LOAD_FIELD.getPreferredName(), forecastedShardWriteLoad);
    }

    public OptionalDouble getForecastedWriteLoadForShard(int shardId) {
        double writeLoadForecast = forecastedShardWriteLoad[shardId];
        return writeLoadForecast == -1.0 ? OptionalDouble.empty() : OptionalDouble.of(writeLoadForecast);
    }

    public static Builder builder(int numberOfShards) {
        return new Builder(numberOfShards);
    }

    public static class Builder {
        private final double[] shardWriteLoadForecast;

        public Builder(int numberOfShards) {
            this.shardWriteLoadForecast = new double[numberOfShards];
            Arrays.fill(shardWriteLoadForecast, -1);
        }

        public void withForecastedWriteLoad(int shardId, double shardLoad) {
            shardWriteLoadForecast[shardId] = shardLoad;
        }

        public IndexWriteLoadForecast build() {
            return new IndexWriteLoadForecast(shardWriteLoadForecast);
        }
    }
}
