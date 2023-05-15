/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Holds the Data Lifecycle Management metadata that are configuring how a data stream is managed.
 */
public class DataLifecycle implements SimpleDiffable<DataLifecycle>, ToXContentObject {

    public static final Setting<RolloverConfiguration> CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING = new Setting<>(
        "cluster.dlm.default.rollover",
        "max_age=auto,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000",
        (s) -> RolloverConfiguration.parseSetting(s, "cluster.dlm.default.rollover"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final FeatureFlag DLM_FEATURE_FLAG = new FeatureFlag("dlm");

    public static final DataLifecycle EMPTY = new DataLifecycle();
    public static final String DLM_ORIGIN = "data_lifecycle";

    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    public static final ConstructingObjectParser<DataLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, unused) -> new DataLifecycle((TimeValue) args[0])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), DATA_RETENTION_FIELD.getPreferredName()),
            DATA_RETENTION_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
    }

    public static boolean isEnabled() {
        return DLM_FEATURE_FLAG.isEnabled();
    }

    @Nullable
    private final TimeValue dataRetention;

    public DataLifecycle() {
        this.dataRetention = null;
    }

    public DataLifecycle(@Nullable TimeValue dataRetention) {
        this.dataRetention = dataRetention;
    }

    public DataLifecycle(long timeInMills) {
        this(TimeValue.timeValueMillis(timeInMills));
    }

    /**
     * This method composes a series of lifecycles to a final one. The lifecycles are getting composed one level deep,
     * meaning that the keys present on the latest lifecycle will override the ones of the others. If a key is missing
     * then it keeps the value of the previous lifecycles. For example, if we have the following two lifecycles:
     * [
     *   {
     *     "lifecycle": {
     *       "data_retention" : "10d"
     *     }
     *   },
     *   {
     *     "lifecycle": {
     *       "data_retention" : "20d"
     *     }
     *   }
     * ]
     * The result will be { "lifecycle": { "data_retention" : "20d"}} because the second data retention overrides the first.
     * However, if we have the following two lifecycles:
     * [
     *   {
     *     "lifecycle": {
     *       "data_retention" : "10d"
     *     }
     *   },
     *   {
     *   "lifecycle": { }
     *   }
     * ]
     * The result will be { "lifecycle": { "data_retention" : "10d"} } because the latest lifecycle does not have any
     * information on retention.
     * @param lifecycles a sorted list of lifecycles in the order that they will be composed
     * @return the final lifecycle
     */
    @Nullable
    public static DataLifecycle compose(List<DataLifecycle> lifecycles) {
        DataLifecycle.Builder builder = null;
        for (DataLifecycle current : lifecycles) {
            if (builder == null) {
                builder = Builder.newBuilder(current);
            } else {
                if (current.dataRetention != null) {
                    builder.dataRetention(current.getDataRetention());
                }
            }
        }
        return builder == null ? null : builder.build();
    }

    @Nullable
    public TimeValue getDataRetention() {
        return dataRetention;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataLifecycle that = (DataLifecycle) o;
        return Objects.equals(dataRetention, that.dataRetention);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataRetention);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(dataRetention);
    }

    public DataLifecycle(StreamInput in) throws IOException {
        dataRetention = in.readOptionalTimeValue();
    }

    public static Diff<DataLifecycle> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataLifecycle::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null);
    }

    /**
     * Converts the data lifecycle to XContent and injects the RolloverConditions if they exist.
     */
    public XContentBuilder toXContent(XContentBuilder builder, Params params, @Nullable RolloverConfiguration rolloverConfiguration)
        throws IOException {
        builder.startObject();
        if (dataRetention != null) {
            builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.getStringRep());
        }
        if (rolloverConfiguration != null) {
            builder.field(ROLLOVER_FIELD.getPreferredName());
            rolloverConfiguration.evaluateAndConvertToXContent(builder, params, dataRetention);
        }
        builder.endObject();
        return builder;
    }

    public static DataLifecycle fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This builder helps during the composition of the data lifecycle templates.
     */
    static class Builder {
        @Nullable
        private TimeValue dataRetention = null;

        Builder dataRetention(@Nullable TimeValue value) {
            dataRetention = value;
            return this;
        }

        DataLifecycle build() {
            return new DataLifecycle(dataRetention);
        }

        static Builder newBuilder(DataLifecycle dataLifecycle) {
            return new Builder().dataRetention(dataLifecycle.getDataRetention());
        }
    }
}
