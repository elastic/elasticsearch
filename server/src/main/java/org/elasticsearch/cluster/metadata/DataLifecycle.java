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
import org.elasticsearch.common.io.stream.Writeable;
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
    // This represents when the data lifecycle was explicitly set to be null.
    // This value takes effect only if it is defined in the index template during the resolution of the lifecycle templates
    public static final DataLifecycle NULL = new DataLifecycle(null, true);
    public static final String DLM_ORIGIN = "data_lifecycle";

    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    public static final ConstructingObjectParser<DataLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, unused) -> new DataLifecycle((Retention) args[0], false)
    );

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            String value = p.textOrNull();
            if (value == null) {
                return Retention.NULL;
            } else {
                return new Retention(TimeValue.parseTimeValue(value, DATA_RETENTION_FIELD.getPreferredName()));
            }
        }, DATA_RETENTION_FIELD, ObjectParser.ValueType.STRING_OR_NULL);
    }

    public static boolean isEnabled() {
        return DLM_FEATURE_FLAG.isEnabled();
    }

    @Nullable
    private final Retention dataRetention;
    private final boolean nullified;

    public DataLifecycle() {
        this(null, false);
    }

    public DataLifecycle(@Nullable TimeValue dataRetention) {
        this(new Retention(dataRetention), false);
    }

    private DataLifecycle(@Nullable Retention dataRetention, boolean nullified) {
        this.dataRetention = dataRetention;
        this.nullified = nullified;
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
            if (current.isNullified()) {
                builder = null;
            } else if (builder == null) {
                builder = Builder.newBuilder(current);
            } else {
                if (current.dataRetention != null) {
                    builder.dataRetention(current.getDataRetention());
                }
            }
        }
        return builder == null ? null : builder.build();
    }

    /**
     * The least amount of time data should be kept by elasticsearch.
     * @return the time period or null, null represents the data should never be deleted.
     */
    @Nullable
    public TimeValue getEffectiveDataRetention() {
        return dataRetention == null ? null : dataRetention.value;
    }

    /**
     * The configuration as provided by the user about the least amount of time data should be kept by elasticsearch.
     * This method differentiates between a missing retention and a nullified retention and this is useful for template
     * composition.
     * @return one of the following:
     * - `null`, represents that the user did not provide data retention, this represents the user has no opinion about retention
     * - `Retention{value = null}`, represents that the user explicitly wants to have infinite retention
     * - `Retention{value = "10d"}`, represents that the user has requested the data to be kept at least 10d.
     */
    @Nullable
    Retention getDataRetention() {
        return dataRetention;
    }

    public boolean isNullified() {
        return nullified;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataLifecycle that = (DataLifecycle) o;
        return Objects.equals(dataRetention, that.dataRetention) && this.nullified == that.nullified;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataRetention, nullified);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dataRetention);
        out.writeBoolean(nullified);
    }

    public DataLifecycle(StreamInput in) throws IOException {
        this(in.readOptionalWriteable(Retention::read), in.readBoolean());
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
        if (nullified) {
            return builder.nullValue();
        }

        builder.startObject();
        if (dataRetention != null) {
            if (dataRetention.value() == null) {
                builder.nullField(DATA_RETENTION_FIELD.getPreferredName());
            } else {
                builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.value().getStringRep());
            }
        }
        if (rolloverConfiguration != null) {
            builder.field(ROLLOVER_FIELD.getPreferredName());
            rolloverConfiguration.evaluateAndConvertToXContent(builder, params, getEffectiveDataRetention());
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
        private Retention dataRetention = null;
        private boolean nullified = false;

        Builder dataRetention(@Nullable Retention value) {
            dataRetention = value;
            return this;
        }

        Builder nullified(boolean value) {
            nullified = value;
            return this;
        }

        DataLifecycle build() {
            return new DataLifecycle(dataRetention, nullified);
        }

        static Builder newBuilder(DataLifecycle dataLifecycle) {
            return new Builder().dataRetention(dataLifecycle.getDataRetention()).nullified(dataLifecycle.isNullified());
        }
    }

    /**
     * Retention is the least amount of time that the data will be kept by elasticsearch.
     * @param value is a time period or null. Null represents an explicitly set infinite retention period
     */
    record Retention(@Nullable TimeValue value) implements Writeable {

        public static final Retention NULL = new Retention(null);

        public static Retention read(StreamInput in) throws IOException {
            return new Retention(in.readOptionalTimeValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalTimeValue(value);
        }
    }
}
