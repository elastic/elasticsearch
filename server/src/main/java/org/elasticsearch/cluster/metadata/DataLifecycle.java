/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
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

    public static final String DLM_ORIGIN = "data_lifecycle";

    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    public static final ParseField DOWNSAMPLING_FIELD = new ParseField("downsampling");
    public static final ParseField DOWNSAMPLING_AFTER_FIELD = new ParseField("after");
    public static final ParseField DOWNSAMPLING_FIXED_INTERVAL_FIELD = new ParseField("fixed_interval");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");
    public static final ObjectParser<Downsample.Builder, Void> DOWNSAMPLE_PARSER = new ObjectParser<>(
        Downsample.Builder.class.getName(),
        true,
        Downsample.Builder::new
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, unused) -> {
            if (args.length == 2) {
                return new DataLifecycle((Retention) args[0], (List<Downsample>) args[1]);
            } else {
                return new DataLifecycle((Retention) args[0], (List<Downsample>) null);
            }
        }
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

        /*
         * We pass an explicit null marker here because we want to be able to distinguish the difference between the case when the
         * downsampling field does not exist like {"lifecycle": {}}, and the case when the downsampling field is explicitly null like
         * {"lifecycle": {"downsampling": null}}. In the former case the constructor will be passed null. In the latter case the
         * constructor will be passed a singleton list with the sole value being NULL_MARKER.
         */
        PARSER.declareObjectArrayOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> DOWNSAMPLE_PARSER.apply(parser, context).build(),
            DOWNSAMPLING_FIELD,
            Downsample.NULL_MARKER
        );
        DOWNSAMPLE_PARSER.declareString(Downsample.Builder::setAfter, DOWNSAMPLING_AFTER_FIELD);
        DOWNSAMPLE_PARSER.declareString(Downsample.Builder::setFixedInterval, DOWNSAMPLING_FIXED_INTERVAL_FIELD);
    }

    public static boolean isEnabled() {
        return DLM_FEATURE_FLAG.isEnabled();
    }

    @Nullable
    private final Retention dataRetention;
    @Nullable
    private final Downsampling downsampling;

    public DataLifecycle() {
        this((TimeValue) null);
    }

    public DataLifecycle(@Nullable TimeValue dataRetention) {
        this(dataRetention == null ? null : new Retention(dataRetention));
    }

    public DataLifecycle(@Nullable Retention dataRetention) {
        this(dataRetention, (Downsampling) null);
    }

    public DataLifecycle(@Nullable Retention dataRetention, @Nullable Downsampling downsampling) {
        this.dataRetention = dataRetention;
        this.downsampling = downsampling;
    }

    /*
     * This constructor is meant to only be used by the xcontent parser.
     */
    private DataLifecycle(@Nullable Retention retention, @Nullable List<Downsample> downsamples) {
        this.dataRetention = retention;
        if (downsamples == null) {
            /*
             * This is the case where the parser found {"lifecycle": null}
             */
            this.downsampling = null;
        } else if (downsamples.size() == 1 && downsamples.get(0).equals(Downsample.NULL_MARKER)) {
            /*
             * This is the special case where the parser found a {"lifecycle": {"downsampling": null}}, and it notifies us by passing
             * this marker in a singleton list.
             */
            this.downsampling = new Downsampling(null);
        } else {
            this.downsampling = new Downsampling(downsamples);
        }
    }

    public DataLifecycle(long timeInMills) {
        this(TimeValue.timeValueMillis(timeInMills));
    }

    /**
     * The least amount of time data should be kept by elasticsearch.
     * @return the time period or null, null represents that data should never be deleted.
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

    @Nullable
    public Downsampling getDownsampling() {
        return downsampling;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataLifecycle that = (DataLifecycle) o;
        return Objects.equals(dataRetention, that.dataRetention) && Objects.equals(downsampling, that.downsampling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataRetention, downsampling);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            out.writeOptionalWriteable(dataRetention);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_013)) {
            out.writeOptionalWriteable(downsampling);
        }
    }

    public DataLifecycle(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            dataRetention = in.readOptionalWriteable(Retention::read);
        } else {
            dataRetention = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_013)) {
            downsampling = in.readOptionalWriteable(Downsampling::read);
        } else {
            downsampling = null;
        }
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
        if (downsampling != null) {
            if (downsampling.downsamples != null) {
                builder.startArray(DOWNSAMPLING_FIELD.getPreferredName());
                for (Downsample downsample : downsampling.downsamples) {
                    builder.startObject();
                    builder.field(DOWNSAMPLING_AFTER_FIELD.getPreferredName(), downsample.after.getStringRep());
                    builder.field(DOWNSAMPLING_FIXED_INTERVAL_FIELD.getPreferredName(), downsample.fixedInterval.getStringRep());
                    builder.endObject();
                }
                builder.endArray();
            } else {
                builder.nullField(DOWNSAMPLING_FIELD.getPreferredName());
            }
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
        @Nullable
        Downsampling downsampling = null;

        Builder dataRetention(@Nullable Retention value) {
            dataRetention = value;
            return this;
        }

        Builder downsampling(@Nullable Downsampling value) {
            downsampling = value;
            return this;
        }

        DataLifecycle build() {
            return new DataLifecycle(dataRetention, downsampling);
        }

        static Builder newBuilder(DataLifecycle dataLifecycle) {
            return new Builder().dataRetention(dataLifecycle.getDataRetention()).downsampling(dataLifecycle.getDownsampling());
        }
    }

    /**
     * Retention is the least amount of time that the data will be kept by elasticsearch. Public for testing.
     * @param value is a time period or null. Null represents an explicitly set infinite retention period
     */
    public record Retention(@Nullable TimeValue value) implements Writeable {

        // For testing
        public static final Retention NULL = new Retention(null);

        public static Retention read(StreamInput in) throws IOException {
            return new Retention(in.readOptionalTimeValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalTimeValue(value);
        }
    }

    public record Downsampling(List<Downsample> downsamples) implements Writeable {
        public static final Downsampling NULL = new Downsampling(null);

        public static Downsampling read(StreamInput in) throws IOException {
            return new Downsampling(in.readOptionalList(Downsample::read));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalCollection(downsamples);
        }
    }

    public record Downsample(TimeValue after, TimeValue fixedInterval) implements Writeable {

        public static final Downsample NULL_MARKER = new Downsample(TimeValue.MINUS_ONE, TimeValue.MINUS_ONE);

        public static Downsample read(StreamInput in) throws IOException {
            return new Downsample(in.readTimeValue(), in.readTimeValue());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeTimeValue(after);
            out.writeTimeValue(fixedInterval);
        }

        public static class Builder {
            private TimeValue after;
            private TimeValue fixedInterval;

            public void setAfter(String after) {
                this.after = TimeValue.parseTimeValue(after, "");
            }

            public void setFixedInterval(String fixedInterval) {
                this.fixedInterval = TimeValue.parseTimeValue(fixedInterval, "");
            }

            public Downsample build() {
                return new Downsample(after, fixedInterval);
            }
        }
    }
}
