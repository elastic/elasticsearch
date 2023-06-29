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
import org.elasticsearch.action.downsample.DownsampleConfig;
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
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Holds the Data Lifecycle Management metadata that are configuring how a data stream is managed. Currently, it supports the following
 * configurations:
 * - data retention
 * - downsampling
 */
public class DataLifecycle implements SimpleDiffable<DataLifecycle>, ToXContentObject {

    public static final Setting<RolloverConfiguration> CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING = new Setting<>(
        "cluster.lifecycle.default.rollover",
        "max_age=auto,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000",
        (s) -> RolloverConfiguration.parseSetting(s, "cluster.lifecycle.default.rollover"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final FeatureFlag DLM_FEATURE_FLAG = new FeatureFlag("dlm");

    public static final String DLM_ORIGIN = "data_lifecycle";

    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    public static final ParseField DOWNSAMPLING_FIELD = new ParseField("downsampling");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    public static final ConstructingObjectParser<DataLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, unused) -> new DataLifecycle((Retention) args[0], (Downsampling) args[1])
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
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Downsampling.NULL;
            } else {
                return new Downsampling(AbstractObjectParser.parseArray(p, c, Downsampling.Round::fromXContent));
            }
        }, DOWNSAMPLING_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL);
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
        this(dataRetention == null ? null : new Retention(dataRetention), null);
    }

    public DataLifecycle(@Nullable Retention dataRetention, @Nullable Downsampling downsampling) {
        this.dataRetention = dataRetention;
        this.downsampling = downsampling;
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

    /**
     * The configured downsampling rounds with the `after` and the `fixed_interval` per round. If downsampling is
     * not configured then it returns null.
     */
    @Nullable
    public List<Downsampling.Round> getDownsamplingRounds() {
        return downsampling == null ? null : downsampling.rounds();
    }

    /**
     * Returns the configured wrapper object as it was defined in the template. This should be used only during
     * template composition.
     */
    @Nullable
    Downsampling getDownsampling() {
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
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_025)) {
            out.writeOptionalWriteable(downsampling);
        }
    }

    public DataLifecycle(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            dataRetention = in.readOptionalWriteable(Retention::read);
        } else {
            dataRetention = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_025)) {
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
        if (downsampling != null) {
            builder.field(DOWNSAMPLING_FIELD.getPreferredName());
            downsampling.toXContent(builder, params);
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
        @Nullable
        private Downsampling downsampling = null;

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

    /**
     * Downsampling holds the configuration about when should elasticsearch downsample a backing index.
     * @param rounds is a list of downsampling configuration which instructs when a backing index should be downsampled (`after`) and at
     *               which interval (`fixed_interval`). Null represents an explicit no downsampling during template composition.
     */
    public record Downsampling(@Nullable List<Round> rounds) implements Writeable, ToXContentFragment {

        /**
         * A round represents the configuration for when and how elasticsearch will downsample a backing index.
         * @param after is a TimeValue configuring how old (based on generation age) should a backing index be before downsampling
         * @param config contains the interval that the backing index is going to be downsampled.
         */
        public record Round(TimeValue after, DownsampleConfig config) implements Writeable, ToXContentObject {

            public static final ParseField AFTER_FIELD = new ParseField("after");
            public static final ParseField FIXED_INTERVAL_FIELD = new ParseField("fixed_interval");

            private static final ConstructingObjectParser<Round, Void> PARSER = new ConstructingObjectParser<>(
                "downsampling_round",
                false,
                (args, unused) -> new Round((TimeValue) args[0], new DownsampleConfig((DateHistogramInterval) args[1]))
            );

            static {
                PARSER.declareString(
                    ConstructingObjectParser.optionalConstructorArg(),
                    value -> TimeValue.parseTimeValue(value, AFTER_FIELD.getPreferredName()),
                    AFTER_FIELD
                );
                PARSER.declareField(
                    constructorArg(),
                    p -> new DateHistogramInterval(p.text()),
                    new ParseField(FIXED_INTERVAL_FIELD.getPreferredName()),
                    ObjectParser.ValueType.STRING
                );
            }

            public static Round read(StreamInput in) throws IOException {
                return new Round(in.readTimeValue(), new DownsampleConfig(in));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeTimeValue(after);
                out.writeWriteable(config);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(AFTER_FIELD.getPreferredName(), after.getStringRep());
                config.toXContentFragment(builder);
                builder.endObject();
                return builder;
            }

            public static Round fromXContent(XContentParser parser, Void context) throws IOException {
                return PARSER.parse(parser, context);
            }

            @Override
            public String toString() {
                return Strings.toString(this, true, true);
            }
        }

        // For testing
        public static final Downsampling NULL = new Downsampling(null);

        public Downsampling {
            if (rounds != null) {
                if (rounds.isEmpty()) {
                    throw new IllegalArgumentException("Downsampling configuration should have at least one round configured.");
                }
                Round previous = null;
                for (Round round : rounds) {
                    if (previous == null) {
                        previous = round;
                    } else {
                        if (round.after.compareTo(previous.after) < 0) {
                            throw new IllegalArgumentException(
                                "A downsampling round must have a later 'after' value than the proceeding, "
                                    + round.after.getStringRep()
                                    + " is not after "
                                    + previous.after.getStringRep()
                                    + "."
                            );
                        }
                        DownsampleConfig.validateSourceAndTargetIntervals(previous.config(), round.config());
                    }
                }
            }
        }

        public static Downsampling read(StreamInput in) throws IOException {
            return new Downsampling(in.readOptionalList(Round::read));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalCollection(rounds, (o, v) -> v.writeTo(o));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (rounds == null) {
                builder.nullValue();
            } else {
                builder.startArray();
                for (Round round : rounds) {
                    round.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder;
        }
    }
}
