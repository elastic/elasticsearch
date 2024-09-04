/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Holds the data stream lifecycle metadata that are configuring how a data stream is managed. Currently, it supports the following
 * configurations:
 * - enabled
 * - data retention
 * - downsampling
 */
public class DataStreamLifecycle implements SimpleDiffable<DataStreamLifecycle>, ToXContentObject {

    // Versions over the wire
    public static final TransportVersion ADDED_ENABLED_FLAG_VERSION = TransportVersions.V_8_10_X;
    public static final String EFFECTIVE_RETENTION_REST_API_CAPABILITY = "data_stream_lifecycle_effective_retention";

    public static final String DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME = "data_streams.lifecycle_only.mode";
    // The following XContent params are used to enrich the DataStreamLifecycle json with effective retention information
    // This should be set only when the lifecycle is used in a response to the user and NEVER when we expect the json to
    // be deserialized.
    public static final String INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME = "include_effective_retention";
    public static final Map<String, String> INCLUDE_EFFECTIVE_RETENTION_PARAMS = Map.of(
        DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME,
        "true"
    );
    public static final Tuple<TimeValue, RetentionSource> INFINITE_RETENTION = Tuple.tuple(null, RetentionSource.DATA_STREAM_CONFIGURATION);

    /**
     * Check if {@link #DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME} is present and set to {@code true}, indicating that
     * we're running in a cluster configuration that is only expecting to use data streams lifecycles.
     *
     * @param settings the node settings
     * @return true if {@link #DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME} is present and set
     */
    public static boolean isDataStreamsLifecycleOnlyMode(final Settings settings) {
        return settings.getAsBoolean(DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME, false);
    }

    public static final Setting<RolloverConfiguration> CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING = new Setting<>(
        "cluster.lifecycle.default.rollover",
        "max_age=auto,max_primary_shard_size=50gb,min_docs=1,max_primary_shard_docs=200000000",
        (s) -> RolloverConfiguration.parseSetting(s, "cluster.lifecycle.default.rollover"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final DataStreamLifecycle DEFAULT = new DataStreamLifecycle();

    public static final String DATA_STREAM_LIFECYCLE_ORIGIN = "data_stream_lifecycle";

    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    public static final ParseField EFFECTIVE_RETENTION_FIELD = new ParseField("effective_retention");
    public static final ParseField RETENTION_SOURCE_FIELD = new ParseField("retention_determined_by");
    public static final ParseField DOWNSAMPLING_FIELD = new ParseField("downsampling");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    public static final ConstructingObjectParser<DataStreamLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, unused) -> new DataStreamLifecycle((Retention) args[0], (Downsampling) args[1], (Boolean) args[2])
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
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    @Nullable
    private final Retention dataRetention;
    @Nullable
    private final Downsampling downsampling;
    private final boolean enabled;

    public DataStreamLifecycle() {
        this(null, null, null);
    }

    public DataStreamLifecycle(@Nullable Retention dataRetention, @Nullable Downsampling downsampling, @Nullable Boolean enabled) {
        this.enabled = enabled == null || enabled;
        this.dataRetention = dataRetention;
        this.downsampling = downsampling;
    }

    /**
     * Returns true, if this data stream lifecycle configuration is enabled and false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * The least amount of time data should be kept by elasticsearch. The effective retention is a function with three parameters,
     * the {@link DataStreamLifecycle#dataRetention}, the global retention and whether this lifecycle is associated with an internal
     * data stream.
     * @param globalRetention The global retention, or null if global retention does not exist.
     * @param isInternalDataStream A flag denoting if this lifecycle is associated with an internal data stream or not
     * @return the time period or null, null represents that data should never be deleted.
     */
    @Nullable
    public TimeValue getEffectiveDataRetention(@Nullable DataStreamGlobalRetention globalRetention, boolean isInternalDataStream) {
        return getEffectiveDataRetentionWithSource(globalRetention, isInternalDataStream).v1();
    }

    /**
     * The least amount of time data should be kept by elasticsearch.. The effective retention is a function with three parameters,
     * the {@link DataStreamLifecycle#dataRetention}, the global retention and whether this lifecycle is associated with an internal
     * data stream.
     * @param globalRetention The global retention, or null if global retention does not exist.
     * @param isInternalDataStream A flag denoting if this lifecycle is associated with an internal data stream or not
     * @return A tuple containing the time period or null as v1 (where null represents that data should never be deleted), and the non-null
     * retention source as v2.
     */
    public Tuple<TimeValue, RetentionSource> getEffectiveDataRetentionWithSource(
        @Nullable DataStreamGlobalRetention globalRetention,
        boolean isInternalDataStream
    ) {
        // If lifecycle is disabled there is no effective retention
        if (enabled == false) {
            return INFINITE_RETENTION;
        }
        var dataStreamRetention = getDataStreamRetention();
        if (globalRetention == null || isInternalDataStream) {
            return Tuple.tuple(dataStreamRetention, RetentionSource.DATA_STREAM_CONFIGURATION);
        }
        if (dataStreamRetention == null) {
            return globalRetention.defaultRetention() != null
                ? Tuple.tuple(globalRetention.defaultRetention(), RetentionSource.DEFAULT_GLOBAL_RETENTION)
                : Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        }
        if (globalRetention.maxRetention() != null && globalRetention.maxRetention().getMillis() < dataStreamRetention.getMillis()) {
            return Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        } else {
            return Tuple.tuple(dataStreamRetention, RetentionSource.DATA_STREAM_CONFIGURATION);
        }
    }

    /**
     * The least amount of time data the data stream is requesting es to keep the data.
     * NOTE: this can be overridden by the {@link DataStreamLifecycle#getEffectiveDataRetention(DataStreamGlobalRetention,boolean)}.
     * @return the time period or null, null represents that data should never be deleted.
     */
    @Nullable
    public TimeValue getDataStreamRetention() {
        return dataRetention == null ? null : dataRetention.value;
    }

    /**
     * This method checks if the effective retention is matching what the user has configured; if the effective retention
     * does not match then it adds a warning informing the user about the effective retention and the source.
     */
    public void addWarningHeaderIfDataRetentionNotEffective(
        @Nullable DataStreamGlobalRetention globalRetention,
        boolean isInternalDataStream
    ) {
        if (globalRetention == null || isInternalDataStream) {
            return;
        }
        Tuple<TimeValue, DataStreamLifecycle.RetentionSource> effectiveDataRetentionWithSource = getEffectiveDataRetentionWithSource(
            globalRetention,
            isInternalDataStream
        );
        if (effectiveDataRetentionWithSource.v1() == null) {
            return;
        }
        String effectiveRetentionStringRep = effectiveDataRetentionWithSource.v1().getStringRep();
        switch (effectiveDataRetentionWithSource.v2()) {
            case DEFAULT_GLOBAL_RETENTION -> HeaderWarning.addWarning(
                "Not providing a retention is not allowed for this project. The default retention of ["
                    + effectiveRetentionStringRep
                    + "] will be applied."
            );
            case MAX_GLOBAL_RETENTION -> {
                String retentionProvidedPart = getDataStreamRetention() == null
                    ? "Not providing a retention is not allowed for this project."
                    : "The retention provided ["
                        + (getDataStreamRetention() == null ? "infinite" : getDataStreamRetention().getStringRep())
                        + "] is exceeding the max allowed data retention of this project ["
                        + effectiveRetentionStringRep
                        + "].";
                HeaderWarning.addWarning(
                    retentionProvidedPart + " The max retention of [" + effectiveRetentionStringRep + "] will be applied"
                );
            }
            case DATA_STREAM_CONFIGURATION -> {
            }
        }
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

        final DataStreamLifecycle that = (DataStreamLifecycle) o;
        return Objects.equals(dataRetention, that.dataRetention)
            && Objects.equals(downsampling, that.downsampling)
            && enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataRetention, downsampling, enabled);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeOptionalWriteable(dataRetention);
        }
        if (out.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
            out.writeOptionalWriteable(downsampling);
            out.writeBoolean(enabled);
        }
    }

    public DataStreamLifecycle(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            dataRetention = in.readOptionalWriteable(Retention::read);
        } else {
            dataRetention = null;
        }
        if (in.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
            downsampling = in.readOptionalWriteable(Downsampling::read);
            enabled = in.readBoolean();
        } else {
            downsampling = null;
            enabled = true;
        }
    }

    public static Diff<DataStreamLifecycle> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamLifecycle::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null, null, false);
    }

    /**
     * Converts the data stream lifecycle to XContent, enriches it with effective retention information when requested
     * and injects the RolloverConditions if they exist.
     * In order to request the effective retention you need to set {@link #INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME} to true
     * in the XContent params.
     * NOTE: this is used for serialising user output and the result is never deserialised in elasticsearch.
     */
    public XContentBuilder toXContent(
        XContentBuilder builder,
        Params params,
        @Nullable RolloverConfiguration rolloverConfiguration,
        @Nullable DataStreamGlobalRetention globalRetention,
        boolean isInternalDataStream
    ) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        if (dataRetention != null) {
            if (dataRetention.value() == null) {
                builder.nullField(DATA_RETENTION_FIELD.getPreferredName());
            } else {
                builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.value().getStringRep());
            }
        }
        Tuple<TimeValue, RetentionSource> effectiveDataRetentionWithSource = getEffectiveDataRetentionWithSource(
            globalRetention,
            isInternalDataStream
        );
        if (params.paramAsBoolean(INCLUDE_EFFECTIVE_RETENTION_PARAM_NAME, false)) {
            if (effectiveDataRetentionWithSource.v1() != null) {
                builder.field(EFFECTIVE_RETENTION_FIELD.getPreferredName(), effectiveDataRetentionWithSource.v1().getStringRep());
                builder.field(RETENTION_SOURCE_FIELD.getPreferredName(), effectiveDataRetentionWithSource.v2().displayName());
            }
        }

        if (downsampling != null) {
            builder.field(DOWNSAMPLING_FIELD.getPreferredName());
            downsampling.toXContent(builder, params);
        }
        if (rolloverConfiguration != null) {
            builder.field(ROLLOVER_FIELD.getPreferredName());
            rolloverConfiguration.evaluateAndConvertToXContent(builder, params, effectiveDataRetentionWithSource.v1());
        }
        builder.endObject();
        return builder;
    }

    /**
     * This method deserialises XContent format as it was generated ONLY by {@link DataStreamLifecycle#toXContent(XContentBuilder, Params)}.
     * It does not support the output of
     * {@link DataStreamLifecycle#toXContent(XContentBuilder, Params, RolloverConfiguration, DataStreamGlobalRetention, boolean)} because
     * this output is enriched with derived fields we do not handle in this deserialisation.
     */
    public static DataStreamLifecycle fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Adds a retention param to signal that this serialisation should include the effective retention metadata.
     * @param params the XContent params to be extended with the new flag
     * @return XContent params with `include_effective_retention` set to true. If the flag exists it will override it.
     */
    public static ToXContent.Params addEffectiveRetentionParams(ToXContent.Params params) {
        return new DelegatingMapParams(INCLUDE_EFFECTIVE_RETENTION_PARAMS, params);
    }

    public static Builder newBuilder(DataStreamLifecycle lifecycle) {
        return new Builder().dataRetention(lifecycle.getDataRetention())
            .downsampling(lifecycle.getDownsampling())
            .enabled(lifecycle.isEnabled());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * This builder helps during the composition of the data stream lifecycle templates.
     */
    public static class Builder {
        @Nullable
        private Retention dataRetention = null;
        @Nullable
        private Downsampling downsampling = null;
        private boolean enabled = true;

        public Builder enabled(boolean value) {
            enabled = value;
            return this;
        }

        public Builder dataRetention(@Nullable Retention value) {
            dataRetention = value;
            return this;
        }

        public Builder dataRetention(@Nullable TimeValue value) {
            dataRetention = value == null ? null : new Retention(value);
            return this;
        }

        public Builder dataRetention(long value) {
            dataRetention = new Retention(TimeValue.timeValueMillis(value));
            return this;
        }

        public Builder downsampling(@Nullable Downsampling value) {
            downsampling = value;
            return this;
        }

        public DataStreamLifecycle build() {
            return new DataStreamLifecycle(dataRetention, downsampling, enabled);
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

        public static final long FIVE_MINUTES_MILLIS = TimeValue.timeValueMinutes(5).getMillis();

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

            public Round {
                if (config.getFixedInterval().estimateMillis() < FIVE_MINUTES_MILLIS) {
                    throw new IllegalArgumentException(
                        "A downsampling round must have a fixed interval of at least five minutes but found: " + config.getFixedInterval()
                    );
                }
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
                if (rounds.size() > 10) {
                    throw new IllegalArgumentException(
                        "Downsampling configuration supports maximum 10 configured rounds. Found: " + rounds.size()
                    );
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
            return new Downsampling(in.readOptionalCollectionAsList(Round::read));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalCollection(rounds, StreamOutput::writeWriteable);
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

    /**
     * This enum represents all configuration sources that can influence the retention of a data stream.
     */
    public enum RetentionSource {
        DATA_STREAM_CONFIGURATION,
        DEFAULT_GLOBAL_RETENTION,
        MAX_GLOBAL_RETENTION;

        public String displayName() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }
}
