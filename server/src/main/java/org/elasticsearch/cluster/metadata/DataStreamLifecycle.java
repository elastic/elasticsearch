/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataStreamLifecycle, Void> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, unused) -> new DataStreamLifecycle((Boolean) args[0], (TimeValue) args[1], (List<DownsamplingRound>) args[2])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
        // For the retention and the downsampling, there was a bug that would allow an explicit null value to be
        // stored in the data stream when the lifecycle was not composed with another one. We need to be able to read
        // from a previous cluster state so we allow here explicit null values also to be parsed.
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            String value = p.textOrNull();
            if (value == null) {
                return null;
            } else {
                return TimeValue.parseTimeValue(value, DATA_RETENTION_FIELD.getPreferredName());
            }
        }, DATA_RETENTION_FIELD, ObjectParser.ValueType.STRING_OR_NULL);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            } else {
                return AbstractObjectParser.parseArray(p, c, DownsamplingRound::fromXContent);
            }
        }, DOWNSAMPLING_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL);
    }

    @Nullable
    private final Boolean enabled;
    @Nullable
    private final TimeValue dataRetention;
    @Nullable
    private final List<DownsamplingRound> downsampling;

    public DataStreamLifecycle() {
        this(null, null, null);
    }

    public DataStreamLifecycle(
        @Nullable Boolean enabled,
        @Nullable TimeValue dataRetention,
        @Nullable List<DownsamplingRound> downsampling
    ) {
        this.enabled = enabled;
        this.dataRetention = dataRetention;
        DownsamplingRound.validateRounds(downsampling);
        this.downsampling = downsampling;
    }

    /**
     * Returns true, if this data stream lifecycle configuration is enabled or null, false otherwise
     */
    public boolean isEffectivelyEnabled() {
        return enabled == null || enabled;
    }

    /**
     * Returns the exact value of the configuration
     */
    public Boolean enabled() {
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
        if (isEffectivelyEnabled()) {
            return INFINITE_RETENTION;
        }
        if (globalRetention == null || isInternalDataStream) {
            return Tuple.tuple(dataRetention, RetentionSource.DATA_STREAM_CONFIGURATION);
        }
        if (dataRetention == null) {
            return globalRetention.defaultRetention() != null
                ? Tuple.tuple(globalRetention.defaultRetention(), RetentionSource.DEFAULT_GLOBAL_RETENTION)
                : Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        }
        if (globalRetention.maxRetention() != null && globalRetention.maxRetention().getMillis() < dataRetention.getMillis()) {
            return Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        } else {
            return Tuple.tuple(dataRetention, RetentionSource.DATA_STREAM_CONFIGURATION);
        }
    }

    /**
     * The least amount of time data the data stream is requesting es to keep the data.
     * NOTE: this can be overridden by the {@link DataStreamLifecycle#getEffectiveDataRetention(DataStreamGlobalRetention,boolean)}.
     * @return the time period or null, null represents that data should never be deleted.
     */
    @Nullable
    public TimeValue dataRetention() {
        return dataRetention;
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
                String retentionProvidedPart = dataRetention() == null
                    ? "Not providing a retention is not allowed for this project."
                    : "The retention provided ["
                        + (dataRetention() == null ? "infinite" : dataRetention().getStringRep())
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
     * The configured downsampling rounds with the `after` and the `fixed_interval` per round. If downsampling is
     * not configured then it returns null.
     */
    @Nullable
    public List<DownsamplingRound> downsampling() {
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
        return Objects.hash(enabled, dataRetention, downsampling);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            // When the data retention is not null, for bwc reasons we need an extra flag
            if (dataRetention != null && out.getTransportVersion().before(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE)) {
                out.writeBoolean(true);
            }
            out.writeOptionalTimeValue(dataRetention);
        }
        if (out.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
            // When the data retention is not null, for bwc reasons we need an extra flag
            if (downsampling != null && out.getTransportVersion().before(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE)) {
                out.writeBoolean(true);
            }
            out.writeOptionalCollection(downsampling);
            if (out.getTransportVersion().before(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE)) {
                out.writeBoolean(isEffectivelyEnabled());
            } else {
                out.writeOptionalBoolean(enabled);
            }
        }
    }

    public DataStreamLifecycle(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            // When the data retention is not null, for bwc reasons we need an extra flag
            boolean isDefined = in.getTransportVersion().before(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE) ? in.readBoolean() : true;
            dataRetention = isDefined ? in.readOptionalTimeValue() : null;
        } else {
            dataRetention = null;
        }
        if (in.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
            boolean isDefined = in.getTransportVersion().before(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE) ? in.readBoolean() : true;
            downsampling = isDefined ? in.readOptionalCollectionAsList(DownsamplingRound::read) : null;
            if (in.getTransportVersion().before(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE)) {
                enabled = in.readBoolean();
            } else {
                enabled = in.readOptionalBoolean();
            }
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
            builder.field(DATA_RETENTION_FIELD.getPreferredName(), dataRetention.getStringRep());
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
            builder.array(DOWNSAMPLING_FIELD.getPreferredName(), downsampling);
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

    public static Builder builder(DataStreamLifecycle lifecycle) {
        return new Builder(lifecycle);
    }

    public static Builder builder() {
        return new Builder(null);
    }

    /**
     * This builder helps during the composition of the data stream lifecycle templates.
     */
    public static class Builder {
        @Nullable
        private Boolean enabled = null;
        @Nullable
        private TimeValue dataRetention = null;
        @Nullable
        private List<DownsamplingRound> downsampling = null;

        private Builder(@Nullable DataStreamLifecycle lifecycle) {
            if (lifecycle != null) {
                enabled = lifecycle.enabled;
                dataRetention = lifecycle.dataRetention;
                downsampling = lifecycle.downsampling;
            }
        }

        public Builder enabled(@Nullable Boolean value) {
            enabled = value;
            return this;
        }

        public Builder dataRetention(@Nullable TimeValue value) {
            dataRetention = value;
            return this;
        }

        public Builder dataRetention(long value) {
            dataRetention = TimeValue.timeValueMillis(value);
            return this;
        }

        public Builder downsampling(@Nullable List<DownsamplingRound> rounds) {
            downsampling = rounds;
            return this;
        }

        public DataStreamLifecycle build() {
            return new DataStreamLifecycle(enabled, dataRetention, downsampling);
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

    /**
     * A round represents the configuration for when and how elasticsearch will downsample a backing index.
     * @param after is a TimeValue configuring how old (based on generation age) should a backing index be before downsampling
     * @param config contains the interval that the backing index is going to be downsampled.
     */
    public record DownsamplingRound(TimeValue after, DownsampleConfig config) implements Writeable, ToXContentObject {

        public static final ParseField AFTER_FIELD = new ParseField("after");
        public static final ParseField FIXED_INTERVAL_FIELD = new ParseField("fixed_interval");
        public static final long FIVE_MINUTES_MILLIS = TimeValue.timeValueMinutes(5).getMillis();

        private static final ConstructingObjectParser<DownsamplingRound, Void> PARSER = new ConstructingObjectParser<>(
            "downsampling_round",
            false,
            (args, unused) -> new DownsamplingRound((TimeValue) args[0], new DownsampleConfig((DateHistogramInterval) args[1]))
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

        static void validateRounds(List<DownsamplingRound> rounds) {
            if (rounds == null) {
                return;
            }
            if (rounds.isEmpty()) {
                throw new IllegalArgumentException("Downsampling configuration should have at least one round configured.");
            }
            if (rounds.size() > 10) {
                throw new IllegalArgumentException(
                    "Downsampling configuration supports maximum 10 configured rounds. Found: " + rounds.size()
                );
            }
            DownsamplingRound previous = null;
            for (DownsamplingRound round : rounds) {
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

        public static DownsamplingRound read(StreamInput in) throws IOException {
            return new DownsamplingRound(in.readTimeValue(), new DownsampleConfig(in));
        }

        public DownsamplingRound {
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

        public static DownsamplingRound fromXContent(XContentParser parser, Void context) throws IOException {
            return PARSER.parse(parser, context);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

    /**
     * Represents the template configuration of a lifecycle. It supports explicitly resettable values
     * to allow value reset during template composition.
     */
    public record Template(
        ResettableValue<Boolean> enabled,
        ResettableValue<TimeValue> dataRetention,
        ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> downsampling
    ) implements ToXContentObject, Writeable {

        public Template {
            if (downsampling.isDefined() && downsampling.get() != null) {
                DownsamplingRound.validateRounds(downsampling.get());
            }
        }

        public static final DataStreamLifecycle.Template EMPTY = new DataStreamLifecycle.Template(
            ResettableValue.undefined(),
            ResettableValue.undefined(),
            ResettableValue.undefined()
        );

        // Current default lifecycle, EMPTY is effectively the
        // same without an explicitly setting enabled to true
        public static final DataStreamLifecycle.Template DEFAULT = new DataStreamLifecycle.Template(
            ResettableValue.create(true),
            ResettableValue.undefined(),
            ResettableValue.undefined()
        );

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DataStreamLifecycle.Template, Void> PARSER = new ConstructingObjectParser<>(
            "lifecycle_template",
            false,
            (args, unused) -> new DataStreamLifecycle.Template(
                args[0] == null ? ResettableValue.undefined() : (ResettableValue<Boolean>) args[0],
                args[1] == null ? ResettableValue.undefined() : (ResettableValue<TimeValue>) args[1],
                args[2] == null ? ResettableValue.undefined() : (ResettableValue<List<DataStreamLifecycle.DownsamplingRound>>) args[2]
            )
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL
                    ? ResettableValue.reset()
                    : ResettableValue.create(p.booleanValue()),
                ENABLED_FIELD,
                ObjectParser.ValueType.BOOLEAN_OR_NULL
            );
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                String value = p.textOrNull();
                return value == null
                    ? ResettableValue.reset()
                    : ResettableValue.create(TimeValue.parseTimeValue(value, DATA_RETENTION_FIELD.getPreferredName()));
            }, DATA_RETENTION_FIELD, ObjectParser.ValueType.STRING_OR_NULL);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                    return ResettableValue.reset();
                } else {
                    return ResettableValue.create(AbstractObjectParser.parseArray(p, c, DownsamplingRound::fromXContent));
                }
            }, DOWNSAMPLING_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // The order of the fields is like this for bwc reasons
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                ResettableValue.write(out, dataRetention, StreamOutput::writeTimeValue);
            }
            if (out.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
                ResettableValue.write(out, downsampling, StreamOutput::writeOptionalCollection);
                if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE)) {
                    ResettableValue.write(out, enabled, StreamOutput::writeBoolean);
                } else {
                    out.writeBoolean(enabled.get() == null || enabled.get());
                }
            }
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<Boolean> enabled = ResettableValue.undefined();
            ResettableValue<TimeValue> dataRetention = ResettableValue.undefined();
            ResettableValue<List<DownsamplingRound>> downsampling = ResettableValue.undefined();

            // The order of the fields is like this for bwc reasons
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                dataRetention = ResettableValue.read(in, StreamInput::readTimeValue);
            }
            if (in.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
                downsampling = ResettableValue.read(in, i -> i.readCollectionAsList(DownsamplingRound::read));
                if (in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE)) {
                    enabled = ResettableValue.read(in, StreamInput::readBoolean);
                } else {
                    enabled = ResettableValue.create(in.readBoolean());
                }
            }
            return new Template(enabled, dataRetention, downsampling);
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        /**
         * Converts the template to XContent, depending on the {@param params} set by {@link ResettableValue#hideResetValues(Params)}
         * it may or may not display any explicit nulls when the value is to be reset.
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return toXContent(builder, params, null, null, false);
        }

        /**
         * Converts the template to XContent, depending on the {@param params} set by {@link ResettableValue#hideResetValues(Params)}
         * it may or may not display any explicit nulls when the value is to be reset.
         */
        public XContentBuilder toXContent(
            XContentBuilder builder,
            Params params,
            @Nullable RolloverConfiguration rolloverConfiguration,
            @Nullable DataStreamGlobalRetention globalRetention,
            boolean isInternalDataStream
        ) throws IOException {
            builder.startObject();
            enabled.toXContent(builder, params, ENABLED_FIELD.getPreferredName());
            dataRetention.toXContent(builder, params, DATA_RETENTION_FIELD.getPreferredName(), TimeValue::getStringRep);
            downsampling.toXContent(builder, params, DOWNSAMPLING_FIELD.getPreferredName());
            if (rolloverConfiguration != null) {
                builder.field(ROLLOVER_FIELD.getPreferredName());
                rolloverConfiguration.evaluateAndConvertToXContent(
                    builder,
                    params,
                    toDataStreamLifecycle().getEffectiveDataRetention(globalRetention, isInternalDataStream)
                );
            }
            builder.endObject();
            return builder;
        }

        public static Builder builder(DataStreamLifecycle.Template template) {
            return new Builder(template);
        }

        public static Builder builder() {
            return new Builder(null);
        }

        public static class Builder {
            private ResettableValue<Boolean> enabled = ResettableValue.undefined();
            private ResettableValue<TimeValue> dataRetention = ResettableValue.undefined();
            private ResettableValue<List<DownsamplingRound>> downsampling = ResettableValue.undefined();

            private Builder(Template template) {
                if (template != null) {
                    enabled = template.enabled();
                    dataRetention = template.dataRetention();
                    downsampling = template.downsampling();
                }
            }

            public Builder enabled(ResettableValue<Boolean> enabled) {
                this.enabled = enabled;
                return this;
            }

            public Builder enabled(@Nullable Boolean enabled) {
                this.enabled = ResettableValue.create(enabled);
                return this;
            }

            public Builder dataRetention(ResettableValue<TimeValue> dataRetention) {
                this.dataRetention = dataRetention;
                return this;
            }

            public Builder dataRetention(@Nullable TimeValue dataRetention) {
                this.dataRetention = ResettableValue.create(dataRetention);
                return this;
            }

            public Builder downsampling(ResettableValue<List<DownsamplingRound>> downsampling) {
                this.downsampling = downsampling;
                return this;
            }

            public Builder downsampling(@Nullable List<DownsamplingRound> downsampling) {
                this.downsampling = ResettableValue.create(downsampling);
                return this;
            }

            public Template build() {
                return new Template(enabled, dataRetention, downsampling);
            }
        }

        public DataStreamLifecycle toDataStreamLifecycle() {
            return new DataStreamLifecycle(enabled.get(), dataRetention.get(), downsampling.get());
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }
}
