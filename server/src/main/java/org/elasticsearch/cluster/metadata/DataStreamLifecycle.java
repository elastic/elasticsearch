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
import org.elasticsearch.common.util.FeatureFlag;
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
 * Holds the data stream lifecycle configuration that defines how the data stream indices of a data stream are managed. The lifecycle also
 * has a type that determines the type of index component it can manage. Currently, we support data and failures.
 * Lifecycle supports the following configurations:
 * - enabled, applicable to data and failures
 * - data retention, applicable to data and failures
 * - downsampling and downsampling method, applicable only to data
 */
public class DataStreamLifecycle implements SimpleDiffable<DataStreamLifecycle>, ToXContentObject {

    public static final FeatureFlag DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG = new FeatureFlag("dlm_searchable_snapshots");

    // Versions over the wire
    private static final TransportVersion INTRODUCE_LIFECYCLE_TEMPLATE = TransportVersion.fromName("introduce_lifecycle_template");
    public static final TransportVersion ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM = TransportVersion.fromName("add_sample_method_downsample_dlm");
    public static final TransportVersion SEARCHABLE_SNAPSHOTS_DLM_TV = TransportVersion.fromName("searchable_snapshots_dlm");
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
    private static final String DOWNSAMPLING_NOT_SUPPORTED_ERROR_MESSAGE =
        "Failure store lifecycle does not support downsampling, please remove the downsampling configuration.";
    public static final String DOWNSAMPLING_METHOD_WITHOUT_ROUNDS_ERROR =
        "Downsampling method can only be set when there is at least one downsampling round.";

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

    public static final DataStreamLifecycle DEFAULT_DATA_LIFECYCLE = dataLifecycleBuilder().build();
    public static final DataStreamLifecycle DEFAULT_FAILURE_LIFECYCLE = failuresLifecycleBuilder().build();

    public static final String DATA_STREAM_LIFECYCLE_ORIGIN = "data_stream_lifecycle";

    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    public static final ParseField EFFECTIVE_RETENTION_FIELD = new ParseField("effective_retention");
    public static final ParseField RETENTION_SOURCE_FIELD = new ParseField("retention_determined_by");
    public static final ParseField DOWNSAMPLING_FIELD = new ParseField("downsampling");
    public static final ParseField DOWNSAMPLING_METHOD_FIELD = new ParseField("downsampling_method");
    public static final ParseField FROZEN_AFTER_FIELD = new ParseField("frozen_after");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataStreamLifecycle, LifecycleType> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, lt) -> new DataStreamLifecycle(
            lt,
            (Boolean) args[0],
            (TimeValue) args[1],
            (List<DownsamplingRound>) args[2],
            (DownsampleConfig.SamplingMethod) args[3],
            (TimeValue) args[4]
        )
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
                return AbstractObjectParser.parseArray(p, null, DownsamplingRound::fromXContent);
            }
        }, DOWNSAMPLING_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DownsampleConfig.SamplingMethod.fromString(p.text()),
            DOWNSAMPLING_METHOD_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            String value = p.textOrNull();
            if (value == null) {
                return null;
            } else {
                return TimeValue.parseTimeValue(value, FROZEN_AFTER_FIELD.getPreferredName());
            }
        }, FROZEN_AFTER_FIELD, ObjectParser.ValueType.STRING_OR_NULL);
    }

    private static final TransportVersion INTRODUCE_FAILURES_LIFECYCLE = TransportVersion.fromName("introduce_failures_lifecycle");

    private final LifecycleType lifecycleType;
    private final boolean enabled;
    @Nullable
    private final TimeValue dataRetention;
    @Nullable
    private final List<DownsamplingRound> downsamplingRounds;
    @Nullable
    private final DownsampleConfig.SamplingMethod downsamplingMethod;
    @Nullable
    private final TimeValue frozenAfter;

    /**
     * This constructor is visible for testing, please use {@link DataStreamLifecycle#dataLifecycleBuilder()} or
     * {@link DataStreamLifecycle#failuresLifecycleBuilder()}.
     */
    DataStreamLifecycle(
        LifecycleType lifecycleType,
        @Nullable Boolean enabled,
        @Nullable TimeValue dataRetention,
        @Nullable List<DownsamplingRound> downsamplingRounds,
        @Nullable DownsampleConfig.SamplingMethod downsamplingMethod,
        @Nullable TimeValue frozenAfter
    ) {
        this.lifecycleType = lifecycleType;
        this.enabled = enabled == null || enabled;
        this.dataRetention = dataRetention;
        if (lifecycleType == LifecycleType.FAILURES && downsamplingRounds != null) {
            throw new IllegalArgumentException(DOWNSAMPLING_NOT_SUPPORTED_ERROR_MESSAGE);
        }
        // Validate incorrectly because this may be constructed from state where an invalid configuration exists.
        DownsamplingRound.validateRoundsIncorrectly(downsamplingRounds);
        this.downsamplingRounds = downsamplingRounds;
        if (downsamplingMethod != null && downsamplingRounds == null) {
            throw new IllegalArgumentException(DOWNSAMPLING_METHOD_WITHOUT_ROUNDS_ERROR);
        }
        this.downsamplingMethod = downsamplingMethod;
        if (frozenAfter != null && frozenAfter.compareTo(TimeValue.ZERO) <= 0) {
            throw new IllegalArgumentException("frozen_after must be a positive time value");
        }
        this.frozenAfter = frozenAfter;
    }

    /**
     * Returns true, if this data stream lifecycle configuration is enabled, false otherwise
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return true if the lifecycle manages the failure store, false otherwise
     */
    public boolean targetsFailureStore() {
        return lifecycleType == LifecycleType.FAILURES;
    }

    public String getLifecycleType() {
        return lifecycleType.label;
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
        if (enabled() == false) {
            return INFINITE_RETENTION;
        }
        if (globalRetention == null || isInternalDataStream) {
            return Tuple.tuple(dataRetention(), RetentionSource.DATA_STREAM_CONFIGURATION);
        }
        if (dataRetention() == null) {
            return globalRetention.defaultRetention() != null
                ? Tuple.tuple(
                    globalRetention.defaultRetention(),
                    targetsFailureStore() ? RetentionSource.DEFAULT_FAILURES_RETENTION : RetentionSource.DEFAULT_GLOBAL_RETENTION
                )
                : Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        }
        if (globalRetention.maxRetention() != null && globalRetention.maxRetention().getMillis() < dataRetention().getMillis()) {
            return Tuple.tuple(globalRetention.maxRetention(), RetentionSource.MAX_GLOBAL_RETENTION);
        } else {
            return Tuple.tuple(dataRetention(), RetentionSource.DATA_STREAM_CONFIGURATION);
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
     * not configured, then it returns null.
     */
    @Nullable
    public List<DownsamplingRound> downsamplingRounds() {
        return downsamplingRounds;
    }

    /**
     * The configured downsampling method. If downsampling is not configured, then it returns null.
     */
    @Nullable
    public DownsampleConfig.SamplingMethod downsamplingMethod() {
        return downsamplingMethod;
    }

    @Nullable
    public TimeValue frozenAfter() {
        return frozenAfter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DataStreamLifecycle that = (DataStreamLifecycle) o;
        return lifecycleType == that.lifecycleType
            && Objects.equals(dataRetention, that.dataRetention)
            && Objects.equals(downsamplingRounds, that.downsamplingRounds)
            && Objects.equals(downsamplingMethod, that.downsamplingMethod)
            && Objects.equals(frozenAfter, that.frozenAfter)
            && enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lifecycleType, enabled, dataRetention, downsamplingRounds, downsamplingMethod, frozenAfter);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
            out.writeOptionalTimeValue(dataRetention);
        } else {
            writeLegacyOptionalValue(dataRetention, out, StreamOutput::writeTimeValue);
        }
        if (out.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
            out.writeOptionalCollection(downsamplingRounds);
        } else {
            writeLegacyOptionalValue(downsamplingRounds, out, StreamOutput::writeCollection);
        }
        out.writeBoolean(enabled());
        if (out.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE)) {
            lifecycleType.writeTo(out);
        }
        if (out.getTransportVersion().supports(ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM)) {
            out.writeOptionalWriteable(downsamplingMethod);
        }
        if (DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled() && out.getTransportVersion().supports(SEARCHABLE_SNAPSHOTS_DLM_TV)) {
            out.writeOptionalTimeValue(frozenAfter);
        }
    }

    public DataStreamLifecycle(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
            dataRetention = in.readOptionalTimeValue();
        } else {
            dataRetention = readLegacyOptionalValue(in, StreamInput::readTimeValue);
        }
        if (in.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
            downsamplingRounds = in.readOptionalCollectionAsList(DownsamplingRound::read);
        } else {
            downsamplingRounds = readLegacyOptionalValue(in, is -> is.readCollectionAsList(DownsamplingRound::read));
        }
        enabled = in.readBoolean();
        lifecycleType = in.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE) ? LifecycleType.read(in) : LifecycleType.DATA;
        downsamplingMethod = in.getTransportVersion().supports(ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM)
            ? in.readOptionalWriteable(DownsampleConfig.SamplingMethod::read)
            : null;
        frozenAfter = DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled() && in.getTransportVersion().supports(SEARCHABLE_SNAPSHOTS_DLM_TV)
            ? in.readOptionalTimeValue()
            : null;
    }

    /**
     * Previous versions were serialising <code>value</code> in way that also captures an explicit null. Meaning, first they serialise
     * a boolean flag signaling if this value is defined, and then another boolean flag if the value is non-null. We do not need explicit
     * null values anymore, so we treat them the same as non defined, but for bwc reasons we still need to write all the flags.
     * @param value value to be serialised that used to be explicitly nullable.
     * @param writer the writer of the value, it should NOT be an optional writer
     * @throws IOException
     */
    private static <T> void writeLegacyOptionalValue(T value, StreamOutput out, Writer<T> writer) throws IOException {
        boolean isDefined = value != null;
        out.writeBoolean(isDefined);
        if (isDefined) {
            // There are no explicit null values anymore, so it's always true
            out.writeBoolean(true);
            writer.write(out, value);
        }
    }

    /**
     * Previous versions were de-serialising <code>value</code> in way that also captures an explicit null. Meaning, first they de-serialise
     * a boolean flag signaling if this value is defined, and then another boolean flag if the value is non-null. We do not need explicit
     * null values anymore, so we treat them the same as non defined, but for bwc reasons we still need to read all the flags.
     * @param reader the reader of the value, it should NOT be an optional reader
     * @throws IOException
     */
    private static <T> T readLegacyOptionalValue(StreamInput in, Reader<T> reader) throws IOException {
        T value = null;
        boolean isDefined = in.readBoolean();
        if (isDefined) {
            boolean isNotNull = in.readBoolean();
            if (isNotNull) {
                value = reader.read(in);
            }
        }
        return value;
    }

    public static Diff<DataStreamLifecycle> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamLifecycle::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
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

        if (downsamplingRounds != null) {
            builder.field(DOWNSAMPLING_FIELD.getPreferredName(), downsamplingRounds);
        }
        if (downsamplingMethod != null) {
            builder.field(DOWNSAMPLING_METHOD_FIELD.getPreferredName(), downsamplingMethod.toString());
        }
        if (DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled() && frozenAfter != null) {
            builder.field(FROZEN_AFTER_FIELD.getPreferredName(), frozenAfter.getStringRep());
        }
        if (rolloverConfiguration != null) {
            builder.field(ROLLOVER_FIELD.getPreferredName());
            rolloverConfiguration.evaluateAndConvertToXContent(builder, params, effectiveDataRetentionWithSource.v1());
        }
        builder.endObject();
        return builder;
    }

    /**
     * This method de-serialises a data lifecycle as it was generated ONLY by
     * {@link DataStreamLifecycle#toXContent(XContentBuilder, Params)}. It does not support the output of
     * {@link DataStreamLifecycle#toXContent(XContentBuilder, Params, RolloverConfiguration, DataStreamGlobalRetention, boolean)} because
     * this output is enriched with derived fields we do not handle in this de-serialisation.
     */
    public static DataStreamLifecycle dataLifecycleFromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, LifecycleType.DATA);
    }

    /**
     * This method de-serialises a failures lifecycle as it was generated ONLY by
     * {@link DataStreamLifecycle#toXContent(XContentBuilder, Params)}. It does not support the output of
     * {@link DataStreamLifecycle#toXContent(XContentBuilder, Params, RolloverConfiguration, DataStreamGlobalRetention, boolean)} because
     * this output is enriched with derived fields we do not handle in this de-serialisation.
     */
    public static DataStreamLifecycle failureLifecycleFromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, LifecycleType.FAILURES);
    }

    /**
     * Adds a retention param to signal that this serialisation should include the effective retention metadata.
     * @param params the XContent params to be extended with the new flag
     * @return XContent params with `include_effective_retention` set to true. If the flag exists it will override it.
     */
    public static ToXContent.Params addEffectiveRetentionParams(ToXContent.Params params) {
        return new DelegatingMapParams(INCLUDE_EFFECTIVE_RETENTION_PARAMS, params);
    }

    /**
     * This enum represents all configuration sources that can influence the retention of a data stream.
     */
    public enum RetentionSource {
        DATA_STREAM_CONFIGURATION,
        DEFAULT_GLOBAL_RETENTION,
        MAX_GLOBAL_RETENTION,
        DEFAULT_FAILURES_RETENTION;

        public String displayName() {
            return this.toString().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * A round represents the configuration for when and how elasticsearch will downsample a backing index.
     * @param after is a TimeValue configuring how old (based on generation age) should a backing index be before downsampling
     * @param fixedInterval contains the interval that the backing index is going to be downsampled.
     */
    public record DownsamplingRound(TimeValue after, DateHistogramInterval fixedInterval) implements Writeable, ToXContentObject {

        public static final ParseField AFTER_FIELD = new ParseField("after");
        public static final ParseField FIXED_INTERVAL_FIELD = new ParseField("fixed_interval");
        public static final long FIVE_MINUTES_MILLIS = TimeValue.timeValueMinutes(5).getMillis();

        private static final ConstructingObjectParser<DownsamplingRound, Void> PARSER = new ConstructingObjectParser<>(
            "downsampling_round",
            false,
            (args, unused) -> new DownsamplingRound((TimeValue) args[0], (DateHistogramInterval) args[1])
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
                FIXED_INTERVAL_FIELD,
                ObjectParser.ValueType.STRING
            );
        }

        /**
         * Validates the downsampling rounds, but incorrectly. By "incorrectly" we mean that it
         * only checks that the rounds are multiples of the _first_ downsampling round's interval,
         * instead of being a multiple of the _previous_ downsampling round's interval. However,
         * there may be instances of an invalid configuration already stored on disk in cluster
         * state in the template or data stream metadata. This method remains as the "old" version
         * of the validation. Use {@link #validateRounds(List)} to validate with the correct
         * behavior.
         */
        @Deprecated(since = "8.19.12,9.3.1,9.4.0")
        public static void validateRoundsIncorrectly(List<DownsamplingRound> rounds) {
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
                    DownsampleConfig.validateSourceAndTargetIntervals(previous.fixedInterval(), round.fixedInterval());
                }
            }
        }

        /**
         * Validates that the downsampling rounds are non-empty, there are fewer than 10 present,
         * and that each round's `fixed_interval` is a multiple of the previous round's interval.
         */
        public static void validateRounds(List<DownsamplingRound> rounds) {
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
                if (previous != null) {
                    if (round.after.compareTo(previous.after) < 0) {
                        throw new IllegalArgumentException(
                            "A downsampling round must have a later 'after' value than the proceeding, "
                                + round.after.getStringRep()
                                + " is not after "
                                + previous.after.getStringRep()
                                + "."
                        );
                    }
                    DownsampleConfig.validateSourceAndTargetIntervals(previous.fixedInterval(), round.fixedInterval());
                }
                previous = round;
            }
        }

        public static DownsamplingRound read(StreamInput in) throws IOException {
            TimeValue after = in.readTimeValue();
            DateHistogramInterval fixedInterval = in.getTransportVersion().supports(ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM)
                ? new DateHistogramInterval(in)
                : new DownsampleConfig(in).getFixedInterval();
            return new DownsamplingRound(after, fixedInterval);
        }

        public DownsamplingRound {
            if (fixedInterval.estimateMillis() < FIVE_MINUTES_MILLIS) {
                throw new IllegalArgumentException(
                    "A downsampling round must have a fixed interval of at least five minutes but found: " + fixedInterval
                );
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeTimeValue(after);
            if (out.getTransportVersion().supports(ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM)) {
                out.writeWriteable(fixedInterval);
            } else {
                out.writeWriteable(new DownsampleConfig(fixedInterval, null));
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(AFTER_FIELD.getPreferredName(), after.getStringRep());
            builder.field(FIXED_INTERVAL_FIELD.getPreferredName(), fixedInterval().toString());
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
        LifecycleType lifecycleType,
        boolean enabled,
        ResettableValue<TimeValue> dataRetention,
        ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> downsamplingRounds,
        ResettableValue<DownsampleConfig.SamplingMethod> downsamplingMethod,
        ResettableValue<TimeValue> frozenAfter
    ) implements ToXContentObject, Writeable {

        public Template {
            if (lifecycleType == LifecycleType.FAILURES && downsamplingRounds.get() != null) {
                throw new IllegalArgumentException(DOWNSAMPLING_NOT_SUPPORTED_ERROR_MESSAGE);
            }
            if (downsamplingRounds.isDefined() && downsamplingRounds.get() != null) {
                // Validate incorrectly because the Template object may be constructed by
                // state on disk which cannot be validated correctly without breaking.
                DownsamplingRound.validateRoundsIncorrectly(downsamplingRounds.get());
            } else if (downsamplingMethod.isDefined() && downsamplingMethod.get() != null) {
                throw new IllegalArgumentException(DOWNSAMPLING_METHOD_WITHOUT_ROUNDS_ERROR);
            }
        }

        public static final DataStreamLifecycle.Template DATA_DEFAULT = dataLifecycleBuilder().enabled(true).buildTemplate();

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DataStreamLifecycle.Template, LifecycleType> PARSER = new ConstructingObjectParser<>(
            "lifecycle_template",
            false,
            (args, lt) -> new DataStreamLifecycle.Template(
                lt,
                args[0] == null || (boolean) args[0],
                args[1] == null ? ResettableValue.undefined() : (ResettableValue<TimeValue>) args[1],
                args[2] == null ? ResettableValue.undefined() : (ResettableValue<List<DataStreamLifecycle.DownsamplingRound>>) args[2],
                args[3] == null ? ResettableValue.undefined() : (ResettableValue<DownsampleConfig.SamplingMethod>) args[3],
                args[4] == null ? ResettableValue.undefined() : (ResettableValue<TimeValue>) args[4]
            )
        );

        static {
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
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
                    return ResettableValue.create(AbstractObjectParser.parseArray(p, null, DownsamplingRound::fromXContent));
                }
            }, DOWNSAMPLING_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_OR_NULL);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                String value = p.textOrNull();
                return value == null ? ResettableValue.reset() : ResettableValue.create(DownsampleConfig.SamplingMethod.fromString(value));
            }, DOWNSAMPLING_METHOD_FIELD, ObjectParser.ValueType.STRING_OR_NULL);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
                String value = p.textOrNull();
                return value == null
                    ? ResettableValue.reset()
                    : ResettableValue.create(TimeValue.parseTimeValue(value, FROZEN_AFTER_FIELD.getPreferredName()));
            }, FROZEN_AFTER_FIELD, ObjectParser.ValueType.STRING_OR_NULL);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // The order of the fields is like this for bwc reasons
            if (out.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
                ResettableValue.write(out, dataRetention, StreamOutput::writeTimeValue);
            } else {
                writeLegacyValue(out, dataRetention, StreamOutput::writeTimeValue);
            }
            if (out.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
                ResettableValue.write(out, downsamplingRounds, StreamOutput::writeCollection);
            } else {
                writeLegacyValue(out, downsamplingRounds, StreamOutput::writeCollection);
            }
            out.writeBoolean(enabled);
            if (out.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE)) {
                lifecycleType.writeTo(out);
            }
            if (out.getTransportVersion().supports(ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM)) {
                ResettableValue.write(out, downsamplingMethod, StreamOutput::writeWriteable);
            }
            if (DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled() && out.getTransportVersion().supports(SEARCHABLE_SNAPSHOTS_DLM_TV)) {
                ResettableValue.write(out, frozenAfter, StreamOutput::writeTimeValue);
            }
        }

        /**
         * Before the introduction of the ResettableValues we used to serialise the explicit nulls differently. Legacy codes defines the
         * two boolean flags as "isDefined" and "hasValue" while the ResettableValue, "isDefined" and "shouldReset". This inverts the
         * semantics of the second flag and that's why we use this method.
         */
        private static <T> void writeLegacyValue(StreamOutput out, ResettableValue<T> value, Writeable.Writer<T> writer)
            throws IOException {
            out.writeBoolean(value.isDefined());
            if (value.isDefined()) {
                out.writeBoolean(value.shouldReset() == false);
                if (value.shouldReset() == false) {
                    writer.write(out, value.get());
                }
            }
        }

        /**
         * Before the introduction of the ResettableValues we used to serialise the explicit nulls differently. Legacy codes defines the
         * two boolean flags as "isDefined" and "hasValue" while the ResettableValue, "isDefined" and "shouldReset". This inverts the
         * semantics of the second flag and that's why we use this method.
         */
        static <T> ResettableValue<T> readLegacyValues(StreamInput in, Writeable.Reader<T> reader) throws IOException {
            boolean isDefined = in.readBoolean();
            if (isDefined == false) {
                return ResettableValue.undefined();
            }
            boolean hasNonNullValue = in.readBoolean();
            if (hasNonNullValue == false) {
                return ResettableValue.reset();
            }
            T value = reader.read(in);
            return ResettableValue.create(value);
        }

        public static Template read(StreamInput in) throws IOException {
            boolean enabled = true;
            ResettableValue<TimeValue> dataRetention = ResettableValue.undefined();
            ResettableValue<List<DownsamplingRound>> downsamplingRounds = ResettableValue.undefined();

            // The order of the fields is like this for bwc reasons
            if (in.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
                dataRetention = ResettableValue.read(in, StreamInput::readTimeValue);
            } else {
                dataRetention = readLegacyValues(in, StreamInput::readTimeValue);
            }
            if (in.getTransportVersion().supports(INTRODUCE_LIFECYCLE_TEMPLATE)) {
                downsamplingRounds = ResettableValue.read(in, i -> i.readCollectionAsList(DownsamplingRound::read));
            } else {
                downsamplingRounds = readLegacyValues(in, i -> i.readCollectionAsList(DownsamplingRound::read));
            }
            enabled = in.readBoolean();
            var lifecycleTarget = in.getTransportVersion().supports(INTRODUCE_FAILURES_LIFECYCLE)
                ? LifecycleType.read(in)
                : LifecycleType.DATA;
            ResettableValue<DownsampleConfig.SamplingMethod> downsamplingMethod = in.getTransportVersion()
                .supports(ADD_SAMPLE_METHOD_DOWNSAMPLE_DLM)
                    ? ResettableValue.read(in, DownsampleConfig.SamplingMethod::read)
                    : ResettableValue.undefined();
            ResettableValue<TimeValue> frozenAfter = DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled()
                && in.getTransportVersion().supports(SEARCHABLE_SNAPSHOTS_DLM_TV)
                    ? ResettableValue.read(in, StreamInput::readTimeValue)
                    : ResettableValue.undefined();
            return new Template(lifecycleTarget, enabled, dataRetention, downsamplingRounds, downsamplingMethod, frozenAfter);
        }

        public static Template dataLifecycleTemplatefromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, LifecycleType.DATA);
        }

        public static Template failuresLifecycleTemplatefromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, LifecycleType.FAILURES);
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
            builder.field(ENABLED_FIELD.getPreferredName(), enabled);
            dataRetention.toXContent(builder, params, DATA_RETENTION_FIELD.getPreferredName(), TimeValue::getStringRep);
            downsamplingRounds.toXContent(builder, params, DOWNSAMPLING_FIELD.getPreferredName());
            downsamplingMethod.toXContent(
                builder,
                params,
                DOWNSAMPLING_METHOD_FIELD.getPreferredName(),
                DownsampleConfig.SamplingMethod::toString
            );
            if (DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled()) {
                frozenAfter.toXContent(builder, params, FROZEN_AFTER_FIELD.getPreferredName(), TimeValue::getStringRep);
            }
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

        public DataStreamLifecycle toDataStreamLifecycle() {
            return new DataStreamLifecycle(
                lifecycleType,
                enabled,
                dataRetention.get(),
                downsamplingRounds.get(),
                downsamplingMethod.get(),
                frozenAfter.get()
            );
        }
    }

    public static Builder builder(DataStreamLifecycle lifecycle) {
        return new Builder(lifecycle);
    }

    public static Builder builder(Template template) {
        return new Builder(template);
    }

    /**
     * This builder factory initialises a builder of a data lifecycle, meaning when it builds we will either get a data lifecycle or a
     * data lifecycle template.
     */
    public static Builder dataLifecycleBuilder() {
        return new Builder(LifecycleType.DATA);
    }

    /**
     * This builder factory initialises a builder of a failures lifecycle, meaning when it builds we will either get a failures lifecycle or
     * a failures lifecycle template, if downsampling is not null the final "building" will throw an exception.
     */
    public static Builder failuresLifecycleBuilder() {
        return new Builder(LifecycleType.FAILURES);
    }

    /**
     * Builds and composes the data stream lifecycle or the respective template.
     */
    public static class Builder {
        private final LifecycleType lifecycleType;
        private boolean enabled = true;
        private ResettableValue<TimeValue> dataRetention = ResettableValue.undefined();
        private ResettableValue<List<DownsamplingRound>> downsamplingRounds = ResettableValue.undefined();
        private ResettableValue<DownsampleConfig.SamplingMethod> downsamplingMethod = ResettableValue.undefined();
        private ResettableValue<TimeValue> frozenAfter = ResettableValue.undefined();

        private Builder(LifecycleType lifecycleType) {
            this.lifecycleType = lifecycleType;
        }

        private Builder(DataStreamLifecycle.Template template) {
            lifecycleType = template.lifecycleType();
            enabled = template.enabled();
            dataRetention = template.dataRetention();
            downsamplingRounds = template.downsamplingRounds();
            downsamplingMethod = template.downsamplingMethod();
            frozenAfter = template.frozenAfter();
        }

        private Builder(DataStreamLifecycle lifecycle) {
            lifecycleType = lifecycle.lifecycleType;
            enabled = lifecycle.enabled();
            dataRetention = ResettableValue.create(lifecycle.dataRetention());
            downsamplingRounds = ResettableValue.create(lifecycle.downsamplingRounds());
            downsamplingMethod = ResettableValue.create(lifecycle.downsamplingMethod());
            frozenAfter = ResettableValue.create(lifecycle.frozenAfter());
        }

        public Builder composeTemplate(DataStreamLifecycle.Template template) {
            assert lifecycleType == template.lifecycleType() : "Trying to compose templates with different lifecycle types";
            enabled(template.enabled());
            dataRetention(template.dataRetention());
            downsamplingRounds(template.downsamplingRounds());
            downsamplingMethod(template.downsamplingMethod());
            frozenAfter(template.frozenAfter());
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder dataRetention(ResettableValue<TimeValue> dataRetention) {
            if (dataRetention.isDefined()) {
                this.dataRetention = dataRetention;
            }
            return this;
        }

        public Builder dataRetention(@Nullable TimeValue dataRetention) {
            this.dataRetention = ResettableValue.create(dataRetention);
            return this;
        }

        public Builder downsamplingRounds(ResettableValue<List<DownsamplingRound>> downsampling) {
            if (downsampling.isDefined()) {
                this.downsamplingRounds = downsampling;
            }
            return this;
        }

        public Builder downsamplingRounds(@Nullable List<DownsamplingRound> downsampling) {
            this.downsamplingRounds = ResettableValue.create(downsampling);
            return this;
        }

        public Builder downsamplingMethod(ResettableValue<DownsampleConfig.SamplingMethod> downsamplingMethod) {
            if (downsamplingMethod.isDefined()) {
                this.downsamplingMethod = downsamplingMethod;
            }
            return this;
        }

        public Builder downsamplingMethod(@Nullable DownsampleConfig.SamplingMethod downsamplingMethod) {
            this.downsamplingMethod = ResettableValue.create(downsamplingMethod);
            return this;
        }

        public Builder frozenAfter(ResettableValue<TimeValue> frozenAfter) {
            if (frozenAfter.isDefined()) {
                this.frozenAfter = frozenAfter;
            }
            return this;
        }

        public Builder frozenAfter(@Nullable TimeValue frozenAfter) {
            this.frozenAfter = ResettableValue.create(frozenAfter);
            return this;
        }

        public DataStreamLifecycle build() {
            return new DataStreamLifecycle(
                lifecycleType,
                enabled,
                dataRetention.get(),
                downsamplingRounds.get(),
                downsamplingMethod.get(),
                frozenAfter.get()
            );
        }

        public Template buildTemplate() {
            return new Template(lifecycleType, enabled, dataRetention, downsamplingRounds, downsamplingMethod, frozenAfter);
        }
    }

    /**
     * Defines the target index component managed by the lifecycle. Currently, it supports data and failures.
     * Visible for testing
     */
    enum LifecycleType implements Writeable {
        DATA("data"),
        FAILURES("failures");

        private final String label;

        LifecycleType(String label) {
            this.label = label;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static LifecycleType read(StreamInput in) throws IOException {
            return in.readEnum(LifecycleType.class);
        }
    }
}
