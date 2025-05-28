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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Holds the data stream lifecycle configuration that defines how the data stream indices of a data stream are managed. The lifecycle also
 * has a type that determines the type of index component it can manage. Currently, we support data and failures.
 * Lifecycle supports the following configurations:
 * - enabled, applicable to data and failures
 * - data retention, applicable to data and failures
 * - downsampling, applicable only to data
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
    private static final String DOWNSAMPLING_NOT_SUPPORTED_ERROR_MESSAGE =
        "Failure store lifecycle does not support downsampling, please remove the downsampling configuration.";

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

    public static final DataStreamLifecycle DEFAULT_DATA_LIFECYCLE = DataStreamLifecycle.createDataLifecycle(null, null, null);
    public static final DataStreamLifecycle DEFAULT_FAILURE_LIFECYCLE = DataStreamLifecycle.createFailuresLifecycle(null, null);

    public static final String DATA_STREAM_LIFECYCLE_ORIGIN = "data_stream_lifecycle";

    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField DATA_RETENTION_FIELD = new ParseField("data_retention");
    public static final ParseField EFFECTIVE_RETENTION_FIELD = new ParseField("effective_retention");
    public static final ParseField RETENTION_SOURCE_FIELD = new ParseField("retention_determined_by");
    public static final ParseField DOWNSAMPLING_FIELD = new ParseField("downsampling");
    private static final ParseField ROLLOVER_FIELD = new ParseField("rollover");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataStreamLifecycle, LifecycleType> PARSER = new ConstructingObjectParser<>(
        "lifecycle",
        false,
        (args, lt) -> new DataStreamLifecycle(lt, (Boolean) args[0], (TimeValue) args[1], (List<DownsamplingRound>) args[2])
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
    }

    private final LifecycleType lifecycleType;
    private final boolean enabled;
    @Nullable
    private final TimeValue dataRetention;
    @Nullable
    private final List<DownsamplingRound> downsampling;

    /**
     * This constructor is visible for testing, please use {@link DataStreamLifecycle#createDataLifecycle(Boolean, TimeValue, List)} or
     * {@link DataStreamLifecycle#createFailuresLifecycle(Boolean, TimeValue)}.
     */
    DataStreamLifecycle(
        LifecycleType lifecycleType,
        @Nullable Boolean enabled,
        @Nullable TimeValue dataRetention,
        @Nullable List<DownsamplingRound> downsampling
    ) {
        this.lifecycleType = lifecycleType;
        this.enabled = enabled == null || enabled;
        this.dataRetention = dataRetention;
        if (lifecycleType == LifecycleType.FAILURES && downsampling != null) {
            throw new IllegalArgumentException(DOWNSAMPLING_NOT_SUPPORTED_ERROR_MESSAGE);
        }
        DownsamplingRound.validateRounds(downsampling);
        this.downsampling = downsampling;
    }

    /**
     * This factory method creates a lifecycle applicable for the data index component of a data stream. This
     * means it supports all configuration applicable for backing indices.
     */
    public static DataStreamLifecycle createDataLifecycle(
        @Nullable Boolean enabled,
        @Nullable TimeValue dataRetention,
        @Nullable List<DownsamplingRound> downsampling
    ) {
        return new DataStreamLifecycle(LifecycleType.DATA, enabled, dataRetention, downsampling);
    }

    /**
     * This factory method creates a lifecycle applicable for the failures index component of a data stream. This
     * means it supports only enabling and retention.
     */
    public static DataStreamLifecycle createFailuresLifecycle(@Nullable Boolean enabled, @Nullable TimeValue dataRetention) {
        return new DataStreamLifecycle(LifecycleType.FAILURES, enabled, dataRetention, null);
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
        return lifecycleType == that.lifecycleType
            && Objects.equals(dataRetention, that.dataRetention)
            && Objects.equals(downsampling, that.downsampling)
            && enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lifecycleType, enabled, dataRetention, downsampling);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                out.writeOptionalTimeValue(dataRetention);
            } else {
                writeLegacyOptionalValue(dataRetention, out, StreamOutput::writeTimeValue);
            }

        }
        if (out.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
            if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                out.writeOptionalCollection(downsampling);
            } else {
                writeLegacyOptionalValue(downsampling, out, StreamOutput::writeCollection);
            }
            out.writeBoolean(enabled());
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)) {
            lifecycleType.writeTo(out);
        }
    }

    public DataStreamLifecycle(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            if (in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                dataRetention = in.readOptionalTimeValue();
            } else {
                dataRetention = readLegacyOptionalValue(in, StreamInput::readTimeValue);
            }
        } else {
            dataRetention = null;
        }
        if (in.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
            if (in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                downsampling = in.readOptionalCollectionAsList(DownsamplingRound::read);
            } else {
                downsampling = readLegacyOptionalValue(in, is -> is.readCollectionAsList(DownsamplingRound::read));
            }
            enabled = in.readBoolean();
        } else {
            downsampling = null;
            enabled = true;
        }
        lifecycleType = in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
            ? LifecycleType.read(in)
            : LifecycleType.DATA;
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
        return "DataStreamLifecycle{"
            + "lifecycleTarget="
            + lifecycleType
            + ", enabled="
            + enabled
            + ", dataRetention="
            + dataRetention
            + ", downsampling="
            + downsampling
            + '}';
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
            builder.field(DOWNSAMPLING_FIELD.getPreferredName(), downsampling);
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
     * This factory method creates a lifecycle template applicable for the data index component of a data stream. This
     * means it supports all configuration applicable for backing indices.
     */
    public static Template createDataLifecycleTemplate(
        boolean enabled,
        TimeValue dataRetention,
        List<DataStreamLifecycle.DownsamplingRound> downsampling
    ) {
        return new Template(LifecycleType.DATA, enabled, ResettableValue.create(dataRetention), ResettableValue.create(downsampling));
    }

    /**
     * This factory method creates a lifecycle template applicable for the data index component of a data stream. This
     * means it supports all configuration applicable for backing indices.
     */
    public static Template createDataLifecycleTemplate(
        boolean enabled,
        ResettableValue<TimeValue> dataRetention,
        ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> downsampling
    ) {
        return new Template(LifecycleType.DATA, enabled, dataRetention, downsampling);
    }

    /**
     * This factory method creates a lifecycle template applicable for the failures index component of a data stream. This
     * means it supports only setting the enabled and the retention.
     */
    public static Template createFailuresLifecycleTemplate(boolean enabled, TimeValue dataRetention) {
        return new Template(LifecycleType.FAILURES, enabled, ResettableValue.create(dataRetention), ResettableValue.undefined());
    }

    /**
     * Represents the template configuration of a lifecycle. It supports explicitly resettable values
     * to allow value reset during template composition.
     */
    public record Template(
        LifecycleType lifecycleType,
        boolean enabled,
        ResettableValue<TimeValue> dataRetention,
        ResettableValue<List<DataStreamLifecycle.DownsamplingRound>> downsampling
    ) implements ToXContentObject, Writeable {

        Template(
            LifecycleType lifecycleType,
            boolean enabled,
            TimeValue dataRetention,
            List<DataStreamLifecycle.DownsamplingRound> downsampling
        ) {
            this(lifecycleType, enabled, ResettableValue.create(dataRetention), ResettableValue.create(downsampling));
        }

        public Template {
            if (lifecycleType == LifecycleType.FAILURES && downsampling.get() != null) {
                throw new IllegalArgumentException(DOWNSAMPLING_NOT_SUPPORTED_ERROR_MESSAGE);
            }
            if (downsampling.isDefined() && downsampling.get() != null) {
                DownsamplingRound.validateRounds(downsampling.get());
            }
        }

        public static final DataStreamLifecycle.Template DATA_DEFAULT = new DataStreamLifecycle.Template(
            LifecycleType.DATA,
            true,
            ResettableValue.undefined(),
            ResettableValue.undefined()
        );

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DataStreamLifecycle.Template, LifecycleType> PARSER = new ConstructingObjectParser<>(
            "lifecycle_template",
            false,
            (args, lt) -> new DataStreamLifecycle.Template(
                lt,
                args[0] == null || (boolean) args[0],
                args[1] == null ? ResettableValue.undefined() : (ResettableValue<TimeValue>) args[1],
                args[2] == null ? ResettableValue.undefined() : (ResettableValue<List<DataStreamLifecycle.DownsamplingRound>>) args[2]
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
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // The order of the fields is like this for bwc reasons
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                    ResettableValue.write(out, dataRetention, StreamOutput::writeTimeValue);
                } else {
                    writeLegacyValue(out, dataRetention, StreamOutput::writeTimeValue);
                }
            }
            if (out.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
                if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                    ResettableValue.write(out, downsampling, StreamOutput::writeCollection);
                } else {
                    writeLegacyValue(out, downsampling, StreamOutput::writeCollection);
                }
                out.writeBoolean(enabled);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)) {
                lifecycleType.writeTo(out);
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
            ResettableValue<List<DownsamplingRound>> downsampling = ResettableValue.undefined();

            // The order of the fields is like this for bwc reasons
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                if (in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                    dataRetention = ResettableValue.read(in, StreamInput::readTimeValue);
                } else {
                    dataRetention = readLegacyValues(in, StreamInput::readTimeValue);
                }
            }
            if (in.getTransportVersion().onOrAfter(ADDED_ENABLED_FLAG_VERSION)) {
                if (in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_LIFECYCLE_TEMPLATE_8_19)) {
                    downsampling = ResettableValue.read(in, i -> i.readCollectionAsList(DownsamplingRound::read));
                } else {
                    downsampling = readLegacyValues(in, i -> i.readCollectionAsList(DownsamplingRound::read));
                }
                enabled = in.readBoolean();
            }
            var lifecycleTarget = in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
                ? LifecycleType.read(in)
                : LifecycleType.DATA;
            return new Template(lifecycleTarget, enabled, dataRetention, downsampling);
        }

        public static Template dataLifecycleTemplateFromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, LifecycleType.DATA);
        }

        public static Template failuresLifecycleTemplateFromXContent(XContentParser parser) throws IOException {
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

        public DataStreamLifecycle toDataStreamLifecycle() {
            return new DataStreamLifecycle(lifecycleType, enabled, dataRetention.get(), downsampling.get());
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
        @Nullable
        private TimeValue dataRetention = null;
        @Nullable
        private List<DownsamplingRound> downsampling = null;

        private Builder(LifecycleType lifecycleType) {
            this.lifecycleType = lifecycleType;
        }

        private Builder(DataStreamLifecycle.Template template) {
            lifecycleType = template.lifecycleType();
            enabled = template.enabled();
            dataRetention = template.dataRetention().get();
            downsampling = template.downsampling().get();
        }

        private Builder(DataStreamLifecycle lifecycle) {
            lifecycleType = lifecycle.lifecycleType;
            enabled = lifecycle.enabled();
            dataRetention = lifecycle.dataRetention();
            downsampling = lifecycle.downsampling();
        }

        public Builder composeTemplate(DataStreamLifecycle.Template template) {
            assert lifecycleType == template.lifecycleType() : "Trying to compose templates with different lifecycle types";
            enabled(template.enabled());
            dataRetention(template.dataRetention());
            downsampling(template.downsampling());
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder dataRetention(ResettableValue<TimeValue> dataRetention) {
            if (dataRetention.isDefined()) {
                this.dataRetention = dataRetention.get();
            }
            return this;
        }

        public Builder dataRetention(@Nullable TimeValue dataRetention) {
            this.dataRetention = dataRetention;
            return this;
        }

        public Builder downsampling(ResettableValue<List<DownsamplingRound>> downsampling) {
            if (downsampling.isDefined()) {
                this.downsampling = downsampling.get();
            }
            return this;
        }

        public Builder downsampling(@Nullable List<DownsamplingRound> downsampling) {
            this.downsampling = downsampling;
            return this;
        }

        public DataStreamLifecycle build() {
            return new DataStreamLifecycle(lifecycleType, enabled, dataRetention, downsampling);
        }

        public Template buildTemplate() {
            return new Template(lifecycleType, enabled, dataRetention, downsampling);
        }
    }

    /**
     * Defines the target index component managed by the lifecycle. Currently, it supports data and failures.
     * Visible for testing
     */
    enum LifecycleType implements Writeable {
        DATA("data", (byte) 0),
        FAILURES("failures", (byte) 1);

        private final String label;
        private final byte id;
        private static final Map<Byte, LifecycleType> REGISTRY = Arrays.stream(LifecycleType.values())
            .collect(Collectors.toMap(l -> l.id, Function.identity()));

        LifecycleType(String label, byte id) {
            this.label = label;
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.write(id);
        }

        public static LifecycleType read(StreamInput in) throws IOException {
            return REGISTRY.get(in.readByte());
        }
    }
}
