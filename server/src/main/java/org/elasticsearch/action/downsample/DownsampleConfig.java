/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.downsample;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a DownsampleAction that downsamples time series
 * (TSDB) indices. We have made great effort to simplify the rollup configuration and currently
 * only requires a fixed time interval and optionally the sampling method. So, it has the following format:
 *
 *  {
 *    "fixed_interval": "1d",
 *    "sampling_method": "aggregate"
 *  }
 *
 * fixed_interval is one or multiples of SI units and has no calendar-awareness (e.g. doesn't account
 * for leap corrections, does not have variable length months, etc). Calendar-aware interval is not currently
 * supported.
 *
 * Also, the rollup configuration uses the UTC time zone by default and the "@timestamp" field as
 * the index field that stores the timestamp of the time series index.
 *
 * Finally, we have left methods such as {@link DownsampleConfig#getTimestampField()},
 * {@link DownsampleConfig#getTimeZone()} and  {@link DownsampleConfig#getIntervalType()} for
 * future extensions.
 */
public class DownsampleConfig implements NamedWriteable, ToXContentObject {
    public static final TransportVersion ADD_LAST_VALUE_DOWNSAMPLE_API = TransportVersion.fromName("add_last_value_downsample_api");

    private static final String NAME = "downsample/action/config";
    public static final String FIXED_INTERVAL = "fixed_interval";
    public static final String SAMPLING_METHOD = "sampling_method";
    public static final String TIME_ZONE = "time_zone";
    public static final String DEFAULT_TIMEZONE = ZoneId.of("UTC").getId();

    private static final String timestampField = DataStreamTimestampFieldMapper.DEFAULT_PATH;
    private final DateHistogramInterval fixedInterval;
    private final String timeZone = DEFAULT_TIMEZONE;
    private final String intervalType = FIXED_INTERVAL;
    @Nullable
    private final SamplingMethod samplingMethod;

    private static final ConstructingObjectParser<DownsampleConfig, Void> PARSER;

    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> {
            DateHistogramInterval fixedInterval = (DateHistogramInterval) a[0];
            if (fixedInterval != null) {
                return new DownsampleConfig(fixedInterval, (SamplingMethod) a[1]);
            } else {
                throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL + "] is required.");
            }
        });

        PARSER.declareField(
            constructorArg(),
            p -> new DateHistogramInterval(p.text()),
            new ParseField(FIXED_INTERVAL),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            p -> SamplingMethod.fromString(p.text()),
            new ParseField(SAMPLING_METHOD),
            ObjectParser.ValueType.STRING
        );
    }

    /**
     * Create a new {@link DownsampleConfig} using the given configuration parameters.
     * @param fixedInterval the fixed interval to use for computing the date histogram for the rolled up documents (required).
     * @param samplingMethod the method used to downsample metrics, when null it default to {@link SamplingMethod#AGGREGATE}.
     */
    public DownsampleConfig(final DateHistogramInterval fixedInterval, @Nullable SamplingMethod samplingMethod) {
        if (fixedInterval == null) {
            throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL + "] is required.");
        }
        this.fixedInterval = fixedInterval;
        this.samplingMethod = samplingMethod;

        // validate interval
        createRounding(this.fixedInterval.toString(), this.timeZone);
    }

    public DownsampleConfig(final StreamInput in) throws IOException {
        fixedInterval = new DateHistogramInterval(in);
        if (in.getTransportVersion().supports(ADD_LAST_VALUE_DOWNSAMPLE_API)) {
            samplingMethod = in.readOptionalWriteable(SamplingMethod::read);
        } else {
            samplingMethod = null;
        }
    }

    /**
     * This method validates the target downsampling configuration can be applied on an index that has been
     * already downsampled from the source configuration. The requirements are:
     * - The target interval needs to be greater than source interval
     * - The target interval needs to be a multiple of the source interval
     * throws an IllegalArgumentException to signal that the target interval is not acceptable
     */
    public static void validateSourceAndTargetIntervals(
        DateHistogramInterval sourceFxedInterval,
        DateHistogramInterval targetFixedInterval
    ) {
        long sourceMillis = sourceFxedInterval.estimateMillis();
        long targetMillis = targetFixedInterval.estimateMillis();
        if (sourceMillis >= targetMillis) {
            // Downsampling interval must be greater than source interval
            throw new IllegalArgumentException(
                "Downsampling interval ["
                    + targetFixedInterval
                    + "] must be greater than the source index interval ["
                    + sourceFxedInterval
                    + "]."
            );
        } else if (targetMillis % sourceMillis != 0) {
            // Downsampling interval must be a multiple of the source interval
            throw new IllegalArgumentException(
                "Downsampling interval ["
                    + targetFixedInterval
                    + "] must be a multiple of the source index interval ["
                    + sourceFxedInterval
                    + "]."
            );
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        fixedInterval.writeTo(out);
        if (out.getTransportVersion().supports(ADD_LAST_VALUE_DOWNSAMPLE_API)) {
            out.writeOptionalWriteable(samplingMethod);
        }
    }

    /**
     * Get the timestamp field to be used for rolling up data. Currently,
     * only the "@timestamp" value is supported.
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * Get the interval type. Currently, only fixed_interval is supported
     */
    public String getIntervalType() {
        return intervalType;
    }

    /**
     * Get the interval value
     */
    public DateHistogramInterval getInterval() {
        return getFixedInterval();
    }

    /**
     * Get the fixed_interval value
     */
    public DateHistogramInterval getFixedInterval() {
        return fixedInterval;
    }

    /**
     * Get the timezone to apply
     */
    public String getTimeZone() {
        return timeZone;
    }

    /**
     * Create the rounding for this date histogram
     */
    public Rounding.Prepared createRounding() {
        return createRounding(fixedInterval.toString(), timeZone);
    }

    /**
     * @return the user configured sampling method
     */
    @Nullable
    public SamplingMethod getSamplingMethod() {
        return samplingMethod;
    }

    /**
     * @return the sampling method that will be used based on this configuration.
     */
    public SamplingMethod getSamplingMethodOrDefault() {
        return SamplingMethod.getOrDefault(samplingMethod);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            toXContentFragment(builder);
        }
        return builder.endObject();
    }

    public XContentBuilder toXContentFragment(final XContentBuilder builder) throws IOException {
        builder.field(FIXED_INTERVAL, fixedInterval.toString());
        if (samplingMethod != null) {
            builder.field(SAMPLING_METHOD, samplingMethod.label);
        }
        return builder;
    }

    public static DownsampleConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other instanceof DownsampleConfig == false) {
            return false;
        }
        final DownsampleConfig that = (DownsampleConfig) other;
        return Objects.equals(fixedInterval, that.fixedInterval)
            && Objects.equals(intervalType, that.intervalType)
            && ZoneId.of(timeZone, ZoneId.SHORT_IDS).getRules().equals(ZoneId.of(that.timeZone, ZoneId.SHORT_IDS).getRules())
            && Objects.equals(samplingMethod, that.samplingMethod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedInterval, intervalType, ZoneId.of(timeZone), samplingMethod);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Rounding.Prepared createRounding(final String expr, final String timeZone) {
        Rounding.DateTimeUnit timeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expr);
        final Rounding.Builder rounding;
        if (timeUnit != null) {
            rounding = new Rounding.Builder(timeUnit);
        } else {
            rounding = new Rounding.Builder(TimeValue.parseTimeValue(expr, "createRounding"));
        }
        rounding.timeZone(ZoneId.of(timeZone, ZoneId.SHORT_IDS));
        return rounding.build().prepareForUnknown();
    }

    /**
     * Generates a downsample index name in the format
     * prefix-fixedInterval-baseIndexName
     *
     * Note that this looks for the base index name of the provided index metadata via the
     * {@link IndexMetadata#INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY} setting. This means that in case
     * the provided index was already downsampled, we'll use the original source index (of the
     * current provided downsample index) as the base index name.
     */
    public static String generateDownsampleIndexName(
        String prefix,
        IndexMetadata sourceIndexMetadata,
        DateHistogramInterval fixedInterval
    ) {
        String downsampleOriginName = IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME.get(sourceIndexMetadata.getSettings());
        String sourceIndexName;
        if (Strings.hasText(downsampleOriginName)) {
            sourceIndexName = downsampleOriginName;
        } else if (Strings.hasText(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(sourceIndexMetadata.getSettings()))) {
            // bwc for downsample indices created pre 8.10 which didn't configure the origin
            sourceIndexName = IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(sourceIndexMetadata.getSettings());
        } else {
            sourceIndexName = sourceIndexMetadata.getIndex().getName();
        }
        return prefix + fixedInterval + "-" + sourceIndexName;
    }

    public enum SamplingMethod implements Writeable {
        AGGREGATE((byte) 0, "aggregate"),
        LAST_VALUE((byte) 1, "last_value");

        private final byte id;
        private final String label;

        SamplingMethod(byte id, String label) {
            this.id = id;
            this.label = label;
        }

        byte id() {
            return id;
        }

        public static SamplingMethod read(StreamInput in) throws IOException {
            var id = in.readByte();
            return switch (id) {
                case 0 -> AGGREGATE;
                case 1 -> LAST_VALUE;
                default -> throw new IllegalArgumentException(
                    "Sampling method id ["
                        + id
                        + "] is not one of the accepted ids "
                        + Arrays.stream(values()).map(SamplingMethod::id).toList()
                        + "."
                );
            };
        }

        /**
         * Parses the configured sampling method from string (case-insensitive).
         * @return the used sampling method, or null when the label is null.
         */
        @Nullable
        public static SamplingMethod fromString(@Nullable String label) {
            if (label == null) {
                return null;
            }
            return switch (label.toLowerCase(Locale.ROOT)) {
                case "aggregate" -> AGGREGATE;
                case "last_value" -> LAST_VALUE;
                default -> throw new IllegalArgumentException(
                    "Sampling method ["
                        + label
                        + "] is not one of the accepted methods "
                        + Arrays.stream(values()).map(SamplingMethod::toString).toList()
                        + "."
                );
            };
        }

        /**
         * Retrieves the configured sampling method from the index metadata. In case that it is null,
         * it checks if the index is downsampled and returns the `aggregate` that was the only sampling
         * method before we introduced last value.
         * @return the used sampling method, or null if the index is not downsampled.
         */
        @Nullable
        public static SamplingMethod fromIndexMetadata(IndexMetadata indexMetadata) {
            SamplingMethod method = fromString(indexMetadata.getSettings().get(IndexMetadata.INDEX_DOWNSAMPLE_METHOD_KEY));
            if (method != null) {
                return method;
            }
            // Indices downsampled before the sampling method was introduced, will not have the sampling method in their metadata.
            // For this reason, we first verify that they are indeed downsampled, and then we return `aggregate` because this was
            // the only option available at the time.
            boolean isIndexDownsampled = indexMetadata.getSettings().get(IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL_KEY) != null;
            return isIndexDownsampled ? AGGREGATE : null;
        }

        /**
         * @return the sampling method that will be used based on this configuration. Default to {@link SamplingMethod#AGGREGATE}
         * when the provided sampling method is null.
         */
        public static SamplingMethod getOrDefault(@Nullable SamplingMethod samplingMethod) {
            return samplingMethod == null ? SamplingMethod.AGGREGATE : samplingMethod;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        @Override
        public String toString() {
            return label;
        }
    }
}
