/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.time.DateTimeFormatterTimestampConverter;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Objects;

/**
 * Describes the format of the data used in the job and how it should
 * be interpreted by autodetect.
 * <p>
 * Data must either be in JSON or SMILE format (and only JSON is publicly documented).
 * The {@linkplain DataFormat} enum is always set to XCONTENT. {@link #getTimeField()}
 * is the name of the field containing the timestamp and {@link #getTimeFormat()}
 * is the format code for the date string in as described by
 * {@link java.time.format.DateTimeFormatter}.
 */
public class DataDescription implements ToXContentObject, Writeable {
    /**
     * Enum of the acceptable data formats.
     */
    public enum DataFormat implements Writeable {
        XCONTENT;

        /**
         * Delimited used to be an option, although it was never documented.
         * We silently convert it to XContent now.
         */
        private static final String REMOVED_DELIMITED = "DELIMITED";

        /**
         * Case-insensitive from string method.
         * Works with either XCONTENT, Xcontent, etc.
         * The old value DELIMITED is tolerated as it may have been persisted,
         * but is silently converted to XCONTENT as it was never documented.
         * Any other values throw an exception.
         * @param value String representation
         * @return The data format
         */
        public static DataFormat forString(String value) {
            String valueUpperCase = value.toUpperCase(Locale.ROOT);
            return REMOVED_DELIMITED.equals(valueUpperCase) ? XCONTENT : DataFormat.valueOf(valueUpperCase);
        }

        public static DataFormat readFromStream(StreamInput in) {
            try {
                return in.readEnum(DataFormat.class);
            } catch (IOException e) {
                // Older nodes may serialise DELIMITED on the wire, which will cause an exception.
                // We just silently convert to XCONTENT like we do when parsing.
                return XCONTENT;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final ParseField DATA_DESCRIPTION_FIELD = new ParseField("data_description");
    public static final ParseField FORMAT_FIELD = new ParseField("format");
    public static final ParseField TIME_FIELD_NAME_FIELD = new ParseField("time_field");
    public static final ParseField TIME_FORMAT_FIELD = new ParseField("time_format");

    /**
     * Special time format string for epoch times (seconds)
     */
    public static final String EPOCH = "epoch";

    /**
     * Special time format string for epoch times (milli-seconds)
     */
    public static final String EPOCH_MS = "epoch_ms";

    /**
     * By default autodetect expects the timestamp in a field with this name
     */
    public static final String DEFAULT_TIME_FIELD = "time";

    private final String timeFieldName;
    private final String timeFormat;

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(
            DATA_DESCRIPTION_FIELD.getPreferredName(),
            ignoreUnknownFields,
            Builder::new
        );

        if (ignoreUnknownFields == false) {
            // The strict parser needs to tolerate this field as it's documented, but there's only one value so we don't need to store it
            parser.declareString((builder, format) -> DataFormat.forString(format), FORMAT_FIELD);
        }
        parser.declareString(Builder::setTimeField, TIME_FIELD_NAME_FIELD);
        parser.declareString(Builder::setTimeFormat, TIME_FORMAT_FIELD);

        return parser;
    }

    public DataDescription(String timeFieldName, String timeFormat) {
        this.timeFieldName = timeFieldName;
        this.timeFormat = timeFormat;
    }

    public DataDescription(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_8_0_0)) {
            DataFormat.readFromStream(in);
        }
        timeFieldName = in.readString();
        timeFormat = in.readString();
        if (in.getVersion().before(Version.V_8_0_0)) {
            // fieldDelimiter
            if (in.readBoolean()) {
                in.read();
            }
            // quoteCharacter
            if (in.readBoolean()) {
                in.read();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_8_0_0)) {
            DataFormat.XCONTENT.writeTo(out);
        }
        out.writeString(timeFieldName);
        out.writeString(timeFormat);
        if (out.getVersion().before(Version.V_8_0_0)) {
            // fieldDelimiter
            out.writeBoolean(false);
            // quoteCharacter
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TIME_FIELD_NAME_FIELD.getPreferredName(), timeFieldName);
        builder.field(TIME_FORMAT_FIELD.getPreferredName(), timeFormat);
        builder.endObject();
        return builder;
    }

    /**
     * The name of the field containing the timestamp
     *
     * @return A String if set or <code>null</code>
     */
    public String getTimeField() {
        return timeFieldName;
    }

    /**
     * Either {@value #EPOCH}, {@value #EPOCH_MS} or a SimpleDateTime format string.
     * If not set (is <code>null</code> or an empty string) or set to
     * {@value #EPOCH_MS} (the default) then the date is assumed to be in
     * milliseconds from the epoch.
     *
     * @return A String if set or <code>null</code>
     */
    public String getTimeFormat() {
        return timeFormat;
    }

    /**
     * Return true if the time is in a format that needs transforming.
     * Anytime format this isn't {@value #EPOCH} or <code>null</code>
     * needs transforming.
     *
     * @return True if the time field needs to be transformed.
     */
    public boolean isTransformTime() {
        return timeFormat != null && EPOCH.equals(timeFormat) == false;
    }

    /**
     * Return true if the time format is {@value #EPOCH_MS}
     *
     * @return True if the date is in milli-seconds since the epoch.
     */
    public boolean isEpochMs() {
        return EPOCH_MS.equals(timeFormat);
    }

    /**
     * Overridden equality test
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof DataDescription == false) {
            return false;
        }

        DataDescription that = (DataDescription) other;

        return Objects.equals(this.timeFieldName, that.timeFieldName) && Objects.equals(this.timeFormat, that.timeFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeFieldName, timeFormat);
    }

    public static class Builder {

        private String timeFieldName = DEFAULT_TIME_FIELD;
        private String timeFormat = EPOCH_MS;

        public Builder setTimeField(String fieldName) {
            timeFieldName = ExceptionsHelper.requireNonNull(fieldName, TIME_FIELD_NAME_FIELD.getPreferredName() + " must not be null");
            return this;
        }

        public Builder setTimeFormat(String format) {
            ExceptionsHelper.requireNonNull(format, TIME_FORMAT_FIELD.getPreferredName() + " must not be null");
            switch (format) {
                case EPOCH:
                case EPOCH_MS:
                    break;
                default:
                    try {
                        DateTimeFormatterTimestampConverter.ofPattern(format, ZoneOffset.UTC);
                    } catch (IllegalArgumentException e) {
                        throw ExceptionsHelper.badRequestException(
                            Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, format),
                            e.getCause()
                        );
                    }
            }
            timeFormat = format;
            return this;
        }

        public DataDescription build() {
            return new DataDescription(timeFieldName, timeFormat);
        }
    }
}
