/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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
 * Data must either be in a textual delineated format (e.g. csv, tsv) or JSON
 * the {@linkplain DataFormat} enum indicates which. {@link #getTimeField()}
 * is the name of the field containing the timestamp and {@link #getTimeFormat()}
 * is the format code for the date string in as described by
 * {@link java.time.format.DateTimeFormatter}. The default quote character for
 * delineated formats is {@value #DEFAULT_QUOTE_CHAR} but any other character can be
 * used.
 */
public class DataDescription implements ToXContentObject, Writeable {
    /**
     * Enum of the acceptable data formats.
     */
    public enum DataFormat implements Writeable {
        XCONTENT,
        DELIMITED;

        /**
         * Delimited used to be called delineated. We keep supporting that for backwards
         * compatibility.
         */
        private static final String DEPRECATED_DELINEATED = "DELINEATED";

        /**
         * Case-insensitive from string method.
         * Works with either JSON, json, etc.
         *
         * @param value String representation
         * @return The data format
         */
        public static DataFormat forString(String value) {
            String valueUpperCase = value.toUpperCase(Locale.ROOT);
            return DEPRECATED_DELINEATED.equals(valueUpperCase) ? DELIMITED : DataFormat
                    .valueOf(valueUpperCase);
        }

        public static DataFormat readFromStream(StreamInput in) throws IOException {
            return in.readEnum(DataFormat.class);
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
    public static final ParseField FIELD_DELIMITER_FIELD = new ParseField("field_delimiter");
    public static final ParseField QUOTE_CHARACTER_FIELD = new ParseField("quote_character");

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

    /**
     * The default field delimiter expected by the native autodetect
     * program.
     */
    public static final char DEFAULT_DELIMITER = '\t';

    /**
     * Csv data must have this line ending
     */
    public static final char LINE_ENDING = '\n';

    /**
     * The default quote character used to escape text in
     * delineated data formats
     */
    public static final char DEFAULT_QUOTE_CHAR = '"';

    private final DataFormat dataFormat;
    private final String timeFieldName;
    private final String timeFormat;
    private final Character fieldDelimiter;
    private final Character quoteCharacter;

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser =
            new ObjectParser<>(DATA_DESCRIPTION_FIELD.getPreferredName(), ignoreUnknownFields, Builder::new);

        parser.declareString(Builder::setFormat, FORMAT_FIELD);
        parser.declareString(Builder::setTimeField, TIME_FIELD_NAME_FIELD);
        parser.declareString(Builder::setTimeFormat, TIME_FORMAT_FIELD);
        parser.declareField(Builder::setFieldDelimiter, DataDescription::extractChar, FIELD_DELIMITER_FIELD, ValueType.STRING);
        parser.declareField(Builder::setQuoteCharacter, DataDescription::extractChar, QUOTE_CHARACTER_FIELD, ValueType.STRING);

        return parser;
    }

    public DataDescription(DataFormat dataFormat, String timeFieldName, String timeFormat, Character fieldDelimiter,
                           Character quoteCharacter) {
        this.dataFormat = dataFormat;
        this.timeFieldName = timeFieldName;
        this.timeFormat = timeFormat;
        this.fieldDelimiter = fieldDelimiter;
        this.quoteCharacter = quoteCharacter;
    }

    public DataDescription(StreamInput in) throws IOException {
        dataFormat = DataFormat.readFromStream(in);
        timeFieldName = in.readString();
        timeFormat = in.readString();
        fieldDelimiter = in.readBoolean() ? (char) in.read() : null;
        quoteCharacter = in.readBoolean() ? (char) in.read() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        dataFormat.writeTo(out);
        out.writeString(timeFieldName);
        out.writeString(timeFormat);
        if (fieldDelimiter != null) {
            out.writeBoolean(true);
            out.write(fieldDelimiter);
        } else {
            out.writeBoolean(false);
        }
        if (quoteCharacter != null) {
            out.writeBoolean(true);
            out.write(quoteCharacter);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (dataFormat != DataFormat.XCONTENT) {
            builder.field(FORMAT_FIELD.getPreferredName(), dataFormat);
        }
        builder.field(TIME_FIELD_NAME_FIELD.getPreferredName(), timeFieldName);
        builder.field(TIME_FORMAT_FIELD.getPreferredName(), timeFormat);
        if (fieldDelimiter != null) {
            builder.field(FIELD_DELIMITER_FIELD.getPreferredName(), String.valueOf(fieldDelimiter));
        }
        if (quoteCharacter != null) {
            builder.field(QUOTE_CHARACTER_FIELD.getPreferredName(), String.valueOf(quoteCharacter));
        }
        builder.endObject();
        return builder;
    }

    /**
     * The format of the data to be processed.
     * Defaults to {@link DataDescription.DataFormat#XCONTENT}
     *
     * @return The data format
     */
    public DataFormat getFormat() {
        return dataFormat;
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
     * If the data is in a delineated format with a header e.g. csv or tsv
     * this is the delimiter character used. This is only applicable if
     * {@linkplain #getFormat()} is {@link DataDescription.DataFormat#DELIMITED}.
     * The default value for delimited format is {@value #DEFAULT_DELIMITER}.
     *
     * @return A char
     */
    public Character getFieldDelimiter() {
        return fieldDelimiter;
    }

    /**
     * The quote character used in delineated formats.
     * The default value for delimited format is {@value #DEFAULT_QUOTE_CHAR}.
     *
     * @return The delineated format quote character
     */
    public Character getQuoteCharacter() {
        return quoteCharacter;
    }

    /**
     * Returns true if the data described by this object needs
     * transforming before processing by autodetect.
     * A transformation must be applied if either a timeformat is
     * not in seconds since the epoch or the data is in Json format.
     *
     * @return True if the data should be transformed.
     */
    public boolean transform() {
        return dataFormat == DataFormat.XCONTENT || isTransformTime();
    }

    /**
     * Return true if the time is in a format that needs transforming.
     * Anytime format this isn't {@value #EPOCH} or <code>null</code>
     * needs transforming.
     *
     * @return True if the time field needs to be transformed.
     */
    public boolean isTransformTime() {
        return timeFormat != null && !EPOCH.equals(timeFormat);
    }

    /**
     * Return true if the time format is {@value #EPOCH_MS}
     *
     * @return True if the date is in milli-seconds since the epoch.
     */
    public boolean isEpochMs() {
        return EPOCH_MS.equals(timeFormat);
    }

    private static Character extractChar(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            String charStr = parser.text();
            if (charStr.length() != 1) {
                throw new IllegalArgumentException("String must be a single character, found [" + charStr + "]");
            }
            return charStr.charAt(0);
        }
        throw new IllegalArgumentException("Unsupported token [" + parser.currentToken() + "]");
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

        return this.dataFormat == that.dataFormat &&
                Objects.equals(this.quoteCharacter, that.quoteCharacter) &&
                Objects.equals(this.timeFieldName, that.timeFieldName) &&
                Objects.equals(this.timeFormat, that.timeFormat) &&
                Objects.equals(this.fieldDelimiter, that.fieldDelimiter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataFormat, quoteCharacter, timeFieldName, timeFormat, fieldDelimiter);
    }

    public static class Builder {

        private DataFormat dataFormat = DataFormat.XCONTENT;
        private String timeFieldName = DEFAULT_TIME_FIELD;
        private String timeFormat = EPOCH_MS;
        private Character fieldDelimiter;
        private Character quoteCharacter;

        public void setFormat(DataFormat format) {
            dataFormat = ExceptionsHelper.requireNonNull(format, FORMAT_FIELD.getPreferredName() + " must not be null");
        }

        private void setFormat(String format) {
            setFormat(DataFormat.forString(format));
        }

        public void setTimeField(String fieldName) {
            timeFieldName = ExceptionsHelper.requireNonNull(fieldName, TIME_FIELD_NAME_FIELD.getPreferredName() + " must not be null");
        }

        public void setTimeFormat(String format) {
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
                                    Messages.getMessage(Messages.JOB_CONFIG_INVALID_TIMEFORMAT, format), e.getCause());
                    }
            }
            timeFormat = format;
        }

        public void setFieldDelimiter(Character delimiter) {
            fieldDelimiter = delimiter;
        }

        public void setQuoteCharacter(Character value) {
            quoteCharacter = value;
        }

        public DataDescription build() {
            if (dataFormat == DataFormat.DELIMITED) {
                if (fieldDelimiter == null) {
                    fieldDelimiter = DEFAULT_DELIMITER;
                }
                if (quoteCharacter == null) {
                    quoteCharacter = DEFAULT_QUOTE_CHAR;
                }
            }
            return new DataDescription(dataFormat, timeFieldName, timeFormat, fieldDelimiter, quoteCharacter);
        }
    }
}
