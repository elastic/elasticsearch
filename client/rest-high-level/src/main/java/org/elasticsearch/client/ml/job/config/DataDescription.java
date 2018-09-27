/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Describes the format of the data used in the job and how it should
 * be interpreted by the ML job.
 * <p>
 * {@link #getTimeField()} is the name of the field containing the timestamp and
 * {@link #getTimeFormat()} is the format code for the date string in as described by
 * {@link java.time.format.DateTimeFormatter}.
 */
public class DataDescription implements ToXContentObject {
    /**
     * Enum of the acceptable data formats.
     */
    public enum DataFormat {
        XCONTENT,

        /**
         * This is deprecated
         */
        DELIMITED;

        /**
         * Case-insensitive from string method.
         * Works with either XCONTENT, XContent, etc.
         *
         * @param value String representation
         * @return The data format
         */
        public static DataFormat forString(String value) {
            return DataFormat.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final ParseField DATA_DESCRIPTION_FIELD = new ParseField("data_description");
    private static final ParseField FORMAT_FIELD = new ParseField("format");
    private static final ParseField TIME_FIELD_NAME_FIELD = new ParseField("time_field");
    private static final ParseField TIME_FORMAT_FIELD = new ParseField("time_format");
    private static final ParseField FIELD_DELIMITER_FIELD = new ParseField("field_delimiter");
    private static final ParseField QUOTE_CHARACTER_FIELD = new ParseField("quote_character");

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
     * The default quote character used to escape text in
     * delimited data formats
     */
    public static final char DEFAULT_QUOTE_CHAR = '"';

    private final DataFormat dataFormat;
    private final String timeFieldName;
    private final String timeFormat;
    private final Character fieldDelimiter;
    private final Character quoteCharacter;

    public static final ObjectParser<Builder, Void> PARSER =
        new ObjectParser<>(DATA_DESCRIPTION_FIELD.getPreferredName(), true, Builder::new);

    static {
        PARSER.declareString(Builder::setFormat, FORMAT_FIELD);
        PARSER.declareString(Builder::setTimeField, TIME_FIELD_NAME_FIELD);
        PARSER.declareString(Builder::setTimeFormat, TIME_FORMAT_FIELD);
        PARSER.declareField(Builder::setFieldDelimiter, DataDescription::extractChar, FIELD_DELIMITER_FIELD, ValueType.STRING);
        PARSER.declareField(Builder::setQuoteCharacter, DataDescription::extractChar, QUOTE_CHARACTER_FIELD, ValueType.STRING);
    }

    public DataDescription(DataFormat dataFormat, String timeFieldName, String timeFormat, Character fieldDelimiter,
                           Character quoteCharacter) {
        this.dataFormat = dataFormat;
        this.timeFieldName = timeFieldName;
        this.timeFormat = timeFormat;
        this.fieldDelimiter = fieldDelimiter;
        this.quoteCharacter = quoteCharacter;
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
     * If the data is in a delimited format with a header e.g. csv or tsv
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
     * The quote character used in delimited formats.
     * The default value for delimited format is {@value #DEFAULT_QUOTE_CHAR}.
     *
     * @return The delimited format quote character
     */
    public Character getQuoteCharacter() {
        return quoteCharacter;
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

        public Builder setFormat(DataFormat format) {
            dataFormat = Objects.requireNonNull(format);
            return this;
        }

        private Builder setFormat(String format) {
            setFormat(DataFormat.forString(format));
            return this;
        }

        public Builder setTimeField(String fieldName) {
            timeFieldName = Objects.requireNonNull(fieldName);
            return this;
        }

        public Builder setTimeFormat(String format) {
            timeFormat = Objects.requireNonNull(format);
            return this;
        }

        public Builder setFieldDelimiter(Character delimiter) {
            fieldDelimiter = delimiter;
            return this;
        }

        public Builder setQuoteCharacter(Character value) {
            quoteCharacter = value;
            return this;
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
