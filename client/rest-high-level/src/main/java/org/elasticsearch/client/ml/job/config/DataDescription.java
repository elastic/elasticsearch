/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
        XCONTENT;

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
    private static final ParseField TIME_FIELD_NAME_FIELD = new ParseField("time_field");
    private static final ParseField TIME_FORMAT_FIELD = new ParseField("time_format");

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

    public static final ObjectParser<Builder, Void> PARSER =
        new ObjectParser<>(DATA_DESCRIPTION_FIELD.getPreferredName(), true, Builder::new);

    static {
        PARSER.declareString(Builder::setTimeField, TIME_FIELD_NAME_FIELD);
        PARSER.declareString(Builder::setTimeFormat, TIME_FORMAT_FIELD);
    }

    public DataDescription(String timeFieldName, String timeFormat) {
        this.timeFieldName = timeFieldName;
        this.timeFormat = timeFormat;
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
     * The format of the data to be processed.
     * Always {@link DataDescription.DataFormat#XCONTENT}
     *
     * @return The data format
     */
    public DataFormat getFormat() {
        return DataFormat.XCONTENT;
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

        public Builder setFormat(DataFormat format) {
            Objects.requireNonNull(format);
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

        public DataDescription build() {
            return new DataDescription(timeFieldName, timeFormat);
        }
    }
}
