/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindStructureAction extends ActionType<FindStructureAction.Response> {

    public static final FindStructureAction INSTANCE = new FindStructureAction();
    public static final String NAME = "cluster:monitor/text_structure/findstructure";

    public static final int MIN_SAMPLE_LINE_COUNT = 2;

    private FindStructureAction() {
        super(NAME, Response::new);
    }

    public static class Response extends ActionResponse implements StatusToXContentObject, Writeable {

        private final TextStructure textStructure;

        public Response(TextStructure textStructure) {
            this.textStructure = textStructure;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            textStructure = new TextStructure(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            textStructure.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            textStructure.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(textStructure);
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            FindStructureAction.Response that = (FindStructureAction.Response) other;
            return Objects.equals(textStructure, that.textStructure);
        }
    }

    public static class Request extends ActionRequest {

        public static final ParseField LINES_TO_SAMPLE = new ParseField("lines_to_sample");
        public static final ParseField LINE_MERGE_SIZE_LIMIT = new ParseField("line_merge_size_limit");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField CHARSET = TextStructure.CHARSET;
        public static final ParseField FORMAT = TextStructure.FORMAT;
        public static final ParseField COLUMN_NAMES = TextStructure.COLUMN_NAMES;
        public static final ParseField HAS_HEADER_ROW = TextStructure.HAS_HEADER_ROW;
        public static final ParseField DELIMITER = TextStructure.DELIMITER;
        public static final ParseField QUOTE = TextStructure.QUOTE;
        public static final ParseField SHOULD_TRIM_FIELDS = TextStructure.SHOULD_TRIM_FIELDS;
        public static final ParseField GROK_PATTERN = TextStructure.GROK_PATTERN;
        // This one is plural in FileStructure, but singular in FileStructureOverrides
        public static final ParseField TIMESTAMP_FORMAT = new ParseField("timestamp_format");
        public static final ParseField TIMESTAMP_FIELD = TextStructure.TIMESTAMP_FIELD;

        public static final ParseField ECS_COMPATIBILITY = TextStructure.ECS_COMPATIBILITY;

        private static final String ARG_INCOMPATIBLE_WITH_FORMAT_TEMPLATE = "[%s] may only be specified if ["
            + FORMAT.getPreferredName()
            + "] is [%s]";

        private Integer linesToSample;
        private Integer lineMergeSizeLimit;
        private TimeValue timeout;
        private String charset;
        private TextStructure.Format format;
        private List<String> columnNames;
        private Boolean hasHeaderRow;
        private Character delimiter;
        private Character quote;
        private Boolean shouldTrimFields;
        private String grokPattern;
        private String ecsCompatibility;
        private String timestampFormat;
        private String timestampField;
        private BytesReference sample;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            linesToSample = in.readOptionalVInt();
            lineMergeSizeLimit = in.readOptionalVInt();
            timeout = in.readOptionalTimeValue();
            charset = in.readOptionalString();
            format = in.readBoolean() ? in.readEnum(TextStructure.Format.class) : null;
            columnNames = in.readBoolean() ? in.readStringList() : null;
            hasHeaderRow = in.readOptionalBoolean();
            delimiter = in.readBoolean() ? (char) in.readVInt() : null;
            quote = in.readBoolean() ? (char) in.readVInt() : null;
            shouldTrimFields = in.readOptionalBoolean();
            grokPattern = in.readOptionalString();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
                ecsCompatibility = in.readOptionalString();
            } else {
                ecsCompatibility = null;
            }
            timestampFormat = in.readOptionalString();
            timestampField = in.readOptionalString();
            sample = in.readBytesReference();
        }

        public Integer getLinesToSample() {
            return linesToSample;
        }

        public void setLinesToSample(Integer linesToSample) {
            this.linesToSample = linesToSample;
        }

        public Integer getLineMergeSizeLimit() {
            return lineMergeSizeLimit;
        }

        public void setLineMergeSizeLimit(Integer lineMergeSizeLimit) {
            this.lineMergeSizeLimit = lineMergeSizeLimit;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        public String getCharset() {
            return charset;
        }

        public void setCharset(String charset) {
            this.charset = (charset == null || charset.isEmpty()) ? null : charset;
        }

        public TextStructure.Format getFormat() {
            return format;
        }

        public void setFormat(TextStructure.Format format) {
            this.format = format;
        }

        public void setFormat(String format) {
            this.format = (format == null || format.isEmpty()) ? null : TextStructure.Format.fromString(format);
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public void setColumnNames(List<String> columnNames) {
            this.columnNames = (columnNames == null || columnNames.isEmpty()) ? null : columnNames;
        }

        public void setColumnNames(String[] columnNames) {
            this.columnNames = (columnNames == null || columnNames.length == 0) ? null : Arrays.asList(columnNames);
        }

        public Boolean getHasHeaderRow() {
            return hasHeaderRow;
        }

        public void setHasHeaderRow(Boolean hasHeaderRow) {
            this.hasHeaderRow = hasHeaderRow;
        }

        public Character getDelimiter() {
            return delimiter;
        }

        public void setDelimiter(Character delimiter) {
            this.delimiter = delimiter;
        }

        public void setDelimiter(String delimiter) {
            if (delimiter == null || delimiter.isEmpty()) {
                this.delimiter = null;
            } else if (delimiter.length() == 1) {
                this.delimiter = delimiter.charAt(0);
            } else {
                throw new IllegalArgumentException(DELIMITER.getPreferredName() + " must be a single character");
            }
        }

        public Character getQuote() {
            return quote;
        }

        public void setQuote(Character quote) {
            this.quote = quote;
        }

        public void setQuote(String quote) {
            if (quote == null || quote.isEmpty()) {
                this.quote = null;
            } else if (quote.length() == 1) {
                this.quote = quote.charAt(0);
            } else {
                throw new IllegalArgumentException(QUOTE.getPreferredName() + " must be a single character");
            }
        }

        public Boolean getShouldTrimFields() {
            return shouldTrimFields;
        }

        public void setShouldTrimFields(Boolean shouldTrimFields) {
            this.shouldTrimFields = shouldTrimFields;
        }

        public String getGrokPattern() {
            return grokPattern;
        }

        public void setGrokPattern(String grokPattern) {
            this.grokPattern = (grokPattern == null || grokPattern.isEmpty()) ? null : grokPattern;
        }

        public String getEcsCompatibility() {
            return ecsCompatibility;
        }

        public void setEcsCompatibility(String ecsCompatibility) {
            this.ecsCompatibility = (ecsCompatibility == null || ecsCompatibility.isEmpty()) ? null : ecsCompatibility;
        }

        public String getTimestampFormat() {
            return timestampFormat;
        }

        public void setTimestampFormat(String timestampFormat) {
            this.timestampFormat = (timestampFormat == null || timestampFormat.isEmpty()) ? null : timestampFormat;
        }

        public String getTimestampField() {
            return timestampField;
        }

        public void setTimestampField(String timestampField) {
            this.timestampField = (timestampField == null || timestampField.isEmpty()) ? null : timestampField;
        }

        public BytesReference getSample() {
            return sample;
        }

        public void setSample(BytesReference sample) {
            this.sample = sample;
        }

        private static ActionRequestValidationException addIncompatibleArgError(
            ParseField arg,
            TextStructure.Format format,
            ActionRequestValidationException validationException
        ) {
            return addValidationError(
                String.format(Locale.ROOT, ARG_INCOMPATIBLE_WITH_FORMAT_TEMPLATE, arg.getPreferredName(), format),
                validationException
            );
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (linesToSample != null && linesToSample < MIN_SAMPLE_LINE_COUNT) {
                validationException = addValidationError(
                    "[" + LINES_TO_SAMPLE.getPreferredName() + "] must be at least [" + MIN_SAMPLE_LINE_COUNT + "] if specified",
                    validationException
                );
            }
            if (lineMergeSizeLimit != null && lineMergeSizeLimit <= 0) {
                validationException = addValidationError(
                    "[" + LINE_MERGE_SIZE_LIMIT.getPreferredName() + "] must be positive if specified",
                    validationException
                );
            }
            if (format != TextStructure.Format.DELIMITED) {
                if (columnNames != null) {
                    validationException = addIncompatibleArgError(COLUMN_NAMES, TextStructure.Format.DELIMITED, validationException);
                }
                if (hasHeaderRow != null) {
                    validationException = addIncompatibleArgError(HAS_HEADER_ROW, TextStructure.Format.DELIMITED, validationException);
                }
                if (delimiter != null) {
                    validationException = addIncompatibleArgError(DELIMITER, TextStructure.Format.DELIMITED, validationException);
                }
                if (quote != null) {
                    validationException = addIncompatibleArgError(QUOTE, TextStructure.Format.DELIMITED, validationException);
                }
                if (shouldTrimFields != null) {
                    validationException = addIncompatibleArgError(SHOULD_TRIM_FIELDS, TextStructure.Format.DELIMITED, validationException);
                }
            }
            if (format != TextStructure.Format.SEMI_STRUCTURED_TEXT) {
                if (grokPattern != null) {
                    validationException = addIncompatibleArgError(
                        GROK_PATTERN,
                        TextStructure.Format.SEMI_STRUCTURED_TEXT,
                        validationException
                    );
                }
            }

            if (ecsCompatibility != null && GrokBuiltinPatterns.isValidEcsCompatibilityMode(ecsCompatibility) == false) {
                validationException = addValidationError(
                    "["
                        + ECS_COMPATIBILITY.getPreferredName()
                        + "] must be one of ["
                        + String.join(", ", GrokBuiltinPatterns.ECS_COMPATIBILITY_MODES)
                        + "] if specified",
                    validationException
                );
            }

            if (sample == null || sample.length() == 0) {
                validationException = addValidationError("sample must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalVInt(linesToSample);
            out.writeOptionalVInt(lineMergeSizeLimit);
            out.writeOptionalTimeValue(timeout);
            out.writeOptionalString(charset);
            if (format == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeEnum(format);
            }
            if (columnNames == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeCollection(columnNames, StreamOutput::writeString);
            }
            out.writeOptionalBoolean(hasHeaderRow);
            if (delimiter == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeVInt(delimiter);
            }
            if (quote == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeVInt(quote);
            }
            out.writeOptionalBoolean(shouldTrimFields);
            out.writeOptionalString(grokPattern);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
                out.writeOptionalString(ecsCompatibility);
            }
            out.writeOptionalString(timestampFormat);
            out.writeOptionalString(timestampField);
            out.writeBytesReference(sample);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                linesToSample,
                lineMergeSizeLimit,
                timeout,
                charset,
                format,
                columnNames,
                hasHeaderRow,
                delimiter,
                grokPattern,
                ecsCompatibility,
                timestampFormat,
                timestampField,
                sample
            );
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            Request that = (Request) other;
            return Objects.equals(this.linesToSample, that.linesToSample)
                && Objects.equals(this.lineMergeSizeLimit, that.lineMergeSizeLimit)
                && Objects.equals(this.timeout, that.timeout)
                && Objects.equals(this.charset, that.charset)
                && Objects.equals(this.format, that.format)
                && Objects.equals(this.columnNames, that.columnNames)
                && Objects.equals(this.hasHeaderRow, that.hasHeaderRow)
                && Objects.equals(this.delimiter, that.delimiter)
                && Objects.equals(this.grokPattern, that.grokPattern)
                && Objects.equals(this.ecsCompatibility, that.ecsCompatibility)
                && Objects.equals(this.timestampFormat, that.timestampFormat)
                && Objects.equals(this.timestampField, that.timestampField)
                && Objects.equals(this.sample, that.sample);
        }
    }
}
