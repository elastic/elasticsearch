/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindFileStructureAction extends ActionType<FindFileStructureAction.Response> {

    public static final FindFileStructureAction INSTANCE = new FindFileStructureAction();
    public static final String NAME = "cluster:monitor/xpack/ml/findfilestructure";

    private FindFileStructureAction() {
        super(NAME, Response::new);
    }

    public static class Response extends ActionResponse implements StatusToXContentObject, Writeable {

        private FileStructure fileStructure;

        public Response(FileStructure fileStructure) {
            this.fileStructure = fileStructure;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            fileStructure = new FileStructure(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            fileStructure.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            fileStructure.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileStructure);
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            FindFileStructureAction.Response that = (FindFileStructureAction.Response) other;
            return Objects.equals(fileStructure, that.fileStructure);
        }
    }

    public static class Request extends ActionRequest {

        public static final ParseField LINES_TO_SAMPLE = new ParseField("lines_to_sample");
        public static final ParseField LINE_MERGE_SIZE_LIMIT = new ParseField("line_merge_size_limit");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField CHARSET = FileStructure.CHARSET;
        public static final ParseField FORMAT = FileStructure.FORMAT;
        public static final ParseField COLUMN_NAMES = FileStructure.COLUMN_NAMES;
        public static final ParseField HAS_HEADER_ROW = FileStructure.HAS_HEADER_ROW;
        public static final ParseField DELIMITER = FileStructure.DELIMITER;
        public static final ParseField QUOTE = FileStructure.QUOTE;
        public static final ParseField SHOULD_TRIM_FIELDS = FileStructure.SHOULD_TRIM_FIELDS;
        public static final ParseField GROK_PATTERN = FileStructure.GROK_PATTERN;
        // This one is plural in FileStructure, but singular in FileStructureOverrides
        public static final ParseField TIMESTAMP_FORMAT = new ParseField("timestamp_format");
        public static final ParseField TIMESTAMP_FIELD = FileStructure.TIMESTAMP_FIELD;

        private static final String ARG_INCOMPATIBLE_WITH_FORMAT_TEMPLATE =
            "[%s] may only be specified if [" + FORMAT.getPreferredName() + "] is [%s]";

        private Integer linesToSample;
        private Integer lineMergeSizeLimit;
        private TimeValue timeout;
        private String charset;
        private FileStructure.Format format;
        private List<String> columnNames;
        private Boolean hasHeaderRow;
        private Character delimiter;
        private Character quote;
        private Boolean shouldTrimFields;
        private String grokPattern;
        private String timestampFormat;
        private String timestampField;
        private BytesReference sample;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            linesToSample = in.readOptionalVInt();
            lineMergeSizeLimit = in.readOptionalVInt();
            timeout = in.readOptionalTimeValue();
            charset = in.readOptionalString();
            format = in.readBoolean() ? in.readEnum(FileStructure.Format.class) : null;
            columnNames = in.readBoolean() ? in.readStringList() : null;
            hasHeaderRow = in.readOptionalBoolean();
            delimiter = in.readBoolean() ? (char) in.readVInt() : null;
            quote = in.readBoolean() ? (char) in.readVInt() : null;
            shouldTrimFields = in.readOptionalBoolean();
            grokPattern = in.readOptionalString();
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

        public FileStructure.Format getFormat() {
            return format;
        }

        public void setFormat(FileStructure.Format format) {
            this.format = format;
        }

        public void setFormat(String format) {
            this.format = (format == null || format.isEmpty()) ? null : FileStructure.Format.fromString(format);
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

        private static ActionRequestValidationException addIncompatibleArgError(ParseField arg, FileStructure.Format format,
                                                                                ActionRequestValidationException validationException) {
            return addValidationError(String.format(Locale.ROOT, ARG_INCOMPATIBLE_WITH_FORMAT_TEMPLATE, arg.getPreferredName(), format),
                validationException);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (linesToSample != null && linesToSample <= 0) {
                validationException =
                    addValidationError("[" + LINES_TO_SAMPLE.getPreferredName() + "] must be positive if specified", validationException);
            }
            if (lineMergeSizeLimit != null && lineMergeSizeLimit <= 0) {
                validationException = addValidationError("[" + LINE_MERGE_SIZE_LIMIT.getPreferredName() + "] must be positive if specified",
                    validationException);
            }
            if (format != FileStructure.Format.DELIMITED) {
                if (columnNames != null) {
                    validationException = addIncompatibleArgError(COLUMN_NAMES, FileStructure.Format.DELIMITED, validationException);
                }
                if (hasHeaderRow != null) {
                    validationException = addIncompatibleArgError(HAS_HEADER_ROW, FileStructure.Format.DELIMITED, validationException);
                }
                if (delimiter != null) {
                    validationException = addIncompatibleArgError(DELIMITER, FileStructure.Format.DELIMITED, validationException);
                }
                if (quote != null) {
                    validationException = addIncompatibleArgError(QUOTE, FileStructure.Format.DELIMITED, validationException);
                }
                if (shouldTrimFields != null) {
                    validationException = addIncompatibleArgError(SHOULD_TRIM_FIELDS, FileStructure.Format.DELIMITED, validationException);
                }
            }
            if (format != FileStructure.Format.SEMI_STRUCTURED_TEXT) {
                if (grokPattern != null) {
                    validationException =
                        addIncompatibleArgError(GROK_PATTERN, FileStructure.Format.SEMI_STRUCTURED_TEXT, validationException);
                }
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
            out.writeOptionalString(timestampFormat);
            out.writeOptionalString(timestampField);
            out.writeBytesReference(sample);
        }

        @Override
        public int hashCode() {
            return Objects.hash(linesToSample, lineMergeSizeLimit, timeout, charset, format, columnNames, hasHeaderRow, delimiter,
                grokPattern, timestampFormat, timestampField, sample);
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
            return Objects.equals(this.linesToSample, that.linesToSample) &&
                Objects.equals(this.lineMergeSizeLimit, that.lineMergeSizeLimit) &&
                Objects.equals(this.timeout, that.timeout) &&
                Objects.equals(this.charset, that.charset) &&
                Objects.equals(this.format, that.format) &&
                Objects.equals(this.columnNames, that.columnNames) &&
                Objects.equals(this.hasHeaderRow, that.hasHeaderRow) &&
                Objects.equals(this.delimiter, that.delimiter) &&
                Objects.equals(this.grokPattern, that.grokPattern) &&
                Objects.equals(this.timestampFormat, that.timestampFormat) &&
                Objects.equals(this.timestampField, that.timestampField) &&
                Objects.equals(this.sample, that.sample);
        }
    }
}
