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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.ml.filestructurefinder.FileStructure;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class FindFileStructureRequest implements Validatable, ToXContentFragment {

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
    public static final ParseField EXPLAIN = new ParseField("explain");

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
    private Boolean explain;
    private BytesReference sample;

    public FindFileStructureRequest() {
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

    public Boolean getExplain() {
        return explain;
    }

    public void setExplain(Boolean explain) {
        this.explain = explain;
    }

    public BytesReference getSample() {
        return sample;
    }

    public void setSample(byte[] sample) {
        this.sample = new BytesArray(sample);
    }

    public void setSample(BytesReference sample) {
        this.sample = Objects.requireNonNull(sample);
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = new ValidationException();
        if (sample == null || sample.length() == 0) {
            validationException.addValidationError("sample must be specified");
        }
        return validationException.validationErrors().isEmpty() ? Optional.empty() : Optional.of(validationException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        if (linesToSample != null) {
            builder.field(LINES_TO_SAMPLE.getPreferredName(), linesToSample);
        }
        if (lineMergeSizeLimit != null) {
            builder.field(LINE_MERGE_SIZE_LIMIT.getPreferredName(), lineMergeSizeLimit);
        }
        if (timeout != null) {
            builder.field(TIMEOUT.getPreferredName(), timeout);
        }
        if (charset != null) {
            builder.field(CHARSET.getPreferredName(), charset);
        }
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        if (columnNames != null) {
            builder.field(COLUMN_NAMES.getPreferredName(), columnNames);
        }
        if (hasHeaderRow != null) {
            builder.field(HAS_HEADER_ROW.getPreferredName(), hasHeaderRow);
        }
        if (delimiter != null) {
            builder.field(DELIMITER.getPreferredName(), delimiter.toString());
        }
        if (quote != null) {
            builder.field(QUOTE.getPreferredName(), quote.toString());
        }
        if (shouldTrimFields != null) {
            builder.field(SHOULD_TRIM_FIELDS.getPreferredName(), shouldTrimFields);
        }
        if (grokPattern != null) {
            builder.field(GROK_PATTERN.getPreferredName(), grokPattern);
        }
        if (timestampFormat != null) {
            builder.field(TIMESTAMP_FORMAT.getPreferredName(), timestampFormat);
        }
        if (timestampField != null) {
            builder.field(TIMESTAMP_FIELD.getPreferredName(), timestampField);
        }
        if (explain != null) {
            builder.field(EXPLAIN.getPreferredName(), explain);
        }
        // Sample is not included in the X-Content representation
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(linesToSample, lineMergeSizeLimit, timeout, charset, format, columnNames, hasHeaderRow, delimiter, grokPattern,
            timestampFormat, timestampField, explain, sample);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FindFileStructureRequest that = (FindFileStructureRequest) other;
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
            Objects.equals(this.explain, that.explain) &&
            Objects.equals(this.sample, that.sample);
    }
}
