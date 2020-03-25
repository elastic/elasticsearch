/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.xpack.core.ml.action.FindFileStructureAction;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.util.List;
import java.util.Objects;

/**
 * An immutable holder for the aspects of file structure detection that can be overridden
 * by the end user.  Every field can be <code>null</code>, and this means that that
 * aspect of the file structure detection is not overridden.
 *
 * There is no consistency checking in this class.  Consistency checking of the different
 * fields is done in {@link FindFileStructureAction.Request}.
 */
public class FileStructureOverrides {

    public static final FileStructureOverrides EMPTY_OVERRIDES = new Builder().build();

    private final String charset;
    private final FileStructure.Format format;
    private final List<String> columnNames;
    private final Boolean hasHeaderRow;
    private final Character delimiter;
    private final Character quote;
    private final Boolean shouldTrimFields;
    private final String grokPattern;
    private final String timestampFormat;
    private final String timestampField;

    public FileStructureOverrides(FindFileStructureAction.Request request) {

        this(request.getCharset(), request.getFormat(), request.getColumnNames(), request.getHasHeaderRow(), request.getDelimiter(),
            request.getQuote(), request.getShouldTrimFields(), request.getGrokPattern(), request.getTimestampFormat(),
            request.getTimestampField());
    }

    private FileStructureOverrides(String charset, FileStructure.Format format, List<String> columnNames, Boolean hasHeaderRow,
                                   Character delimiter, Character quote, Boolean shouldTrimFields, String grokPattern,
                                   String timestampFormat, String timestampField) {
        this.charset = charset;
        this.format = format;
        this.columnNames = (columnNames == null) ? null : List.copyOf(columnNames);
        this.hasHeaderRow = hasHeaderRow;
        this.delimiter = delimiter;
        this.quote = quote;
        this.shouldTrimFields = shouldTrimFields;
        this.grokPattern = grokPattern;
        this.timestampFormat = timestampFormat;
        this.timestampField = timestampField;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getCharset() {
        return charset;
    }

    public FileStructure.Format getFormat() {
        return format;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public Boolean getHasHeaderRow() {
        return hasHeaderRow;
    }

    public Character getDelimiter() {
        return delimiter;
    }

    public Character getQuote() {
        return quote;
    }

    public Boolean getShouldTrimFields() {
        return shouldTrimFields;
    }

    public String getGrokPattern() {
        return grokPattern;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public String getTimestampField() {
        return timestampField;
    }

    @Override
    public int hashCode() {

        return Objects.hash(charset, format, columnNames, hasHeaderRow, delimiter, quote, shouldTrimFields, grokPattern, timestampFormat,
            timestampField);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FileStructureOverrides that = (FileStructureOverrides) other;
        return Objects.equals(this.charset, that.charset) &&
            Objects.equals(this.format, that.format) &&
            Objects.equals(this.columnNames, that.columnNames) &&
            Objects.equals(this.hasHeaderRow, that.hasHeaderRow) &&
            Objects.equals(this.delimiter, that.delimiter) &&
            Objects.equals(this.quote, that.quote) &&
            Objects.equals(this.shouldTrimFields, that.shouldTrimFields) &&
            Objects.equals(this.grokPattern, that.grokPattern) &&
            Objects.equals(this.timestampFormat, that.timestampFormat) &&
            Objects.equals(this.timestampField, that.timestampField);
    }

    public static class Builder {

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

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder setFormat(FileStructure.Format format) {
            this.format = format;
            return this;
        }

        public Builder setColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public Builder setHasHeaderRow(Boolean hasHeaderRow) {
            this.hasHeaderRow = hasHeaderRow;
            return this;
        }

        public Builder setDelimiter(Character delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setQuote(Character quote) {
            this.quote = quote;
            return this;
        }

        public Builder setShouldTrimFields(Boolean shouldTrimFields) {
            this.shouldTrimFields = shouldTrimFields;
            return this;
        }

        public Builder setGrokPattern(String grokPattern) {
            this.grokPattern = grokPattern;
            return this;
        }

        public Builder setTimestampFormat(String timestampFormat) {
            this.timestampFormat = timestampFormat;
            return this;
        }

        public Builder setTimestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        public FileStructureOverrides build() {

            return new FileStructureOverrides(charset, format, columnNames, hasHeaderRow, delimiter, quote, shouldTrimFields, grokPattern,
                timestampFormat, timestampField);
        }
    }
}
