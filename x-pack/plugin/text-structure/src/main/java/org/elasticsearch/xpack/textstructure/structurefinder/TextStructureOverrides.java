/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.action.AbstractFindStructureRequest;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.List;
import java.util.Objects;

/**
 * An immutable holder for the aspects of text structure detection that can be overridden
 * by the end user.  Every field can be <code>null</code>, and this means that that
 * aspect of the text structure detection is not overridden.
 *
 * There is no consistency checking in this class.  Consistency checking of the different
 * fields is done in {@link FindStructureAction.Request}.
 */
public class TextStructureOverrides {

    public static final TextStructureOverrides EMPTY_OVERRIDES = new Builder().build();

    private final String charset;
    private final TextStructure.Format format;
    private final List<String> columnNames;
    private final Boolean hasHeaderRow;
    private final Character delimiter;
    private final Character quote;
    private final Boolean shouldTrimFields;
    private final String grokPattern;
    private final String timestampFormat;
    private final String timestampField;

    private final String ecsCompatibility;

    public TextStructureOverrides(AbstractFindStructureRequest request) {

        this(
            request.getCharset(),
            request.getFormat(),
            request.getColumnNames(),
            request.getHasHeaderRow(),
            request.getDelimiter(),
            request.getQuote(),
            request.getShouldTrimFields(),
            request.getGrokPattern(),
            request.getTimestampFormat(),
            request.getTimestampField(),
            request.getEcsCompatibility()
        );
    }

    private TextStructureOverrides(
        String charset,
        TextStructure.Format format,
        List<String> columnNames,
        Boolean hasHeaderRow,
        Character delimiter,
        Character quote,
        Boolean shouldTrimFields,
        String grokPattern,
        String timestampFormat,
        String timestampField,
        String ecsCompatibility
    ) {
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
        this.ecsCompatibility = ecsCompatibility;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getCharset() {
        return charset;
    }

    public TextStructure.Format getFormat() {
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

    public String getEcsCompatibility() {
        return ecsCompatibility;
    }

    @Override
    public int hashCode() {

        return Objects.hash(
            charset,
            format,
            columnNames,
            hasHeaderRow,
            delimiter,
            quote,
            shouldTrimFields,
            grokPattern,
            timestampFormat,
            timestampField,
            ecsCompatibility
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

        TextStructureOverrides that = (TextStructureOverrides) other;
        return Objects.equals(this.charset, that.charset)
            && Objects.equals(this.format, that.format)
            && Objects.equals(this.columnNames, that.columnNames)
            && Objects.equals(this.hasHeaderRow, that.hasHeaderRow)
            && Objects.equals(this.delimiter, that.delimiter)
            && Objects.equals(this.quote, that.quote)
            && Objects.equals(this.shouldTrimFields, that.shouldTrimFields)
            && Objects.equals(this.grokPattern, that.grokPattern)
            && Objects.equals(this.timestampFormat, that.timestampFormat)
            && Objects.equals(this.timestampField, that.timestampField)
            && Objects.equals(this.ecsCompatibility, that.ecsCompatibility);
    }

    public static class Builder {

        private String charset;
        private TextStructure.Format format;
        private List<String> columnNames;
        private Boolean hasHeaderRow;
        private Character delimiter;
        private Character quote;
        private Boolean shouldTrimFields;
        private String grokPattern;
        private String timestampFormat;
        private String timestampField;

        private String ecsCompatibility;

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder setFormat(TextStructure.Format format) {
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

        public Builder setEcsCompatibility(String ecsCompatibility) {
            this.ecsCompatibility = ecsCompatibility;
            return this;
        }

        public TextStructureOverrides build() {

            return new TextStructureOverrides(
                charset,
                format,
                columnNames,
                hasHeaderRow,
                delimiter,
                quote,
                shouldTrimFields,
                grokPattern,
                timestampFormat,
                timestampField,
                ecsCompatibility
            );
        }
    }
}
