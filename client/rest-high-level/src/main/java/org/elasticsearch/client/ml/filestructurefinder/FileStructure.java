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
package org.elasticsearch.client.ml.filestructurefinder;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Stores the file format determined by Machine Learning.
 */
public class FileStructure implements ToXContentObject {

    public enum Format {

        NDJSON, XML, DELIMITED, SEMI_STRUCTURED_TEXT;

        public static Format fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final ParseField NUM_LINES_ANALYZED = new ParseField("num_lines_analyzed");
    public static final ParseField NUM_MESSAGES_ANALYZED = new ParseField("num_messages_analyzed");
    public static final ParseField SAMPLE_START = new ParseField("sample_start");
    public static final ParseField CHARSET = new ParseField("charset");
    public static final ParseField HAS_BYTE_ORDER_MARKER = new ParseField("has_byte_order_marker");
    public static final ParseField FORMAT = new ParseField("format");
    public static final ParseField MULTILINE_START_PATTERN = new ParseField("multiline_start_pattern");
    public static final ParseField EXCLUDE_LINES_PATTERN = new ParseField("exclude_lines_pattern");
    public static final ParseField COLUMN_NAMES = new ParseField("column_names");
    public static final ParseField HAS_HEADER_ROW = new ParseField("has_header_row");
    public static final ParseField DELIMITER = new ParseField("delimiter");
    public static final ParseField QUOTE = new ParseField("quote");
    public static final ParseField SHOULD_TRIM_FIELDS = new ParseField("should_trim_fields");
    public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp_field");
    public static final ParseField JODA_TIMESTAMP_FORMATS = new ParseField("joda_timestamp_formats");
    public static final ParseField JAVA_TIMESTAMP_FORMATS = new ParseField("java_timestamp_formats");
    public static final ParseField NEED_CLIENT_TIMEZONE = new ParseField("need_client_timezone");
    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField INGEST_PIPELINE = new ParseField("ingest_pipeline");
    public static final ParseField FIELD_STATS = new ParseField("field_stats");
    public static final ParseField EXPLANATION = new ParseField("explanation");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("file_structure", true, Builder::new);

    static {
        PARSER.declareInt(Builder::setNumLinesAnalyzed, NUM_LINES_ANALYZED);
        PARSER.declareInt(Builder::setNumMessagesAnalyzed, NUM_MESSAGES_ANALYZED);
        PARSER.declareString(Builder::setSampleStart, SAMPLE_START);
        PARSER.declareString(Builder::setCharset, CHARSET);
        PARSER.declareBoolean(Builder::setHasByteOrderMarker, HAS_BYTE_ORDER_MARKER);
        PARSER.declareString((p, c) -> p.setFormat(Format.fromString(c)), FORMAT);
        PARSER.declareString(Builder::setMultilineStartPattern, MULTILINE_START_PATTERN);
        PARSER.declareString(Builder::setExcludeLinesPattern, EXCLUDE_LINES_PATTERN);
        PARSER.declareStringArray(Builder::setColumnNames, COLUMN_NAMES);
        PARSER.declareBoolean(Builder::setHasHeaderRow, HAS_HEADER_ROW);
        PARSER.declareString((p, c) -> p.setDelimiter(c.charAt(0)), DELIMITER);
        PARSER.declareString((p, c) -> p.setQuote(c.charAt(0)), QUOTE);
        PARSER.declareBoolean(Builder::setShouldTrimFields, SHOULD_TRIM_FIELDS);
        PARSER.declareString(Builder::setGrokPattern, GROK_PATTERN);
        PARSER.declareString(Builder::setTimestampField, TIMESTAMP_FIELD);
        PARSER.declareStringArray(Builder::setJodaTimestampFormats, JODA_TIMESTAMP_FORMATS);
        PARSER.declareStringArray(Builder::setJavaTimestampFormats, JAVA_TIMESTAMP_FORMATS);
        PARSER.declareBoolean(Builder::setNeedClientTimezone, NEED_CLIENT_TIMEZONE);
        PARSER.declareObject(Builder::setMappings, (p, c) -> new TreeMap<>(p.map()), MAPPINGS);
        PARSER.declareObject(Builder::setIngestPipeline, (p, c) -> p.mapOrdered(), INGEST_PIPELINE);
        PARSER.declareObject(Builder::setFieldStats, (p, c) -> {
            Map<String, FieldStats> fieldStats = new TreeMap<>();
            while (p.nextToken() == XContentParser.Token.FIELD_NAME) {
                fieldStats.put(p.currentName(), FieldStats.PARSER.apply(p, c));
            }
            return fieldStats;
        }, FIELD_STATS);
        PARSER.declareStringArray(Builder::setExplanation, EXPLANATION);
    }

    private final int numLinesAnalyzed;
    private final int numMessagesAnalyzed;
    private final String sampleStart;
    private final String charset;
    private final Boolean hasByteOrderMarker;
    private final Format format;
    private final String multilineStartPattern;
    private final String excludeLinesPattern;
    private final List<String> columnNames;
    private final Boolean hasHeaderRow;
    private final Character delimiter;
    private final Character quote;
    private final Boolean shouldTrimFields;
    private final String grokPattern;
    private final List<String> jodaTimestampFormats;
    private final List<String> javaTimestampFormats;
    private final String timestampField;
    private final boolean needClientTimezone;
    private final SortedMap<String, Object> mappings;
    private final Map<String, Object> ingestPipeline;
    private final SortedMap<String, FieldStats> fieldStats;
    private final List<String> explanation;

    private FileStructure(int numLinesAnalyzed, int numMessagesAnalyzed, String sampleStart, String charset, Boolean hasByteOrderMarker,
                          Format format, String multilineStartPattern, String excludeLinesPattern, List<String> columnNames,
                          Boolean hasHeaderRow, Character delimiter, Character quote, Boolean shouldTrimFields, String grokPattern,
                          String timestampField, List<String> jodaTimestampFormats, List<String> javaTimestampFormats,
                          boolean needClientTimezone, Map<String, Object> mappings, Map<String, Object> ingestPipeline,
                          Map<String, FieldStats> fieldStats, List<String> explanation) {

        this.numLinesAnalyzed = numLinesAnalyzed;
        this.numMessagesAnalyzed = numMessagesAnalyzed;
        this.sampleStart = Objects.requireNonNull(sampleStart);
        this.charset = Objects.requireNonNull(charset);
        this.hasByteOrderMarker = hasByteOrderMarker;
        this.format = Objects.requireNonNull(format);
        this.multilineStartPattern = multilineStartPattern;
        this.excludeLinesPattern = excludeLinesPattern;
        this.columnNames = (columnNames == null) ? null : List.copyOf(columnNames);
        this.hasHeaderRow = hasHeaderRow;
        this.delimiter = delimiter;
        this.quote = quote;
        this.shouldTrimFields = shouldTrimFields;
        this.grokPattern = grokPattern;
        this.timestampField = timestampField;
        this.jodaTimestampFormats = (jodaTimestampFormats == null) ? null : List.copyOf(jodaTimestampFormats);
        this.javaTimestampFormats = (javaTimestampFormats == null) ? null : List.copyOf(javaTimestampFormats);
        this.needClientTimezone = needClientTimezone;
        this.mappings = Collections.unmodifiableSortedMap(new TreeMap<>(mappings));
        this.ingestPipeline = (ingestPipeline == null) ? null : Collections.unmodifiableMap(new LinkedHashMap<>(ingestPipeline));
        this.fieldStats = Collections.unmodifiableSortedMap(new TreeMap<>(fieldStats));
        this.explanation = (explanation == null) ? null : List.copyOf(explanation);
    }

    public int getNumLinesAnalyzed() {
        return numLinesAnalyzed;
    }

    public int getNumMessagesAnalyzed() {
        return numMessagesAnalyzed;
    }

    public String getSampleStart() {
        return sampleStart;
    }

    public String getCharset() {
        return charset;
    }

    public Boolean getHasByteOrderMarker() {
        return hasByteOrderMarker;
    }

    public Format getFormat() {
        return format;
    }

    public String getMultilineStartPattern() {
        return multilineStartPattern;
    }

    public String getExcludeLinesPattern() {
        return excludeLinesPattern;
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

    public String getTimestampField() {
        return timestampField;
    }

    public List<String> getJodaTimestampFormats() {
        return jodaTimestampFormats;
    }

    public List<String> getJavaTimestampFormats() {
        return javaTimestampFormats;
    }

    public boolean needClientTimezone() {
        return needClientTimezone;
    }

    public SortedMap<String, Object> getMappings() {
        return mappings;
    }

    public Map<String, Object> getIngestPipeline() {
        return ingestPipeline;
    }

    public SortedMap<String, FieldStats> getFieldStats() {
        return fieldStats;
    }

    public List<String> getExplanation() {
        return explanation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.field(NUM_LINES_ANALYZED.getPreferredName(), numLinesAnalyzed);
        builder.field(NUM_MESSAGES_ANALYZED.getPreferredName(), numMessagesAnalyzed);
        builder.field(SAMPLE_START.getPreferredName(), sampleStart);
        builder.field(CHARSET.getPreferredName(), charset);
        if (hasByteOrderMarker != null) {
            builder.field(HAS_BYTE_ORDER_MARKER.getPreferredName(), hasByteOrderMarker.booleanValue());
        }
        builder.field(FORMAT.getPreferredName(), format);
        if (multilineStartPattern != null && multilineStartPattern.isEmpty() == false) {
            builder.field(MULTILINE_START_PATTERN.getPreferredName(), multilineStartPattern);
        }
        if (excludeLinesPattern != null && excludeLinesPattern.isEmpty() == false) {
            builder.field(EXCLUDE_LINES_PATTERN.getPreferredName(), excludeLinesPattern);
        }
        if (columnNames != null && columnNames.isEmpty() == false) {
            builder.field(COLUMN_NAMES.getPreferredName(), columnNames);
        }
        if (hasHeaderRow != null) {
            builder.field(HAS_HEADER_ROW.getPreferredName(), hasHeaderRow.booleanValue());
        }
        if (delimiter != null) {
            builder.field(DELIMITER.getPreferredName(), String.valueOf(delimiter));
        }
        if (quote != null) {
            builder.field(QUOTE.getPreferredName(), String.valueOf(quote));
        }
        if (shouldTrimFields != null) {
            builder.field(SHOULD_TRIM_FIELDS.getPreferredName(), shouldTrimFields.booleanValue());
        }
        if (grokPattern != null && grokPattern.isEmpty() == false) {
            builder.field(GROK_PATTERN.getPreferredName(), grokPattern);
        }
        if (timestampField != null && timestampField.isEmpty() == false) {
            builder.field(TIMESTAMP_FIELD.getPreferredName(), timestampField);
        }
        if (jodaTimestampFormats != null && jodaTimestampFormats.isEmpty() == false) {
            builder.field(JODA_TIMESTAMP_FORMATS.getPreferredName(), jodaTimestampFormats);
        }
        if (javaTimestampFormats != null && javaTimestampFormats.isEmpty() == false) {
            builder.field(JAVA_TIMESTAMP_FORMATS.getPreferredName(), javaTimestampFormats);
        }
        builder.field(NEED_CLIENT_TIMEZONE.getPreferredName(), needClientTimezone);
        builder.field(MAPPINGS.getPreferredName(), mappings);
        if (ingestPipeline != null) {
            builder.field(INGEST_PIPELINE.getPreferredName(), ingestPipeline);
        }
        if (fieldStats.isEmpty() == false) {
            builder.startObject(FIELD_STATS.getPreferredName());
            for (Map.Entry<String, FieldStats> entry : fieldStats.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        if (explanation != null && explanation.isEmpty() == false) {
            builder.field(EXPLANATION.getPreferredName(), explanation);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public int hashCode() {

        return Objects.hash(numLinesAnalyzed, numMessagesAnalyzed, sampleStart, charset, hasByteOrderMarker, format,
            multilineStartPattern, excludeLinesPattern, columnNames, hasHeaderRow, delimiter, quote, shouldTrimFields, grokPattern,
            timestampField, jodaTimestampFormats, javaTimestampFormats, needClientTimezone, mappings, fieldStats, explanation);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FileStructure that = (FileStructure) other;
        return this.numLinesAnalyzed == that.numLinesAnalyzed &&
            this.numMessagesAnalyzed == that.numMessagesAnalyzed &&
            Objects.equals(this.sampleStart, that.sampleStart) &&
            Objects.equals(this.charset, that.charset) &&
            Objects.equals(this.hasByteOrderMarker, that.hasByteOrderMarker) &&
            Objects.equals(this.format, that.format) &&
            Objects.equals(this.multilineStartPattern, that.multilineStartPattern) &&
            Objects.equals(this.excludeLinesPattern, that.excludeLinesPattern) &&
            Objects.equals(this.columnNames, that.columnNames) &&
            Objects.equals(this.hasHeaderRow, that.hasHeaderRow) &&
            Objects.equals(this.delimiter, that.delimiter) &&
            Objects.equals(this.quote, that.quote) &&
            Objects.equals(this.shouldTrimFields, that.shouldTrimFields) &&
            Objects.equals(this.grokPattern, that.grokPattern) &&
            Objects.equals(this.timestampField, that.timestampField) &&
            Objects.equals(this.jodaTimestampFormats, that.jodaTimestampFormats) &&
            Objects.equals(this.javaTimestampFormats, that.javaTimestampFormats) &&
            this.needClientTimezone == that.needClientTimezone &&
            Objects.equals(this.mappings, that.mappings) &&
            Objects.equals(this.fieldStats, that.fieldStats) &&
            Objects.equals(this.explanation, that.explanation);
    }

    public static class Builder {

        private int numLinesAnalyzed;
        private int numMessagesAnalyzed;
        private String sampleStart;
        private String charset;
        private Boolean hasByteOrderMarker;
        private Format format;
        private String multilineStartPattern;
        private String excludeLinesPattern;
        private List<String> columnNames;
        private Boolean hasHeaderRow;
        private Character delimiter;
        private Character quote;
        private Boolean shouldTrimFields;
        private String grokPattern;
        private String timestampField;
        private List<String> jodaTimestampFormats;
        private List<String> javaTimestampFormats;
        private boolean needClientTimezone;
        private Map<String, Object> mappings = Collections.emptyMap();
        private Map<String, Object> ingestPipeline;
        private Map<String, FieldStats> fieldStats = Collections.emptyMap();
        private List<String> explanation;

        Builder() {
            this(Format.SEMI_STRUCTURED_TEXT);
        }

        Builder(Format format) {
            setFormat(format);
        }

        Builder setNumLinesAnalyzed(int numLinesAnalyzed) {
            this.numLinesAnalyzed = numLinesAnalyzed;
            return this;
        }

        Builder setNumMessagesAnalyzed(int numMessagesAnalyzed) {
            this.numMessagesAnalyzed = numMessagesAnalyzed;
            return this;
        }

        Builder setSampleStart(String sampleStart) {
            this.sampleStart = Objects.requireNonNull(sampleStart);
            return this;
        }

        Builder setCharset(String charset) {
            this.charset = Objects.requireNonNull(charset);
            return this;
        }

        Builder setHasByteOrderMarker(Boolean hasByteOrderMarker) {
            this.hasByteOrderMarker = hasByteOrderMarker;
            return this;
        }

        Builder setFormat(Format format) {
            this.format = Objects.requireNonNull(format);
            return this;
        }

        Builder setMultilineStartPattern(String multilineStartPattern) {
            this.multilineStartPattern = multilineStartPattern;
            return this;
        }

        Builder setExcludeLinesPattern(String excludeLinesPattern) {
            this.excludeLinesPattern = excludeLinesPattern;
            return this;
        }

        Builder setColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        Builder setHasHeaderRow(Boolean hasHeaderRow) {
            this.hasHeaderRow = hasHeaderRow;
            return this;
        }

        Builder setDelimiter(Character delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        Builder setQuote(Character quote) {
            this.quote = quote;
            return this;
        }

        Builder setShouldTrimFields(Boolean shouldTrimFields) {
            this.shouldTrimFields = shouldTrimFields;
            return this;
        }

        Builder setGrokPattern(String grokPattern) {
            this.grokPattern = grokPattern;
            return this;
        }

        Builder setTimestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        Builder setJodaTimestampFormats(List<String> jodaTimestampFormats) {
            this.jodaTimestampFormats = jodaTimestampFormats;
            return this;
        }

        Builder setJavaTimestampFormats(List<String> javaTimestampFormats) {
            this.javaTimestampFormats = javaTimestampFormats;
            return this;
        }

        Builder setNeedClientTimezone(boolean needClientTimezone) {
            this.needClientTimezone = needClientTimezone;
            return this;
        }

        Builder setMappings(Map<String, Object> mappings) {
            this.mappings = Objects.requireNonNull(mappings);
            return this;
        }

        Builder setIngestPipeline(Map<String, Object> ingestPipeline) {
            this.ingestPipeline = ingestPipeline;
            return this;
        }

        Builder setFieldStats(Map<String, FieldStats> fieldStats) {
            this.fieldStats = Objects.requireNonNull(fieldStats);
            return this;
        }

        Builder setExplanation(List<String> explanation) {
            this.explanation = explanation;
            return this;
        }

        public FileStructure build() {

            return new FileStructure(numLinesAnalyzed, numMessagesAnalyzed, sampleStart, charset, hasByteOrderMarker, format,
                multilineStartPattern, excludeLinesPattern, columnNames, hasHeaderRow, delimiter, quote, shouldTrimFields, grokPattern,
                timestampField, jodaTimestampFormats, javaTimestampFormats, needClientTimezone, mappings, ingestPipeline, fieldStats,
                explanation);
        }
    }
}
