/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.structurefinder;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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
 * Stores the determined file format.
 */
public class TextStructure implements ToXContentObject, Writeable {

    public enum Format {

        NDJSON,
        XML,
        DELIMITED,
        SEMI_STRUCTURED_TEXT;

        public boolean supportsNesting() {
            return switch (this) {
                case NDJSON, XML -> true;
                case DELIMITED, SEMI_STRUCTURED_TEXT -> false;
            };
        }

        public boolean isStructured() {
            return switch (this) {
                case NDJSON, XML, DELIMITED -> true;
                case SEMI_STRUCTURED_TEXT -> false;
            };
        }

        public boolean isSemiStructured() {
            return switch (this) {
                case NDJSON, XML, DELIMITED -> false;
                case SEMI_STRUCTURED_TEXT -> true;
            };
        }

        public static Format fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final String EXPLAIN = "explain";

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
    public static final ParseField ECS_COMPATIBILITY = new ParseField("ecs_compatibility");
    public static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp_field");
    public static final ParseField JODA_TIMESTAMP_FORMATS = new ParseField("joda_timestamp_formats");
    public static final ParseField JAVA_TIMESTAMP_FORMATS = new ParseField("java_timestamp_formats");
    public static final ParseField NEED_CLIENT_TIMEZONE = new ParseField("need_client_timezone");
    public static final ParseField MAPPINGS = new ParseField("mappings");
    public static final ParseField INGEST_PIPELINE = new ParseField("ingest_pipeline");
    public static final ParseField FIELD_STATS = new ParseField("field_stats");
    public static final ParseField EXPLANATION = new ParseField("explanation");

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("file_structure", false, Builder::new);

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
        PARSER.declareString(Builder::setEcsCompatibility, ECS_COMPATIBILITY);
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

    private static String getNonNullEcsCompatibilityString(String ecsCompatibility) {
        return (ecsCompatibility == null || ecsCompatibility.isEmpty()) ? Grok.ECS_COMPATIBILITY_MODES[0] : ecsCompatibility;
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
    private final String ecsCompatibility;
    private final List<String> jodaTimestampFormats;
    private final List<String> javaTimestampFormats;
    private final String timestampField;
    private final boolean needClientTimezone;
    private final SortedMap<String, Object> mappings;
    private final Map<String, Object> ingestPipeline;
    private final SortedMap<String, FieldStats> fieldStats;
    private final List<String> explanation;

    public TextStructure(
        int numLinesAnalyzed,
        int numMessagesAnalyzed,
        String sampleStart,
        String charset,
        Boolean hasByteOrderMarker,
        Format format,
        String multilineStartPattern,
        String excludeLinesPattern,
        List<String> columnNames,
        Boolean hasHeaderRow,
        Character delimiter,
        Character quote,
        Boolean shouldTrimFields,
        String grokPattern,
        String ecsCompatibility,
        String timestampField,
        List<String> jodaTimestampFormats,
        List<String> javaTimestampFormats,
        boolean needClientTimezone,
        Map<String, Object> mappings,
        Map<String, Object> ingestPipeline,
        Map<String, FieldStats> fieldStats,
        List<String> explanation
    ) {

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
        this.ecsCompatibility = getNonNullEcsCompatibilityString(ecsCompatibility);
        this.timestampField = timestampField;
        this.jodaTimestampFormats = (jodaTimestampFormats == null) ? null : List.copyOf(jodaTimestampFormats);
        this.javaTimestampFormats = (javaTimestampFormats == null) ? null : List.copyOf(javaTimestampFormats);
        this.needClientTimezone = needClientTimezone;
        this.mappings = Collections.unmodifiableSortedMap(new TreeMap<>(mappings));
        this.ingestPipeline = (ingestPipeline == null) ? null : Collections.unmodifiableMap(new LinkedHashMap<>(ingestPipeline));
        this.fieldStats = Collections.unmodifiableSortedMap(new TreeMap<>(fieldStats));
        this.explanation = List.copyOf(explanation);
    }

    public TextStructure(StreamInput in) throws IOException {
        numLinesAnalyzed = in.readVInt();
        numMessagesAnalyzed = in.readVInt();
        sampleStart = in.readString();
        charset = in.readString();
        hasByteOrderMarker = in.readOptionalBoolean();
        format = in.readEnum(Format.class);
        multilineStartPattern = in.readOptionalString();
        excludeLinesPattern = in.readOptionalString();
        columnNames = in.readBoolean() ? in.readImmutableList(StreamInput::readString) : null;
        hasHeaderRow = in.readOptionalBoolean();
        delimiter = in.readBoolean() ? (char) in.readVInt() : null;
        quote = in.readBoolean() ? (char) in.readVInt() : null;
        shouldTrimFields = in.readOptionalBoolean();
        grokPattern = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_5_0)) {
            ecsCompatibility = getNonNullEcsCompatibilityString(in.readString());
        } else {
            ecsCompatibility = getNonNullEcsCompatibilityString(null);
        }
        jodaTimestampFormats = in.readBoolean() ? in.readImmutableList(StreamInput::readString) : null;
        javaTimestampFormats = in.readBoolean() ? in.readImmutableList(StreamInput::readString) : null;
        timestampField = in.readOptionalString();
        needClientTimezone = in.readBoolean();
        mappings = Collections.unmodifiableSortedMap(new TreeMap<>(in.readMap()));
        ingestPipeline = in.readBoolean() ? Collections.unmodifiableMap(in.readMap()) : null;
        fieldStats = Collections.unmodifiableSortedMap(new TreeMap<>(in.readMap(StreamInput::readString, FieldStats::new)));
        explanation = in.readImmutableList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numLinesAnalyzed);
        out.writeVInt(numMessagesAnalyzed);
        out.writeString(sampleStart);
        out.writeString(charset);
        out.writeOptionalBoolean(hasByteOrderMarker);
        out.writeEnum(format);
        out.writeOptionalString(multilineStartPattern);
        out.writeOptionalString(excludeLinesPattern);
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
            out.writeString(ecsCompatibility);
        }
        if (jodaTimestampFormats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeCollection(jodaTimestampFormats, StreamOutput::writeString);
        }
        if (javaTimestampFormats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeCollection(javaTimestampFormats, StreamOutput::writeString);
        }
        out.writeOptionalString(timestampField);
        out.writeBoolean(needClientTimezone);
        out.writeGenericMap(mappings);
        if (ingestPipeline == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeGenericMap(ingestPipeline);
        }
        out.writeMap(fieldStats, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        out.writeCollection(explanation, StreamOutput::writeString);
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

    public String getEcsCompatibility() {
        return ecsCompatibility;
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
        builder.field(ECS_COMPATIBILITY.getPreferredName(), ecsCompatibility);
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
        if (params.paramAsBoolean(EXPLAIN, false)) {
            builder.field(EXPLANATION.getPreferredName(), explanation);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public int hashCode() {

        return Objects.hash(
            numLinesAnalyzed,
            numMessagesAnalyzed,
            sampleStart,
            charset,
            hasByteOrderMarker,
            format,
            multilineStartPattern,
            excludeLinesPattern,
            columnNames,
            hasHeaderRow,
            delimiter,
            quote,
            shouldTrimFields,
            grokPattern,
            ecsCompatibility,
            timestampField,
            jodaTimestampFormats,
            javaTimestampFormats,
            needClientTimezone,
            mappings,
            fieldStats,
            explanation
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

        TextStructure that = (TextStructure) other;
        return this.numLinesAnalyzed == that.numLinesAnalyzed
            && this.numMessagesAnalyzed == that.numMessagesAnalyzed
            && Objects.equals(this.sampleStart, that.sampleStart)
            && Objects.equals(this.charset, that.charset)
            && Objects.equals(this.hasByteOrderMarker, that.hasByteOrderMarker)
            && Objects.equals(this.format, that.format)
            && Objects.equals(this.multilineStartPattern, that.multilineStartPattern)
            && Objects.equals(this.excludeLinesPattern, that.excludeLinesPattern)
            && Objects.equals(this.columnNames, that.columnNames)
            && Objects.equals(this.hasHeaderRow, that.hasHeaderRow)
            && Objects.equals(this.delimiter, that.delimiter)
            && Objects.equals(this.quote, that.quote)
            && Objects.equals(this.shouldTrimFields, that.shouldTrimFields)
            && Objects.equals(this.grokPattern, that.grokPattern)
            && Objects.equals(this.ecsCompatibility, that.ecsCompatibility)
            && Objects.equals(this.timestampField, that.timestampField)
            && Objects.equals(this.jodaTimestampFormats, that.jodaTimestampFormats)
            && Objects.equals(this.javaTimestampFormats, that.javaTimestampFormats)
            && this.needClientTimezone == that.needClientTimezone
            && Objects.equals(this.mappings, that.mappings)
            && Objects.equals(this.fieldStats, that.fieldStats)
            && Objects.equals(this.explanation, that.explanation);
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
        private String ecsCompatibility;
        private String timestampField;
        private List<String> jodaTimestampFormats;
        private List<String> javaTimestampFormats;
        private boolean needClientTimezone;
        private Map<String, Object> mappings;
        private Map<String, Object> ingestPipeline;
        private Map<String, FieldStats> fieldStats = Collections.emptyMap();
        private List<String> explanation;

        public Builder() {
            this(Format.SEMI_STRUCTURED_TEXT);
        }

        public Builder(Format format) {
            setFormat(format);
        }

        public Builder setNumLinesAnalyzed(int numLinesAnalyzed) {
            this.numLinesAnalyzed = numLinesAnalyzed;
            return this;
        }

        public Builder setNumMessagesAnalyzed(int numMessagesAnalyzed) {
            this.numMessagesAnalyzed = numMessagesAnalyzed;
            return this;
        }

        public Builder setSampleStart(String sampleStart) {
            this.sampleStart = Objects.requireNonNull(sampleStart);
            return this;
        }

        public Builder setCharset(String charset) {
            this.charset = Objects.requireNonNull(charset);
            return this;
        }

        public Builder setHasByteOrderMarker(Boolean hasByteOrderMarker) {
            this.hasByteOrderMarker = hasByteOrderMarker;
            return this;
        }

        public Builder setFormat(Format format) {
            this.format = Objects.requireNonNull(format);
            return this;
        }

        public Builder setMultilineStartPattern(String multilineStartPattern) {
            this.multilineStartPattern = multilineStartPattern;
            return this;
        }

        public Builder setExcludeLinesPattern(String excludeLinesPattern) {
            this.excludeLinesPattern = excludeLinesPattern;
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

        public Builder setEcsCompatibility(String ecsCompatibility) {
            this.ecsCompatibility = ecsCompatibility;
            return this;
        }

        public Builder setTimestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        public Builder setJodaTimestampFormats(List<String> jodaTimestampFormats) {
            this.jodaTimestampFormats = jodaTimestampFormats;
            return this;
        }

        public Builder setJavaTimestampFormats(List<String> javaTimestampFormats) {
            this.javaTimestampFormats = javaTimestampFormats;
            return this;
        }

        public Builder setNeedClientTimezone(boolean needClientTimezone) {
            this.needClientTimezone = needClientTimezone;
            return this;
        }

        public Builder setMappings(Map<String, Object> mappings) {
            this.mappings = Objects.requireNonNull(mappings);
            return this;
        }

        public Builder setIngestPipeline(Map<String, Object> ingestPipeline) {
            this.ingestPipeline = ingestPipeline;
            return this;
        }

        public Builder setFieldStats(Map<String, FieldStats> fieldStats) {
            this.fieldStats = Objects.requireNonNull(fieldStats);
            return this;
        }

        public Builder setExplanation(List<String> explanation) {
            this.explanation = Objects.requireNonNull(explanation);
            return this;
        }

        @SuppressWarnings("fallthrough")
        public TextStructure build() {

            if (numLinesAnalyzed <= 0) {
                throw new IllegalArgumentException("Number of lines analyzed must be positive.");
            }

            if (numMessagesAnalyzed <= 0) {
                throw new IllegalArgumentException("Number of messages analyzed must be positive.");
            }

            if (numMessagesAnalyzed > numLinesAnalyzed) {
                throw new IllegalArgumentException("Number of messages analyzed cannot be greater than number of lines analyzed.");
            }

            if (sampleStart == null || sampleStart.isEmpty()) {
                throw new IllegalArgumentException("Sample start must be specified.");
            }

            if (charset == null || charset.isEmpty()) {
                throw new IllegalArgumentException("A character set must be specified.");
            }

            if (charset.toUpperCase(Locale.ROOT).startsWith("UTF") == false && hasByteOrderMarker != null) {
                throw new IllegalArgumentException("A byte order marker is only possible for UTF character sets.");
            }

            switch (format) {
                case NDJSON:
                    if (shouldTrimFields != null) {
                        throw new IllegalArgumentException("Should trim fields may not be specified for [" + format + "] structures.");
                    }
                    // $FALL-THROUGH$
                case XML:
                    if (columnNames != null) {
                        throw new IllegalArgumentException("Column names may not be specified for [" + format + "] structures.");
                    }
                    if (hasHeaderRow != null) {
                        throw new IllegalArgumentException("Has header row may not be specified for [" + format + "] structures.");
                    }
                    if (delimiter != null) {
                        throw new IllegalArgumentException("Delimiter may not be specified for [" + format + "] structures.");
                    }
                    if (quote != null) {
                        throw new IllegalArgumentException("Quote may not be specified for [" + format + "] structures.");
                    }
                    if (grokPattern != null) {
                        throw new IllegalArgumentException("Grok pattern may not be specified for [" + format + "] structures.");
                    }
                    break;
                case DELIMITED:
                    if (columnNames == null || columnNames.isEmpty()) {
                        throw new IllegalArgumentException("Column names must be specified for [" + format + "] structures.");
                    }
                    if (hasHeaderRow == null) {
                        throw new IllegalArgumentException("Has header row must be specified for [" + format + "] structures.");
                    }
                    if (delimiter == null) {
                        throw new IllegalArgumentException("Delimiter must be specified for [" + format + "] structures.");
                    }
                    if (grokPattern != null) {
                        throw new IllegalArgumentException("Grok pattern may not be specified for [" + format + "] structures.");
                    }
                    break;
                case SEMI_STRUCTURED_TEXT:
                    if (columnNames != null) {
                        throw new IllegalArgumentException("Column names may not be specified for [" + format + "] structures.");
                    }
                    if (hasHeaderRow != null) {
                        throw new IllegalArgumentException("Has header row may not be specified for [" + format + "] structures.");
                    }
                    if (delimiter != null) {
                        throw new IllegalArgumentException("Delimiter may not be specified for [" + format + "] structures.");
                    }
                    if (quote != null) {
                        throw new IllegalArgumentException("Quote may not be specified for [" + format + "] structures.");
                    }
                    if (shouldTrimFields != null) {
                        throw new IllegalArgumentException("Should trim fields may not be specified for [" + format + "] structures.");
                    }
                    if (grokPattern == null || grokPattern.isEmpty()) {
                        throw new IllegalArgumentException("Grok pattern must be specified for [" + format + "] structures.");
                    }
                    if (ecsCompatibility != null
                        && ecsCompatibility.isEmpty() == false
                        && Grok.isValidEcsCompatibilityMode(ecsCompatibility) == false) {
                        throw new IllegalArgumentException(
                            ECS_COMPATIBILITY.getPreferredName()
                                + "] must be one of ["
                                + String.join(", ", Grok.ECS_COMPATIBILITY_MODES)
                                + "] if specified"
                        );
                    }
                    break;
                default:
                    throw new IllegalStateException("enum value [" + format + "] missing from switch.");
            }

            boolean isTimestampFieldSpecified = (timestampField != null);
            boolean isJodaTimestampFormatsSpecified = (jodaTimestampFormats != null && jodaTimestampFormats.isEmpty() == false);
            boolean isJavaTimestampFormatsSpecified = (javaTimestampFormats != null && javaTimestampFormats.isEmpty() == false);

            if (isTimestampFieldSpecified != isJodaTimestampFormatsSpecified) {
                throw new IllegalArgumentException(
                    "Timestamp field and Joda timestamp formats must both be specified or neither be specified."
                );
            }

            if (isTimestampFieldSpecified != isJavaTimestampFormatsSpecified) {
                throw new IllegalArgumentException(
                    "Timestamp field and Java timestamp formats must both be specified or neither be specified."
                );
            }

            if (needClientTimezone && isTimestampFieldSpecified == false) {
                throw new IllegalArgumentException("Client timezone cannot be needed if there is no timestamp field.");
            }

            if (mappings == null || mappings.isEmpty()) {
                throw new IllegalArgumentException("Mappings must be specified.");
            }

            if (explanation == null || explanation.isEmpty()) {
                throw new IllegalArgumentException("Explanation must be specified.");
            }

            return new TextStructure(
                numLinesAnalyzed,
                numMessagesAnalyzed,
                sampleStart,
                charset,
                hasByteOrderMarker,
                format,
                multilineStartPattern,
                excludeLinesPattern,
                columnNames,
                hasHeaderRow,
                delimiter,
                quote,
                shouldTrimFields,
                grokPattern,
                ecsCompatibility,
                timestampField,
                jodaTimestampFormats,
                javaTimestampFormats,
                needClientTimezone,
                mappings,
                ingestPipeline,
                fieldStats,
                explanation
            );
        }
    }
}
