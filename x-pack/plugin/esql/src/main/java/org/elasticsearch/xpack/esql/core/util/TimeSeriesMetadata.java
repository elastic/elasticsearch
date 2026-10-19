/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An immutable set of time-series labels. Each label contains one or more string values; null and empty
 * values are omitted. Value order and duplicates are preserved, as they are by the ingest TSID builder.
 *
 * <p>The encoded representation is private to this class: readers and scalar functions use the same
 * normalization and serialization so equivalent label sets have the same representation, irrespective of their source
 * or the order in which labels were supplied. Source field names and mapped values must be normalized
 * before constructing metadata.
 */
public final class TimeSeriesMetadata {
    public static final TimeSeriesMetadata EMPTY = new TimeSeriesMetadata(Collections.emptySortedMap());

    private final SortedMap<String, List<String>> labels;

    private TimeSeriesMetadata(SortedMap<String, List<String>> labels) {
        this.labels = Collections.unmodifiableSortedMap(labels);
    }

    /** Fixed encoding workspace, excluding the labels and caller-owned output storage. */
    public static long encodingScratchBytes() {
        // The JSON encoder uses an 8 KB byte buffer and a 4 K char buffer, plus its generator state.
        return 24 * 1024;
    }

    /** Copies already normalized labels, omitting null and empty values. */
    public static TimeSeriesMetadata of(Map<String, String> labels) {
        return EMPTY.withLabels(labels);
    }

    /** Copies all label values, preserving their order and omitting null and empty members. */
    public static TimeSeriesMetadata ofValues(Map<String, ? extends List<String>> labels) {
        return EMPTY.withLabelValues(labels);
    }

    /**
     * Reads exactly the supplied byte slice. A null value represents empty metadata; malformed values,
     * duplicate labels and trailing content are rejected rather than silently changing series identity.
     */
    public static TimeSeriesMetadata read(@Nullable BytesRef bytes) throws IOException {
        if (bytes == null) {
            return EMPTY;
        }
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                bytes.bytes,
                bytes.offset,
                bytes.length
            )
        ) {
            parser.allowDuplicateKeys(false);
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(parser.getTokenLocation(), "time-series metadata must be an object");
            }
            var labels = new TreeMap<String, List<String>>();
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new XContentParseException(parser.getTokenLocation(), "expected a time-series label name");
                }
                String name = parser.currentName();
                validateName(name);
                token = parser.nextToken();
                var values = new ArrayList<String>();
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        readValue(parser, token, name, values);
                    }
                } else {
                    readValue(parser, token, name, values);
                }
                if (values.isEmpty() == false) {
                    labels.put(name, List.copyOf(values));
                }
            }
            if (parser.nextToken() != null) {
                throw new XContentParseException(parser.getTokenLocation(), "unexpected content after time-series metadata");
            }
            return labels.isEmpty() ? EMPTY : new TimeSeriesMetadata(labels);
        }
    }

    private static void readValue(XContentParser parser, XContentParser.Token token, String name, List<String> values) throws IOException {
        if (token == XContentParser.Token.VALUE_STRING) {
            String value = parser.text();
            if (value.isEmpty() == false) {
                values.add(value);
            }
        } else if (token != XContentParser.Token.VALUE_NULL) {
            throw new XContentParseException(parser.getTokenLocation(), "time-series label [" + name + "] must contain strings");
        }
    }

    /** Returns an immutable list, or an empty list when the label is absent. */
    public List<String> get(String name) {
        validateName(name);
        return labels.getOrDefault(name, List.of());
    }

    /** Replaces a label, or removes it when the supplied value is null or empty. */
    public TimeSeriesMetadata set(String name, @Nullable String value) {
        return withLabels(Collections.singletonMap(name, value));
    }

    /** Applies several replacements together without copying the label set for every replacement. */
    public TimeSeriesMetadata withLabels(Map<String, String> values) {
        var replacements = new HashMap<String, List<String>>();
        values.forEach((name, value) -> replacements.put(name, value == null ? List.of() : List.of(value)));
        return withLabelValues(replacements);
    }

    /** Replaces complete label value lists, defensively copying them without changing existing metadata. */
    public TimeSeriesMetadata withLabelValues(Map<String, ? extends List<String>> values) {
        if (values.isEmpty()) {
            return this;
        }
        var result = new TreeMap<>(labels);
        for (var entry : values.entrySet()) {
            String name = entry.getKey();
            validateName(name);
            var normalized = new ArrayList<String>();
            if (entry.getValue() != null) {
                for (String value : entry.getValue()) {
                    if (value != null && value.isEmpty() == false) {
                        normalized.add(value);
                    }
                }
            }
            if (normalized.isEmpty()) {
                result.remove(name);
            } else {
                result.put(name, List.copyOf(normalized));
            }
        }
        return result.isEmpty() ? EMPTY : new TimeSeriesMetadata(result);
    }

    /** Removes the named labels; removing a label that is already absent has no effect. */
    public TimeSeriesMetadata unset(Collection<String> names) {
        if (names.isEmpty()) {
            return this;
        }
        var result = new TreeMap<>(labels);
        for (String name : names) {
            validateName(name);
            result.remove(name);
        }
        return result.isEmpty() ? EMPTY : new TimeSeriesMetadata(result);
    }

    /**
     * Writes opaque metadata, using the same encoding for equivalent normalized label sets. Callers must
     * not interpret this encoding; use {@link #writeKeyTo(OutputStream)} for a series fingerprint.
     * The caller supplies circuit-breaker-accounted storage; the stream is flushed but never closed.
     */
    public void writeTo(OutputStream output) throws IOException {
        writeJson(output);
    }

    /**
     * Writes a 128-bit series fingerprint using the ingest TSID hash over normalized string labels.
     * This is independent of both the metadata encoding and the index-version-specific stored TSID layout.
     * Identity comparisons rely on the hash, as they do for stored series identifiers.
     * The caller supplies circuit-breaker-accounted storage; the stream is flushed but never closed.
     */
    public void writeKeyTo(OutputStream output) throws IOException {
        var builder = new TsidBuilder(labels.size());
        labels.forEach((name, values) -> values.forEach(value -> builder.addStringDimension(name, value)));
        output.write(builder.hash().getBytes());
        output.flush();
    }

    /**
     * Renders the label set as a JSON object for display. This remains separate from the opaque encoded
     * representation so its format can evolve independently. The stream is flushed but never closed.
     */
    public void renderTo(OutputStream output) throws IOException {
        writeJson(output);
    }

    private void writeJson(OutputStream output) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder(Streams.noCloseStream(output))) {
            builder.startObject();
            for (var label : labels.entrySet()) {
                List<String> values = label.getValue();
                if (values.size() == 1) {
                    builder.field(label.getKey(), values.getFirst());
                } else {
                    builder.field(label.getKey(), values);
                }
            }
            builder.endObject();
            builder.flush();
        }
    }

    private static void validateName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("time-series label names must be non-null and non-empty");
        }
    }
}
