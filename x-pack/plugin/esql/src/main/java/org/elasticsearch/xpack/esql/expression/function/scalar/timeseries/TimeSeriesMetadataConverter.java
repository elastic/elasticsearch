/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Loads {@code _timeseries} as a typed label container. The field loads as the JSON object of a series' dimensions, as it does
 * for a keyword column; this converts each value, as it is read, to the canonical form every label container function reads:
 * labels named as PromQL names them, sorted by name, each holding its non-empty values. Equal label sets get equal bytes, so
 * the label container can key joins and groups.
 * <p>
 * TODO: an empty value is left out, so a series whose label is empty and one that lacks the label have the same label
 * container, and a time-series aggregate keyed by it merges their samples. Prometheus has no empty label, so data written by
 * it never holds such a pair; data written otherwise may, and answers then differ from what each series alone gives. Not
 * supported for now.
 */
public final class TimeSeriesMetadataConverter implements ValuesSourceReaderOperator.ConverterFactory {
    public static final TimeSeriesMetadataConverter INSTANCE = new TimeSeriesMetadataConverter();

    private TimeSeriesMetadataConverter() {}

    @Override
    public ValuesSourceReaderOperator.ConverterEvaluator build(DriverContext context) {
        BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(context.breaker(), "load_timeseries");
        return new ValuesSourceReaderOperator.ConverterEvaluator() {
            @Override
            public Block convert(Block block) {
                BytesRefBlock source = (BytesRefBlock) block;
                BytesRef value = new BytesRef();
                try (BytesRefBlock.Builder result = context.blockFactory().newBytesRefBlockBuilder(source.getPositionCount())) {
                    for (int p = 0; p < source.getPositionCount(); p++) {
                        if (source.isNull(p)) {
                            result.appendNull();
                        } else {
                            result.appendBytesRef(canonical(source.getBytesRef(source.getFirstValueIndex(p), value), scratch));
                        }
                    }
                    return result.build();
                }
            }

            @Override
            public void close() {
                Releasables.close(scratch);
            }

            @Override
            public String toString() {
                return "TimeSeriesMetadataConverter";
            }
        };
    }

    /** The label container of the JSON object {@code source} of a series' dimensions, written to {@code scratch}. */
    static BytesRef canonical(BytesRef source, BreakingBytesRefBuilder scratch) {
        scratch.clear();
        try (XContentBuilder builder = TimeSeriesMetadataFunctionEvaluator.jsonBuilder(scratch)) {
            builder.startObject();
            for (var label : readSource(source).entrySet()) {
                List<String> values = label.getValue();
                if (values.size() == 1) {
                    builder.field(label.getKey(), values.getFirst());
                } else {
                    builder.field(label.getKey(), values);
                }
            }
            builder.endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid label container", e);
        }
        return scratch.bytesRefView();
    }

    /**
     * The canonical form: labels sorted by name, each holding its non-empty values in source order, one value written
     * as a string and several as an array.
     */
    private static SortedMap<String, List<String>> readSource(BytesRef source) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                source.bytes,
                source.offset,
                source.length
            )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(parser.getTokenLocation(), "time-series source must be an object");
            }
            var labels = new TreeMap<String, List<String>>();
            readSourceObject(parser, "", labels);
            if (parser.nextToken() != null) {
                throw new XContentParseException(parser.getTokenLocation(), "unexpected content after time-series source");
            }
            return labels;
        }
    }

    private static void readSourceObject(XContentParser parser, String prefix, Map<String, List<String>> labels) throws IOException {
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            String field = prefix + parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                readSourceObject(parser, field + ".", labels);
                continue;
            }
            var values = new ArrayList<String>();
            if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    readSourceValue(parser, token, field, values);
                }
            } else {
                readSourceValue(parser, token, field, values);
            }
            String label = PromqlLabels.labelName(field);
            if (label.isEmpty()) {
                throw new XContentParseException(parser.getTokenLocation(), "time-series label names must be non-empty");
            }
            if (labels.containsKey(label)) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "multiple dimension fields resolve to time-series label [" + label + "]"
                );
            }
            if (values.isEmpty() == false) {
                labels.put(label, values);
            }
        }
    }

    /** Scalars keep their source text; null and empty values are left out. */
    private static void readSourceValue(XContentParser parser, XContentParser.Token token, String field, List<String> values)
        throws IOException {
        if (token.isValue()) {
            String value = parser.text();
            if (value.isEmpty() == false) {
                values.add(value);
            }
        } else if (token != XContentParser.Token.VALUE_NULL) {
            throw new XContentParseException(parser.getTokenLocation(), "time-series dimension [" + field + "] must contain values");
        }
    }
}
