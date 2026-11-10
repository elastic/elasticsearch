/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Casts the keyword {@code _timeseries} - the source of a series' dimensions - to the canonical label container. Dimensions
 * under objects are flattened to their dotted field names and named as PromQL names labels; values keep their source text.
 * <p>
 * Internal: only PromQL translation builds it, so it has no ES|QL function name.
 */
public final class ToTimeSeriesMetadata extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToTimeSeriesMetadata",
        ToTimeSeriesMetadata::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.of(
        KEYWORD,
        (source, field) -> new ToTimeSeriesMetadataFromKeywordEvaluator.Factory(
            source,
            field,
            context -> new BreakingBytesRefBuilder(context.breaker(), "to_timeseries")
        )
    );

    public ToTimeSeriesMetadata(Source source, Expression field) {
        super(source, field);
    }

    private ToTimeSeriesMetadata(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return DataType.TSMD_DATA_TYPE;
    }

    /** The source form of a series' dimensions, encoded as a typed label container in the breaker-accounted {@code scratch}. */
    @ConvertEvaluator(extraName = "FromKeyword")
    static BytesRef fromKeyword(BytesRef in, @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch) {
        scratch.clear();
        try (XContentBuilder builder = TimeSeriesMetadataFunctionEvaluator.jsonBuilder(scratch)) {
            builder.startObject();
            for (var label : readSource(in).entrySet()) {
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
     * as a string and several as an array. Equal label sets get equal bytes, so the label container can key joins and groups.
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

    @Override
    public ToTimeSeriesMetadata replaceChildren(List<Expression> newChildren) {
        return new ToTimeSeriesMetadata(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<ToTimeSeriesMetadata> info() {
        return NodeInfo.create(this, ToTimeSeriesMetadata::new, field());
    }
}
