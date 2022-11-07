/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class Downsample implements ToXContentObject, Writeable {

    public static final String NAME = "downsample";
    public static final ParseField RANGE_FIELD = new ParseField("range");
    public static final ParseField FUNCTION_FIELD = new ParseField("function");
    public static final ParseField PARAMS_FIELD = new ParseField("params");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Downsample, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args, name) -> new Downsample((DateHistogramInterval) args[0], (Function) args[1], (Map<String, Object>) args[2])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> new DateHistogramInterval(p.text()),
            RANGE_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> Function.resolve(p.text()),
            FUNCTION_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (parser, c) -> parser.map(), PARAMS_FIELD);
    }

    private final DateHistogramInterval range;
    private final Function function;
    private final Map<String, Object> parameters;

    public Downsample(DateHistogramInterval range, Function function, Map<String, Object> parameters) {
        this.range = range;
        this.function = function;
        this.parameters = parameters;
    }

    public Downsample(StreamInput in) throws IOException {
        this.range = in.readOptionalWriteable(DateHistogramInterval::new);
        this.function = in.readOptionalEnum(Function.class);
        this.parameters = in.readMap();
    }

    public DateHistogramInterval getRange() {
        return range;
    }

    public Function getFunction() {
        return function;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public static Downsample fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (function != null) {
            builder.field(FUNCTION_FIELD.getPreferredName(), function);
        }
        if (range != null) {
            builder.field(RANGE_FIELD.getPreferredName(), range);
        }
        if (parameters != null) {
            builder.field(PARAMS_FIELD.getPreferredName(), parameters);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(range);
        out.writeOptionalEnum(function);
        out.writeGenericMap(parameters);
    }
}
