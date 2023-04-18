/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class CompositeValuesSourceParserHelper {

    static <VB extends CompositeValuesSourceBuilder<VB>, T> void declareValuesSourceFields(AbstractObjectParser<VB, T> objectParser) {
        objectParser.declareField(VB::field, XContentParser::text, new ParseField("field"), ObjectParser.ValueType.STRING);
        objectParser.declareBoolean(VB::missingBucket, new ParseField("missing_bucket"));
        objectParser.declareString(VB::missingOrder, new ParseField("missing_order"));

        objectParser.declareField(VB::userValuetypeHint, p -> {
            ValueType valueType = ValueType.lenientParse(p.text());
            return valueType;
        }, new ParseField("value_type"), ObjectParser.ValueType.STRING);

        objectParser.declareField(
            VB::script,
            (parser, context) -> Script.parse(parser),
            Script.SCRIPT_PARSE_FIELD,
            ObjectParser.ValueType.OBJECT_OR_STRING
        );

        objectParser.declareField(VB::order, XContentParser::text, new ParseField("order"), ObjectParser.ValueType.STRING);
    }

    public static void writeTo(CompositeValuesSourceBuilder<?> builder, StreamOutput out) throws IOException {
        final byte code;
        if (builder.getClass() == TermsValuesSourceBuilder.class) {
            code = 0;
        } else if (builder.getClass() == DateHistogramValuesSourceBuilder.class) {
            code = 1;
        } else if (builder.getClass() == HistogramValuesSourceBuilder.class) {
            code = 2;
        } else if (builder.getClass() == GeoTileGridValuesSourceBuilder.class) {
            if (out.getTransportVersion().before(TransportVersion.V_7_5_0)) {
                throw new IOException(
                    "Attempting to serialize ["
                        + builder.getClass().getSimpleName()
                        + "] to a stream with unsupported version ["
                        + out.getTransportVersion()
                        + "]"
                );
            }
            code = 3;
        } else {
            throw new IOException("invalid builder type: " + builder.getClass().getSimpleName());
        }
        out.writeByte(code);
        builder.writeTo(out);
    }

    public static CompositeValuesSourceBuilder<?> readFrom(StreamInput in) throws IOException {
        int code = in.readByte();
        return switch (code) {
            case 0 -> new TermsValuesSourceBuilder(in);
            case 1 -> new DateHistogramValuesSourceBuilder(in);
            case 2 -> new HistogramValuesSourceBuilder(in);
            case 3 -> new GeoTileGridValuesSourceBuilder(in);
            default -> throw new IOException("Invalid code " + code);
        };
    }

    public static CompositeValuesSourceBuilder<?> fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String name = parser.currentName();
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String type = parser.currentName();
        token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        final CompositeValuesSourceBuilder<?> builder = switch (type) {
            case TermsValuesSourceBuilder.TYPE -> TermsValuesSourceBuilder.parse(name, parser);
            case DateHistogramValuesSourceBuilder.TYPE -> DateHistogramValuesSourceBuilder.PARSER.parse(parser, name);
            case HistogramValuesSourceBuilder.TYPE -> HistogramValuesSourceBuilder.parse(name, parser);
            case GeoTileGridValuesSourceBuilder.TYPE -> GeoTileGridValuesSourceBuilder.parse(name, parser);
            default -> throw new ParsingException(parser.getTokenLocation(), "invalid source type: " + type);
        };
        parser.nextToken();
        parser.nextToken();
        return builder;
    }

    public static XContentBuilder toXContent(CompositeValuesSourceBuilder<?> source, XContentBuilder builder, Params params)
        throws IOException {
        builder.startObject();
        builder.startObject(source.name());
        source.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
