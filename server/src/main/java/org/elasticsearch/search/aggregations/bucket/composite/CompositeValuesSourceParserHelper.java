/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class CompositeValuesSourceParserHelper {

    private static ParseField MISSING_PARSE_FIELD = new ParseField("missing");
    private static ParseField MISSING_BUCKET_PARSE_FIELD = new ParseField("missing_bucket");

    static <VB extends CompositeValuesSourceBuilder<VB>, T> void declareValuesSourceFields(AbstractObjectParser<VB, T> objectParser) {
        objectParser.declareField(VB::field, XContentParser::text,
            new ParseField("field"), ObjectParser.ValueType.STRING);

        objectParser.declareExclusiveFieldSet(MISSING_BUCKET_PARSE_FIELD.getPreferredName(), MISSING_PARSE_FIELD.getPreferredName());
        objectParser.declareBoolean(VB::missingBucket, MISSING_BUCKET_PARSE_FIELD);
        objectParser.declareString(VB::missing, MISSING_PARSE_FIELD);

        objectParser.declareField(VB::userValuetypeHint, p -> {
            ValueType valueType = ValueType.lenientParse(p.text());
            return valueType;
        }, new ParseField("value_type"), ObjectParser.ValueType.STRING);

        objectParser.declareField(VB::script,
            (parser, context) -> Script.parse(parser), Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);

        objectParser.declareField(VB::order,  XContentParser::text, new ParseField("order"), ObjectParser.ValueType.STRING);
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
            if (out.getVersion().before(Version.V_7_5_0)) {
                throw new IOException("Attempting to serialize [" + builder.getClass().getSimpleName()
                    + "] to a node with unsupported version [" + out.getVersion() + "]");
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
        switch(code) {
            case 0:
                return new TermsValuesSourceBuilder(in);
            case 1:
                return new DateHistogramValuesSourceBuilder(in);
            case 2:
                return new HistogramValuesSourceBuilder(in);
            case 3:
                return new GeoTileGridValuesSourceBuilder(in);
            default:
                throw new IOException("Invalid code " + code);
        }
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
        final CompositeValuesSourceBuilder<?> builder;
        switch(type) {
            case TermsValuesSourceBuilder.TYPE:
                builder = TermsValuesSourceBuilder.parse(name, parser);
                break;
            case DateHistogramValuesSourceBuilder.TYPE:
                builder = DateHistogramValuesSourceBuilder.PARSER.parse(parser, name);
                break;
            case HistogramValuesSourceBuilder.TYPE:
                builder = HistogramValuesSourceBuilder.parse(name, parser);
                break;
            case GeoTileGridValuesSourceBuilder.TYPE:
                builder = GeoTileGridValuesSourceBuilder.parse(name, parser);
                break;
            default:
                throw new ParsingException(parser.getTokenLocation(), "invalid source type: " + type);
        }
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
