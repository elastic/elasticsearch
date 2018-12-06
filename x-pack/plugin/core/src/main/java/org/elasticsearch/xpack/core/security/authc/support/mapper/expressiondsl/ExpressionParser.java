/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses the JSON (XContent) based boolean expression DSL into a tree of {@link RoleMapperExpression} objects.
 */
public final class ExpressionParser {

    public static RoleMapperExpression readExpression(StreamInput in) throws IOException {
        return in.readNamedWriteable(RoleMapperExpression.class);
    }

    public static void writeExpression(RoleMapperExpression expression, StreamOutput out) throws IOException {
        out.writeNamedWriteable(expression);
    }

    static List<RoleMapperExpression> readExpressionList(StreamInput in) throws IOException {
        return in.readNamedWriteableList(RoleMapperExpression.class);
    }

    static void writeExpressionList(List<RoleMapperExpression> list, StreamOutput out) throws IOException {
        out.writeNamedWriteableList(list);
    }

    /**
     * This function exists to be compatible with
     * {@link org.elasticsearch.common.xcontent.ContextParser#parse(XContentParser, Object)}
     */
    public static RoleMapperExpression parseObject(XContentParser parser, String id) throws IOException {
        return new ExpressionParser().parse(id, parser);
    }

    /**
     * @param name    The name of the expression tree within its containing object. Used to provide
     *                descriptive error messages.
     * @param content The XContent (typically JSON) DSL representation of the expression
     */
    public RoleMapperExpression parse(String name, XContentSource content) throws IOException {
        try (InputStream stream = content.getBytes().streamInput()) {
            return parse(name, content.parser(NamedXContentRegistry.EMPTY, stream));
        }
    }

    /**
     * @param name   The name of the expression tree within its containing object. Used to provide
     *               descriptive error messages.
     * @param parser A parser over the XContent (typically JSON) DSL representation of the
     *               expression
     */
    public RoleMapperExpression parse(String name, XContentParser parser) throws IOException {
        return parseRulesObject(name, parser, false);
    }

    private RoleMapperExpression parseRulesObject(String objectName, XContentParser parser,
                                                  boolean allowExcept) throws IOException {
        // find the start of the DSL object
        XContentParser.Token token;
        if (parser.currentToken() == null) {
            token = parser.nextToken();
        } else {
            token = parser.currentToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. expected [{}] to be an object but found [{}] instead",
                    objectName, token);
        }

        final String fieldName = readFieldName(objectName, parser);
        final RoleMapperExpression expr = parseExpression(parser, fieldName, allowExcept, objectName);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] contains multiple fields", objectName);
        }
        return expr;
    }

    private RoleMapperExpression parseExpression(XContentParser parser, String field, boolean allowExcept, String objectName)
            throws IOException {

        if (Fields.ANY.match(field, parser.getDeprecationHandler())) {
            return new AnyExpression(parseExpressionArray(Fields.ANY, parser, false));
        } else if (Fields.ALL.match(field, parser.getDeprecationHandler())) {
            return new AllExpression(parseExpressionArray(Fields.ALL, parser, true));
        } else if (Fields.FIELD.match(field, parser.getDeprecationHandler())) {
            return parseFieldExpression(parser);
        } else if (Fields.EXCEPT.match(field, parser.getDeprecationHandler())) {
            if (allowExcept) {
                return parseExceptExpression(parser);
            } else {
                throw new ElasticsearchParseException("failed to parse rules expression. field [{}] is not allowed within [{}]",
                        field, objectName);
            }
        } else {
            throw new ElasticsearchParseException("failed to parse rules expression. field [{}] is not recognised in object [{}]",
                    field, objectName);
        }
    }

    private RoleMapperExpression parseFieldExpression(XContentParser parser) throws IOException {
        checkStartObject(parser);
        final String fieldName = readFieldName(Fields.FIELD.getPreferredName(), parser);
        final List<FieldExpression.FieldValue> values;
        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
            values = parseArray(Fields.FIELD, parser, this::parseFieldValue);
        } else {
            values = Collections.singletonList(parseFieldValue(parser));
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] contains multiple fields",
                    Fields.FIELD.getPreferredName());
        }
        return new FieldExpression(fieldName, values);
    }

    private RoleMapperExpression parseExceptExpression(XContentParser parser) throws IOException {
        checkStartObject(parser);
        return new ExceptExpression(parseRulesObject(Fields.EXCEPT.getPreferredName(), parser, false));
    }

    private void checkStartObject(XContentParser parser) throws IOException {
        final XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. expected an object but found [{}] instead", token);
        }
    }

    private String readFieldName(String objectName, XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] does not contain any fields", objectName);
        }
        return parser.currentName();
    }

    private List<RoleMapperExpression> parseExpressionArray(ParseField field, XContentParser parser, boolean allowExcept)
            throws IOException {
        parser.nextToken(); // parseArray requires that the parser is positioned at the START_ARRAY token
        return parseArray(field, parser, p -> parseRulesObject(field.getPreferredName(), p, allowExcept));
    }

    private <T> List<T> parseArray(ParseField field, XContentParser parser, CheckedFunction<XContentParser, T, IOException> elementParser)
            throws IOException {
        final XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            List<T> list = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                list.add(elementParser.apply(parser));
            }
            return list;
        } else {
            throw new ElasticsearchParseException("failed to parse rules expression. field [{}] requires an array", field);
        }
    }

    private FieldExpression.FieldValue parseFieldValue(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
                return new FieldExpression.FieldValue(parser.text());

            case VALUE_BOOLEAN:
                return new FieldExpression.FieldValue(parser.booleanValue());

            case VALUE_NUMBER:
                return new FieldExpression.FieldValue(parser.longValue());

            case VALUE_NULL:
                return new FieldExpression.FieldValue(null);

            default:
                throw new ElasticsearchParseException("failed to parse rules expression. expected a field value but found [{}] instead",
                        parser.currentToken());
        }
    }

    public interface Fields {
        ParseField ANY = new ParseField("any");
        ParseField ALL = new ParseField("all");
        ParseField EXCEPT = new ParseField("except");
        ParseField FIELD = new ParseField("field");
    }
}
