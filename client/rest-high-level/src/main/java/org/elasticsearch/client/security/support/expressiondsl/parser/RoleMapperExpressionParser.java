/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.parser;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AllRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.CompositeType;
import org.elasticsearch.client.security.support.expressiondsl.expressions.ExceptRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses the JSON (XContent) based boolean expression DSL into a tree of
 * {@link RoleMapperExpression} objects.
 * Note: As this is client side parser, it mostly validates the structure of
 * DSL being parsed it does not enforce rules
 * like allowing "except" within "except" or "any" expressions.
 */
public final class RoleMapperExpressionParser {
    public static final ParseField FIELD = new ParseField("field");

    public static RoleMapperExpression fromXContent(final XContentParser parser) throws IOException {
        return new RoleMapperExpressionParser().parse("rules", parser);
    }

    /**
     * This function exists to be compatible with
     * {@link org.elasticsearch.xcontent.ContextParser#parse(XContentParser, Object)}
     */
    public static RoleMapperExpression parseObject(XContentParser parser, String id) throws IOException {
        return new RoleMapperExpressionParser().parse(id, parser);
    }

    /**
     * @param name The name of the expression tree within its containing object.
     * Used to provide descriptive error messages.
     * @param parser A parser over the XContent (typically JSON) DSL
     * representation of the expression
     */
    public RoleMapperExpression parse(final String name, final XContentParser parser) throws IOException {
        return parseRulesObject(name, parser);
    }

    private RoleMapperExpression parseRulesObject(final String objectName, final XContentParser parser) throws IOException {
        // find the start of the DSL object
        final XContentParser.Token token;
        if (parser.currentToken() == null) {
            token = parser.nextToken();
        } else {
            token = parser.currentToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse rules expression. expected [{}] to be an object but found [{}] instead",
                objectName,
                token
            );
        }

        final String fieldName = fieldName(objectName, parser);
        final RoleMapperExpression expr = parseExpression(parser, fieldName, objectName);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] contains multiple fields", objectName);
        }
        return expr;
    }

    private RoleMapperExpression parseExpression(XContentParser parser, String field, String objectName) throws IOException {

        if (CompositeType.ANY.getParseField().match(field, parser.getDeprecationHandler())) {
            final AnyRoleMapperExpression.Builder builder = AnyRoleMapperExpression.builder();
            parseExpressionArray(CompositeType.ANY.getParseField(), parser).forEach(builder::addExpression);
            return builder.build();
        } else if (CompositeType.ALL.getParseField().match(field, parser.getDeprecationHandler())) {
            final AllRoleMapperExpression.Builder builder = AllRoleMapperExpression.builder();
            parseExpressionArray(CompositeType.ALL.getParseField(), parser).forEach(builder::addExpression);
            return builder.build();
        } else if (FIELD.match(field, parser.getDeprecationHandler())) {
            return parseFieldExpression(parser);
        } else if (CompositeType.EXCEPT.getParseField().match(field, parser.getDeprecationHandler())) {
            return parseExceptExpression(parser);
        } else {
            throw new ElasticsearchParseException(
                "failed to parse rules expression. field [{}] is not recognised in object [{}]",
                field,
                objectName
            );
        }
    }

    private RoleMapperExpression parseFieldExpression(XContentParser parser) throws IOException {
        checkStartObject(parser);
        final String fieldName = fieldName(FIELD.getPreferredName(), parser);

        final List<Object> values;
        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
            values = parseArray(FIELD, parser, this::parseFieldValue);
        } else {
            values = Collections.singletonList(parseFieldValue(parser));
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException(
                "failed to parse rules expression. object [{}] contains multiple fields",
                FIELD.getPreferredName()
            );
        }

        return FieldRoleMapperExpression.ofKeyValues(fieldName, values.toArray());
    }

    private RoleMapperExpression parseExceptExpression(XContentParser parser) throws IOException {
        checkStartObject(parser);
        return new ExceptRoleMapperExpression(parseRulesObject(CompositeType.EXCEPT.getName(), parser));
    }

    private void checkStartObject(XContentParser parser) throws IOException {
        final XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. expected an object but found [{}] instead", token);
        }
    }

    private String fieldName(String objectName, XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] does not contain any fields", objectName);
        }
        String parsedFieldName = parser.currentName();
        return parsedFieldName;
    }

    private List<RoleMapperExpression> parseExpressionArray(ParseField field, XContentParser parser) throws IOException {
        parser.nextToken(); // parseArray requires that the parser is positioned
                            // at the START_ARRAY token
        return parseArray(field, parser, p -> parseRulesObject(field.getPreferredName(), p));
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

    private Object parseFieldValue(XContentParser parser) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_STRING -> parser.text();
            case VALUE_BOOLEAN -> parser.booleanValue();
            case VALUE_NUMBER -> parser.longValue();
            case VALUE_NULL -> null;
            default -> throw new ElasticsearchParseException(
                "failed to parse rules expression. expected a field value but found [{}] instead",
                parser.currentToken()
            );
        };
    }

}
