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

package org.elasticsearch.client.security.support.expressiondsl.parser;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AllExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.CompositeRoleMapperExpressionBase;
import org.elasticsearch.client.security.support.expressiondsl.expressions.ExceptExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.CompositeExpressionBuilder;
import org.elasticsearch.client.security.support.expressiondsl.fields.DnFieldExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldExpressionBase;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldExpressionBuilder;
import org.elasticsearch.client.security.support.expressiondsl.fields.GroupsFieldExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.MetadataFieldExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.UsernameFieldExpression;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses the JSON (XContent) based boolean expression DSL into a tree of
 * {@link RoleMapperExpression} objects.
 */
public final class RoleMapperExpressionParser {

    /**
     * @param name The name of the expression tree within its containing object.
     * Used to provide descriptive error messages.
     * @param parser A parser over the XContent (typically JSON) DSL
     * representation of the expression
     */
    public RoleMapperExpression parse(final String name, final XContentParser parser) throws IOException {
        return parseRulesObject(name, parser, false);
    }

    private RoleMapperExpression parseRulesObject(final String objectName, final XContentParser parser, boolean allowExcept) throws IOException {
        // find the start of the DSL object
        final XContentParser.Token token;
        if (parser.currentToken() == null) {
            token = parser.nextToken();
        } else {
            token = parser.currentToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. expected [{}] to be an object but found [{}] instead",
                    objectName, token);
        }

        final String fieldName = fieldName(objectName, parser);
        final RoleMapperExpression expr = parseExpression(parser, fieldName, allowExcept, objectName);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] contains multiple fields", objectName);
        }
        return expr;
    }

    private RoleMapperExpression parseExpression(XContentParser parser, String field, boolean allowExcept, String objectName)
            throws IOException {

        if (Fields.ANY.match(field, parser.getDeprecationHandler())) {
            CompositeExpressionBuilder<? extends CompositeRoleMapperExpressionBase> builder = CompositeExpressionBuilder.builder(AnyExpression.class);
            parseExpressionArray(Fields.ANY, parser, false).forEach(builder::addExpression);
            return builder.build();
        } else if (Fields.ALL.match(field, parser.getDeprecationHandler())) {
            CompositeExpressionBuilder<? extends CompositeRoleMapperExpressionBase> builder = CompositeExpressionBuilder.builder(AllExpression.class);
            parseExpressionArray(Fields.ALL, parser, true).forEach(builder::addExpression);
            return builder.build();
        } else if (Fields.FIELD.match(field, parser.getDeprecationHandler())) {
            return parseFieldExpression(parser);
        } else if (Fields.EXCEPT.match(field, parser.getDeprecationHandler())) {
            if (allowExcept) {
                return parseExceptExpression(parser);
            } else {
                throw new ElasticsearchParseException("failed to parse rules expression. field [{}] is not allowed within [{}]", field,
                        objectName);
            }
        } else {
            throw new ElasticsearchParseException("failed to parse rules expression. field [{}] is not recognised in object [{}]", field,
                    objectName);
        }
    }

    private RoleMapperExpression parseFieldExpression(XContentParser parser) throws IOException {
        checkStartObject(parser);
        final FieldExpressionBuilder<? extends FieldExpressionBase> builder = fieldBuilder(Fields.FIELD.getPreferredName(), parser);

        final List<Object> values;
        if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
            values = parseArray(Fields.FIELD, parser, this::parseFieldValue);
        } else {
            values = Collections.singletonList(parseFieldValue(parser));
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] contains multiple fields", Fields.FIELD
                    .getPreferredName());
        }
        values.stream().forEach(builder::addValue);
        return builder.build();
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

    private FieldExpressionBuilder<? extends FieldExpressionBase> fieldBuilder(String objectName, XContentParser parser)
            throws IOException {
        String parsedFieldName = fieldName(objectName, parser);
        String fieldName = parsedFieldName;
        if (parsedFieldName.startsWith("metadata.")) {
            fieldName = parsedFieldName.substring("metadata.".length());
        }
        final FieldExpressionBuilder<? extends FieldExpressionBase> builder;
        switch (fieldName) {
        case "username":
            builder = FieldExpressionBuilder.builder(UsernameFieldExpression.class);
            break;
        case "groups":
            builder = FieldExpressionBuilder.builder(GroupsFieldExpression.class);
            break;
        case "dn":
            builder = FieldExpressionBuilder.builder(DnFieldExpression.class);
            break;
        case "metadata":
            builder = FieldExpressionBuilder.builder(MetadataFieldExpression.class);
            builder.withKey(parsedFieldName);
            break;
        default:
            throw new ElasticsearchParseException("failed to parse field expression, unexpected field name [{}]", fieldName);
        }
        return builder;
    }

    private String fieldName(String objectName, XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ElasticsearchParseException("failed to parse rules expression. object [{}] does not contain any fields", objectName);
        }
        String parsedFieldName = parser.currentName();
        return parsedFieldName;
    }

    private List<RoleMapperExpression> parseExpressionArray(ParseField field, XContentParser parser, boolean allowExcept)
            throws IOException {
        parser.nextToken(); // parseArray requires that the parser is positioned
                            // at the START_ARRAY token
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

    private Object parseFieldValue(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
        case VALUE_STRING:
            return parser.text();

        case VALUE_BOOLEAN:
            return parser.booleanValue();

        case VALUE_NUMBER:
            return parser.longValue();

        case VALUE_NULL:
            return null;

        default:
            throw new ElasticsearchParseException("failed to parse rules expression. expected a field value but found [{}] instead", parser
                    .currentToken());
        }
    }

    public interface Fields {
        ParseField ANY = new ParseField("any");
        ParseField ALL = new ParseField("all");
        ParseField EXCEPT = new ParseField("except");
        ParseField FIELD = new ParseField("field");
    }
}
