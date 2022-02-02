/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * A set of static methods to get {@link Token} from {@link XContentParser}
 * while checking for their types and throw {@link ParsingException} if needed.
 */
public final class XContentParserUtils {

    private XContentParserUtils() {}

    /**
     * Makes sure that current token is of type {@link Token#FIELD_NAME} and the field name is equal to the provided one
     * @throws ParsingException if the token is not of type {@link Token#FIELD_NAME} or is not equal to the given field name
     */
    public static void ensureFieldName(XContentParser parser, Token token, String fieldName) throws IOException {
        ensureExpectedToken(Token.FIELD_NAME, token, parser);
        String currentName = parser.currentName();
        if (currentName.equals(fieldName) == false) {
            String message = "Failed to parse object: expecting field with name [%s] but found [%s]";
            throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, message, fieldName, currentName));
        }
    }

    /**
     * @throws ParsingException with a "unknown field found" reason
     */
    public static void throwUnknownField(String field, XContentLocation location) {
        String message = "Failed to parse object: unknown field [%s] found";
        throw new ParsingException(location, String.format(Locale.ROOT, message, field));
    }

    /**
     * @throws ParsingException with a "unknown token found" reason
     */
    public static void throwUnknownToken(Token token, XContentLocation location) {
        String message = "Failed to parse object: unexpected token [%s] found";
        throw new ParsingException(location, String.format(Locale.ROOT, message, token));
    }

    /**
     * Makes sure that provided token is of the expected type
     *
     * @throws ParsingException if the token is not equal to the expected type
     */
    public static void ensureExpectedToken(Token expected, Token actual, XContentParser parser) {
        if (actual != expected) {
            throw parsingException(parser, expected, actual);
        }
    }

    private static ParsingException parsingException(XContentParser parser, Token expected, Token actual) {
        return new ParsingException(
            parser.getTokenLocation(),
            String.format(Locale.ROOT, "Failed to parse object: expecting token of type [%s] but found [%s]", expected, actual)
        );
    }

    /**
     * Parse the current token depending on its token type. The following token types will be
     * parsed by the corresponding parser methods:
     * <ul>
     *    <li>{@link Token#VALUE_STRING}: {@link XContentParser#text()}</li>
     *    <li>{@link Token#VALUE_NUMBER}: {@link XContentParser#numberValue()} ()}</li>
     *    <li>{@link Token#VALUE_BOOLEAN}: {@link XContentParser#booleanValue()} ()}</li>
     *    <li>{@link Token#VALUE_EMBEDDED_OBJECT}: {@link XContentParser#binaryValue()} ()}</li>
     *    <li>{@link Token#VALUE_NULL}: returns null</li>
     *    <li>{@link Token#START_OBJECT}: {@link XContentParser#mapOrdered()} ()}</li>
     *    <li>{@link Token#START_ARRAY}: {@link XContentParser#listOrderedMap()} ()}</li>
     * </ul>
     *
     * @throws ParsingException if the token is none of the allowed values
     */
    public static Object parseFieldsValue(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        Object value = null;
        if (token == Token.VALUE_STRING) {
            // binary values will be parsed back and returned as base64 strings when reading from json and yaml
            value = parser.text();
        } else if (token == Token.VALUE_NUMBER) {
            value = parser.numberValue();
        } else if (token == Token.VALUE_BOOLEAN) {
            value = parser.booleanValue();
        } else if (token == Token.VALUE_EMBEDDED_OBJECT) {
            // binary values will be parsed back and returned as BytesArray when reading from cbor and smile
            value = new BytesArray(parser.binaryValue());
        } else if (token == Token.VALUE_NULL) {
            value = null;
        } else if (token == Token.START_OBJECT) {
            value = parser.mapOrdered();
        } else if (token == Token.START_ARRAY) {
            value = parser.listOrderedMap();
        } else {
            throwUnknownToken(token, parser.getTokenLocation());
        }
        return value;
    }

    /**
     * This method expects that the current field name is the concatenation of a type, a delimiter and a name
     * (ex: terms#foo where "terms" refers to the type of a registered {@link NamedXContentRegistry.Entry},
     * "#" is the delimiter and "foo" the name of the object to parse).
     *
     * It also expected that following this field name is either an Object or an array xContent structure and
     * the cursor points to the start token of this structure.
     *
     * The method splits the field's name to extract the type and name and then parses the object
     * using the {@link XContentParser#namedObject(Class, String, Object)} method.
     *
     * @param parser      the current {@link XContentParser}
     * @param delimiter   the delimiter to use to splits the field's name
     * @param objectClass the object class of the object to parse
     * @param consumer    something to consume the parsed object
     * @param <T>         the type of the object to parse
     * @throws IOException if anything went wrong during parsing or if the type or name cannot be derived
     *                     from the field's name
     * @throws ParsingException if the parser isn't positioned on either START_OBJECT or START_ARRAY at the beginning
     */
    public static <T> void parseTypedKeysObject(XContentParser parser, String delimiter, Class<T> objectClass, Consumer<T> consumer)
        throws IOException {
        if (parser.currentToken() != Token.START_OBJECT && parser.currentToken() != Token.START_ARRAY) {
            throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
        }
        String currentFieldName = parser.currentName();
        if (Strings.hasLength(currentFieldName)) {
            int position = currentFieldName.indexOf(delimiter);
            if (position > 0) {
                String type = currentFieldName.substring(0, position);
                String name = currentFieldName.substring(position + 1);
                consumer.accept(parser.namedObject(objectClass, type, name));
                return;
            }
            // if we didn't find a delimiter we ignore the object or array for forward compatibility instead of throwing an error
            parser.skipChildren();
        } else {
            throw new ParsingException(parser.getTokenLocation(), "Failed to parse object: empty key");
        }
    }

    /**
     * Parses a list of a given type from the given {@code parser}. Assumes that the parser is currently positioned on a
     * {@link Token#START_ARRAY} token and will fail if it is not. The returned list may or may not be mutable.
     *
     * @param parser      x-content parser
     * @param valueParser parser for expected list value type
     * @return list parsed from parser
     */
    public static <T> List<T> parseList(XContentParser parser, CheckedFunction<XContentParser, T, IOException> valueParser)
        throws IOException {
        XContentParserUtils.ensureExpectedToken(Token.START_ARRAY, parser.currentToken(), parser);
        if (parser.nextToken() == Token.END_ARRAY) {
            return List.of();
        }
        final ArrayList<T> list = new ArrayList<>();
        do {
            list.add(valueParser.apply(parser));
        } while (parser.nextToken() != Token.END_ARRAY);
        return list;
    }
}
