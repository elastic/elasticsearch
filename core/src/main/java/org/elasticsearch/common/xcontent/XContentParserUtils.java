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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * A set of static methods to get {@link Token} from {@link XContentParser}
 * while checking for their types and throw {@link ParsingException} if needed.
 */
public final class XContentParserUtils {

    private XContentParserUtils() {
    }

    /**
     * Makes sure that current token is of type {@link XContentParser.Token#FIELD_NAME} and the the field name is equal to the provided one
     * @throws ParsingException if the token is not of type {@link XContentParser.Token#FIELD_NAME} or is not equal to the given field name
     */
    public static void ensureFieldName(XContentParser parser, Token token, String fieldName) throws IOException {
        ensureExpectedToken(Token.FIELD_NAME, token, parser::getTokenLocation);
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
    public static void throwUnknownToken(XContentParser.Token token, XContentLocation location) {
        String message = "Failed to parse object: unexpected token [%s] found";
        throw new ParsingException(location, String.format(Locale.ROOT, message, token));
    }

    /**
     * Makes sure that provided token is of the expected type
     *
     * @throws ParsingException if the token is not equal to the expected type
     */
    public static void ensureExpectedToken(Token expected, Token actual, Supplier<XContentLocation> location) {
        if (actual != expected) {
            String message = "Failed to parse object: expecting token of type [%s] but found [%s]";
            throw new ParsingException(location.get(), String.format(Locale.ROOT, message, expected, actual));
        }
    }

    /**
     * Parse the current token depending on its token type. The following token types will be
     * parsed by the corresponding parser methods:
     * <ul>
     *    <li>XContentParser.Token.VALUE_STRING: parser.text()</li>
     *    <li>XContentParser.Token.VALUE_NUMBER: parser.numberValue()</li>
     *    <li>XContentParser.Token.VALUE_BOOLEAN: parser.booleanValue()</li>
     *    <li>XContentParser.Token.VALUE_EMBEDDED_OBJECT: parser.binaryValue()</li>
     * </ul>
     *
     * @throws ParsingException if the token none of the allowed values
     */
    public static Object parseStoredFieldsValue(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Object value = null;
        if (token == XContentParser.Token.VALUE_STRING) {
            //binary values will be parsed back and returned as base64 strings when reading from json and yaml
            value = parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = parser.numberValue();
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            value = parser.booleanValue();
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            //binary values will be parsed back and returned as BytesArray when reading from cbor and smile
            value = new BytesArray(parser.binaryValue());
        } else {
            throwUnknownToken(token, parser.getTokenLocation());
        }
        return value;
    }
}
