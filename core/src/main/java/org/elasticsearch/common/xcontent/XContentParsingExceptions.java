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

import java.io.IOException;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * A set of static methods to get {@link org.elasticsearch.common.xcontent.XContentParser.Token} from {@link XContentParser}
 * while checking for their types and throw {@link ParsingException} if needed.
 */
public class XContentParsingExceptions {

    @FunctionalInterface
    public interface TokenSupplier {
        XContentParser.Token getToken() throws IOException;
    }

    /**
     * Get the Token using the TokenSupplier and throws a {@link ParsingException}
     * if the token is not of type Token.FIELD_NAME
     */
    public static XContentParser.Token ensureFieldName(XContentParser parser, TokenSupplier token) throws IOException {
        return ensureType(parser, XContentParser.Token.FIELD_NAME, token.getToken());
    }

    /**
     * Get the Token using the TokenSupplier and throws a {@link ParsingException}
     * if the token is not of type Token.FIELD_NAME or does not equal to the given fieldName
     */
    public static XContentParser.Token ensureFieldName(XContentParser parser, TokenSupplier token, String fieldName) throws IOException {
        XContentParser.Token t = ensureType(parser, XContentParser.Token.FIELD_NAME, token.getToken());

        String current = parser.currentName() != null ? parser.currentName() : "<null>";
        if (current.equals(fieldName) == false) {
            String message = "Failed to parse object: expecting field with name [%s] but found [%s]";
            throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, message, fieldName, current));
        }
        return t;
    }

    /**
     * Throws a {@link ParsingException} with a "unknown field found" reason.
     */
    public static void throwUnknownField(String field, Supplier<XContentLocation> location) {
        String message = "Failed to parse object: unknown field [%s] found";
        throw new ParsingException(location.get(), String.format(Locale.ROOT, message, field));
    }

    private static XContentParser.Token ensureType(XContentParser parser, XContentParser.Token expected, XContentParser.Token current) {
        if (current != expected) {
            String message = "Failed to parse object: expecting token of type [%s] but found [%s]";
            throw new ParsingException(parser.getTokenLocation(), String.format(Locale.ROOT, message, expected, current));
        }
        return current;
    }
}
