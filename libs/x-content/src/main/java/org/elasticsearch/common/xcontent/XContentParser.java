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

import org.elasticsearch.common.CheckedFunction;

import java.io.Closeable;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Interface for pull - parsing {@link XContent} see {@link XContentType} for supported types.
 *
 * To obtain an instance of this class use the following pattern:
 *
 * <pre>
 *     XContentType xContentType = XContentType.JSON;
 *     XContentParser parser = xContentType.xContent().createParser(
 *          NamedXContentRegistry.EMPTY, ParserField."{\"key\" : \"value\"}");
 * </pre>
 */
public interface XContentParser extends Closeable {

    enum Token {
        START_OBJECT {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        END_OBJECT {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        START_ARRAY {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        END_ARRAY {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        FIELD_NAME {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        VALUE_STRING {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        VALUE_NUMBER {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        VALUE_BOOLEAN {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        // usually a binary value
        VALUE_EMBEDDED_OBJECT {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        VALUE_NULL {
            @Override
            public boolean isValue() {
                return false;
            }
        };

        public abstract boolean isValue();
    }

    enum NumberType {
        INT, BIG_INTEGER, LONG, FLOAT, DOUBLE, BIG_DECIMAL
    }

    XContentType contentType();

    Token nextToken() throws IOException;

    void skipChildren() throws IOException;

    Token currentToken();

    String currentName() throws IOException;

    Map<String, Object> map() throws IOException;

    Map<String, Object> mapOrdered() throws IOException;

    Map<String, String> mapStrings() throws IOException;

    /**
     * Returns an instance of {@link Map} holding parsed map.
     * Serves as a replacement for the "map", "mapOrdered" and "mapStrings" methods above.
     *
     * @param mapFactory factory for creating new {@link Map} objects
     * @param mapValueParser parser for parsing a single map value
     * @param <T> map value type
     * @return {@link Map} object
     */
    <T> Map<String, T> map(
        Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser) throws IOException;

    List<Object> list() throws IOException;

    List<Object> listOrderedMap() throws IOException;

    String text() throws IOException;

    String textOrNull() throws IOException;

    CharBuffer charBufferOrNull() throws IOException;

    /**
     * Returns a {@link CharBuffer} holding UTF-8 bytes.
     * This method should be used to read text only binary content should be read through {@link #binaryValue()}
     */
    CharBuffer charBuffer() throws IOException;

    Object objectText() throws IOException;

    Object objectBytes() throws IOException;

    /**
     * Method that can be used to determine whether calling of textCharacters() would be the most efficient way to
     * access textual content for the event parser currently points to.
     *
     * Default implementation simply returns false since only actual
     * implementation class has knowledge of its internal buffering
     * state.
     *
     * This method shouldn't be used to check if the token contains text or not.
     */
    boolean hasTextCharacters();

    char[] textCharacters() throws IOException;

    int textLength() throws IOException;

    int textOffset() throws IOException;

    Number numberValue() throws IOException;

    NumberType numberType() throws IOException;

    short shortValue(boolean coerce) throws IOException;

    int intValue(boolean coerce) throws IOException;

    long longValue(boolean coerce) throws IOException;

    float floatValue(boolean coerce) throws IOException;

    double doubleValue(boolean coerce) throws IOException;

    short shortValue() throws IOException;

    int intValue() throws IOException;

    long longValue() throws IOException;

    float floatValue() throws IOException;

    double doubleValue() throws IOException;

    /**
     * @return true iff the current value is either boolean (<code>true</code> or <code>false</code>) or one of "false", "true".
     */
    boolean isBooleanValue() throws IOException;

    boolean booleanValue() throws IOException;

    /**
     * Reads a plain binary value that was written via one of the following methods:
     *
     * <ul>
     *     <li>{@link XContentBuilder#field(String, byte[], int, int)}}</li>
     *     <li>{@link XContentBuilder#field(String, byte[])}}</li>
     * </ul>
     *
     * as well as via their <code>String</code> variants of the separated value methods.
     * Note: Do not use this method to read values written with:
     * <ul>
     *     <li>{@link XContentBuilder#utf8Value(byte[], int, int)}</li>
     * </ul>
     *
     * these methods write UTF-8 encoded strings and must be read through:
     * <ul>
     *     <li>{@link XContentParser#text()} ()}</li>
     *     <li>{@link XContentParser#textOrNull()} ()}</li>
     *     <li>{@link XContentParser#textCharacters()} ()}}</li>
     * </ul>
     *
     */
    byte[] binaryValue() throws IOException;

    /**
     * Used for error reporting to highlight where syntax errors occur in
     * content being parsed.
     *
     * @return last token's location or null if cannot be determined
     */
    XContentLocation getTokenLocation();

    // TODO remove context entirely when it isn't needed
    /**
     * Parse an object by name.
     */
    <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException;

    /**
     * The registry used to resolve {@link #namedObject(Class, String, Object)}. Use this when building a sub-parser from this parser.
     */
    NamedXContentRegistry getXContentRegistry();

    boolean isClosed();

    /**
     * The callback to notify when parsing encounters a deprecated field.
     */
    DeprecationHandler getDeprecationHandler();
}
