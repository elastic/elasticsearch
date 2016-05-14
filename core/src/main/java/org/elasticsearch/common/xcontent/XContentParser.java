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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for pull - parsing {@link XContent} see {@link XContentType} for supported types.
 *
 * To obtain an instance of this class use the following pattern:
 *
 * <pre>
 *     XContentType xContentType = XContentType.JSON;
 *     XContentParser parser = xContentType.xContent().createParser("{\"key\" : \"value\"}");
 * </pre>
 */
public interface XContentParser extends Releasable {

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
        INT, LONG, FLOAT, DOUBLE
    }

    XContentType contentType();

    Token nextToken() throws IOException;

    void skipChildren() throws IOException;

    Token currentToken();

    String currentName() throws IOException;

    Map<String, Object> map() throws IOException;

    Map<String, Object> mapOrdered() throws IOException;

    List<Object> list() throws IOException;

    List<Object> listOrderedMap() throws IOException;

    String text() throws IOException;

    String textOrNull() throws IOException;

    /**
     * Returns a BytesRef holding UTF-8 bytes or null if a null value is {@link Token#VALUE_NULL}.
     * This method should be used to read text only binary content should be read through {@link #binaryValue()}
     */
    BytesRef utf8BytesOrNull() throws IOException;

    /**
     * Returns a BytesRef holding UTF-8 bytes.
     * This method should be used to read text only binary content should be read through {@link #binaryValue()}
     */
    BytesRef utf8Bytes() throws IOException;

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
     * returns true if the current value is boolean in nature.
     * values that are considered booleans:
     * - boolean value (true/false)
     * - numeric integers (=0 is considered as false, !=0 is true)
     * - one of the following strings: "true","false","on","off","yes","no","1","0"
     */
    boolean isBooleanValue() throws IOException;

    boolean booleanValue() throws IOException;

    /**
     * Reads a plain binary value that was written via one of the following methods:
     *
     * <ul>
     *     <li>{@link XContentBuilder#field(String, org.apache.lucene.util.BytesRef)}</li>
     *     <li>{@link XContentBuilder#field(String, org.elasticsearch.common.bytes.BytesReference)}</li>
     *     <li>{@link XContentBuilder#field(String, byte[], int, int)}}</li>
     *     <li>{@link XContentBuilder#field(String, byte[])}}</li>
     * </ul>
     *
     * as well as via their <code>String</code> variants of the separated value methods.
     * Note: Do not use this method to read values written with:
     * <ul>
     *     <li>{@link XContentBuilder#utf8Field(String, org.apache.lucene.util.BytesRef)}</li>
     *     <li>{@link XContentBuilder#utf8Field(String, org.apache.lucene.util.BytesRef)}</li>
     * </ul>
     *
     * these methods write UTF-8 encoded strings and must be read through:
     * <ul>
     *     <li>{@link XContentParser#utf8Bytes()}</li>
     *     <li>{@link XContentParser#utf8BytesOrNull()}}</li>
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

    boolean isClosed();
}
