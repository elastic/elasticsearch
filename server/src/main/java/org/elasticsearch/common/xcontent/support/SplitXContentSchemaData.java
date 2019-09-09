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

package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;

/**
 * Utility class to split and merge the schema and data of a JSON document.
 * This is typically useful to improve compression, especially in the case that
 * multiple documents share similar schemas.
 */
public final class SplitXContentSchemaData {

    private static final int MAX_LONG_CHAR_COUNT = Long.toString(Long.MAX_VALUE).length();
    private static final BigInteger MAX_LONG_AS_BIG_INTEGER = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG_AS_BIG_INTEGER = BigInteger.valueOf(Long.MIN_VALUE);

    private SplitXContentSchemaData() {}

    /**
     * Split the schema and the data of the current object that the parser is
     * positioned on into two separate streams.
     * @param parser parser of the object to split, must be positioned on a START_OBJECT
     * @param schema where to write the schema of the object, including field names
     * @param data   where to write the data of the object
     */
    public static void splitXContent(XContentParser parser, StreamOutput schema, StreamOutput data) throws IOException {
        if (parser.currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException("The provided parser must be positioned on a START_OBJECT, but got " + parser.currentToken());
        }

        schema.write('{');
        int level = 1;
        for (Token token = parser.nextToken(); level > 0; token = parser.nextToken()) {
            switch (token) {
            case START_OBJECT:
                schema.write('{');
                level++;
                break;
            case END_OBJECT:
                schema.write('}');
                level--;
                break;
            case START_ARRAY:
                schema.write('[');
                break;
            case END_ARRAY:
                schema.write(']');
                break;
            case FIELD_NAME:
                schema.write('F');
                schema.writeString(parser.currentName());
                break;
            case VALUE_STRING:
                schema.write('S');
                data.writeString(parser.text());
                break;
            case VALUE_NUMBER:
                schema.write('N');
                data.writeString(parser.text());
                break;
            case VALUE_BOOLEAN:
                schema.write('B');
                data.writeString(parser.text());
                break;
            case VALUE_NULL:
                schema.write('\0');
                break;
            case VALUE_EMBEDDED_OBJECT:
                schema.write('E');
                data.writeByteArray(parser.binaryValue());
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Merge back the schema and data of an XContent object. Note that calling
     * {@link Closeable#close} in the parser doesn't close the input streams.
     *
     * @param schema input stream containing the schema of the object
     * @param data   input stream containing the data of the object
     * @return a parser for the object
     */
    public static XContentParser mergeXContent(StreamInput schema, StreamInput data) {
        return new AbstractXContentParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION) {

            private Token token;
            private String fieldName;
            private String stringValue;
            private byte[] binaryValue;
            private boolean closed;

            @Override
            public XContentType contentType() {
                return XContentType.JSON;
            }

            @Override
            public Token nextToken() throws IOException {
                token = readToken();
                if (token != null) {
                    switch (token) {
                    case FIELD_NAME:
                        fieldName = schema.readString();
                        stringValue = null;
                        binaryValue = null;
                        break;
                    case VALUE_STRING:
                    case VALUE_NUMBER:
                    case VALUE_BOOLEAN:
                        stringValue = data.readString();
                        binaryValue = null;
                        break;
                    case VALUE_EMBEDDED_OBJECT:
                        stringValue = null;
                        binaryValue = data.readByteArray();
                        break;
                    default:
                        stringValue = null;
                        binaryValue = null;
                        break;
                    }
                }
                return token;
            }

            private Token readToken() throws IOException {
                final int b = schema.read();
                switch (b) {
                case -1:
                    return null;
                case '{':
                    return Token.START_OBJECT;
                case '}':
                    return Token.END_OBJECT;
                case '[':
                    return Token.START_ARRAY;
                case ']':
                    return Token.END_ARRAY;
                case 'F':
                    return Token.FIELD_NAME;
                case 'S':
                    return Token.VALUE_STRING;
                case '\0':
                    return Token.VALUE_NULL;
                case 'N':
                    return Token.VALUE_NUMBER;
                case 'B':
                    return Token.VALUE_BOOLEAN;
                case 'E':
                    return Token.VALUE_EMBEDDED_OBJECT;
                default:
                    throw new IOException("Data is corrupt, unexpected token: " + b);
                }
            }

            @Override
            public void skipChildren() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Token currentToken() {
                return token;
            }

            @Override
            public String currentName() throws IOException {
                return fieldName;
            }

            @Override
            public String text() throws IOException {
                if (binaryValue != null || stringValue == null) {
                    throw new IllegalStateException();
                }
                return stringValue;
            }

            @Override
            public CharBuffer charBuffer() throws IOException {
                if (binaryValue != null || stringValue == null) {
                    throw new IllegalStateException();
                }
                return CharBuffer.wrap(stringValue);
            }

            @Override
            public boolean hasTextCharacters() {
                return false;
            }

            @Override
            public char[] textCharacters() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int textLength() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int textOffset() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Number numberValue() throws IOException {
                if (binaryValue != null || stringValue == null) {
                    throw new IllegalStateException();
                }

                // Fast path for integers
                if (stringValue.length() < MAX_LONG_CHAR_COUNT) {
                    boolean integer = true;
                    for (int i = 0; i < stringValue.length(); ++i) {
                        char c = stringValue.charAt(i);
                        if ((c < '0' || c > '9') && (i > 0 || c != '-')) {
                            integer = false;
                            break;
                        }
                    }

                    if (integer) {
                        return Long.parseLong(stringValue);
                    }
                }

                BigDecimal asBigDecimal = new BigDecimal(stringValue);
                if (asBigDecimal.stripTrailingZeros().scale() <= 0) { // whole number
                    BigInteger asBigInteger = asBigDecimal.toBigIntegerExact();
                    if (asBigInteger.compareTo(MIN_LONG_AS_BIG_INTEGER) >= 0 && asBigInteger.compareTo(MAX_LONG_AS_BIG_INTEGER) <= 0) {
                        return asBigInteger.longValue();
                    } else {
                        return asBigInteger;
                    }
                } else {
                    double asDouble = asBigDecimal.doubleValue();
                    // TODO: Is there a better way?
                    if (asBigDecimal.toString().equals(Double.toString(asDouble))) {
                        return asDouble;
                    } else {
                        return asBigDecimal;
                    }
                }
            }

            @Override
            public NumberType numberType() throws IOException {
                Number number = numberValue();
                if (number instanceof Long) {
                    return NumberType.LONG;
                } else if (number instanceof Double) {
                    return NumberType.DOUBLE;
                } else if (number instanceof BigInteger) {
                    return NumberType.BIG_INTEGER;
                } else if (number instanceof BigDecimal) {
                    return NumberType.BIG_DECIMAL;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public byte[] binaryValue() throws IOException {
                if (binaryValue == null || stringValue != null) {
                    throw new IllegalStateException();
                }
                return binaryValue;
            }

            @Override
            public XContentLocation getTokenLocation() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected boolean doBooleanValue() throws IOException {
                if (binaryValue != null || stringValue == null) {
                    throw new IllegalStateException();
                }
                return Boolean.valueOf(stringValue);
            }

            @Override
            protected short doShortValue() throws IOException {
                return (short) doLongValue();
            }

            @Override
            protected int doIntValue() throws IOException {
                return (int) doLongValue();
            }

            @Override
            protected long doLongValue() throws IOException {
                return numberValue().longValue();
            }

            @Override
            protected float doFloatValue() throws IOException {
                return (float) doDoubleValue();
            }

            @Override
            protected double doDoubleValue() throws IOException {
                return numberValue().doubleValue();
            }

            @Override
            public void close() throws IOException {
                // no-op
                closed = true;
            }

            @Override
            public boolean isClosed() {
                return closed;
            }

        };
    }

}
