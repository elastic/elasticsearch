/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.xcontent;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public interface XContentParser {

    enum Token {
        START_OBJECT,
        END_OBJECT,
        START_ARRAY,
        END_ARRAY,
        FIELD_NAME,
        VALUE_STRING,
        VALUE_NUMBER,
        VALUE_BOOLEAN,
        VALUE_NULL
    }

    enum NumberType {
        INT, LONG, BIG_INTEGER, FLOAT, DOUBLE, BIG_DECIMAL
    }

    XContentType contentType();

    Token nextToken() throws IOException;

    void skipChildren() throws IOException;

    Token currentToken();

    String currentName() throws IOException;

    Map<String, Object> map() throws IOException;

    String text() throws IOException;

    char[] textCharacters() throws IOException;

    int textLength() throws IOException;

    int textOffset() throws IOException;

    Number numberValue() throws IOException;

    NumberType numberType() throws IOException;

    byte byteValue() throws IOException;

    short shortValue() throws IOException;

    int intValue() throws IOException;

    long longValue() throws IOException;

    BigInteger bigIntegerValue() throws IOException;

    float floatValue() throws IOException;

    double doubleValue() throws IOException;

    java.math.BigDecimal decimalValue() throws IOException;

    boolean booleanValue() throws IOException;

    byte[] binaryValue() throws IOException;

    void close();
}
