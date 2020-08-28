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

import java.io.IOException;

public final class XContentUtils {

    private XContentUtils() {
    }

    /**
     * Convert a {@link XContentParser.Token} to a value
     */
    public static Object readValue(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = parser.numberType();
            if (numberType == XContentParser.NumberType.INT) {
                return parser.intValue();
            } else if (numberType == XContentParser.NumberType.LONG) {
                return parser.longValue();
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                return parser.floatValue();
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                return parser.doubleValue();
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue();
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            return parser.list();
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            return parser.binaryValue();
        }
        return null;
    }
}
