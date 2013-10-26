/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.xcontent.support;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public abstract class AbstractXContentParser implements XContentParser {


    @Override
    public boolean isBooleanValue() throws IOException {
        switch (currentToken()) {
            case VALUE_BOOLEAN:
                return true;
            case VALUE_NUMBER:
                NumberType numberType = numberType();
                return numberType == NumberType.LONG || numberType == NumberType.INT;
            case VALUE_STRING:
                return Booleans.isBoolean(textCharacters(), textOffset(), textLength());
            default:
                return false;
        }
    }

    @Override
    public boolean booleanValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_NUMBER) {
            return intValue() != 0;
        } else if (token == Token.VALUE_STRING) {
            return Booleans.parseBoolean(textCharacters(), textOffset(), textLength(), false /* irrelevant */);
        }
        return doBooleanValue();
    }

    protected abstract boolean doBooleanValue() throws IOException;

    @Override
    public short shortValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Short.parseShort(text());
        }
        return doShortValue();
    }

    protected abstract short doShortValue() throws IOException;

    @Override
    public int intValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Integer.parseInt(text());
        }
        return doIntValue();
    }

    protected abstract int doIntValue() throws IOException;

    @Override
    public long longValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Long.parseLong(text());
        }
        return doLongValue();
    }

    protected abstract long doLongValue() throws IOException;

    @Override
    public float floatValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Float.parseFloat(text());
        }
        return doFloatValue();
    }

    protected abstract float doFloatValue() throws IOException;

    @Override
    public double doubleValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Double.parseDouble(text());
        }
        return doDoubleValue();
    }

    protected abstract double doDoubleValue() throws IOException;

    @Override
    public String textOrNull() throws IOException {
        if (currentToken() == Token.VALUE_NULL) {
            return null;
        }
        return text();
    }


    @Override
    public BytesRef bytesOrNull() throws IOException {
        if (currentToken() == Token.VALUE_NULL) {
            return null;
        }
        return bytes();
    }

    @Override
    public Map<String, Object> map() throws IOException {
        return readMap(this);
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        return readOrderedMap(this);
    }

    @Override
    public Map<String, Object> mapAndClose() throws IOException {
        try {
            return map();
        } finally {
            close();
        }
    }

    @Override
    public Map<String, Object> mapOrderedAndClose() throws IOException {
        try {
            return mapOrdered();
        } finally {
            close();
        }
    }


    static interface MapFactory {
        Map<String, Object> newMap();
    }

    static final MapFactory SIMPLE_MAP_FACTORY = new MapFactory() {
        @Override
        public Map<String, Object> newMap() {
            return new HashMap<String, Object>();
        }
    };

    static final MapFactory ORDERED_MAP_FACTORY = new MapFactory() {
        @Override
        public Map<String, Object> newMap() {
            return new LinkedHashMap<String, Object>();
        }
    };

    static Map<String, Object> readMap(XContentParser parser) throws IOException {
        return readMap(parser, SIMPLE_MAP_FACTORY);
    }

    static Map<String, Object> readOrderedMap(XContentParser parser) throws IOException {
        return readMap(parser, ORDERED_MAP_FACTORY);
    }

    static Map<String, Object> readMap(XContentParser parser, MapFactory mapFactory) throws IOException {
        Map<String, Object> map = mapFactory.newMap();
        XContentParser.Token t = parser.currentToken();
        if (t == null) {
            t = parser.nextToken();
        }
        if (t == XContentParser.Token.START_OBJECT) {
            t = parser.nextToken();
        }
        for (; t == XContentParser.Token.FIELD_NAME; t = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            t = parser.nextToken();
            Object value = readValue(parser, mapFactory, t);
            map.put(fieldName, value);
        }
        return map;
    }

    private static List<Object> readList(XContentParser parser, MapFactory mapFactory, XContentParser.Token t) throws IOException {
        ArrayList<Object> list = new ArrayList<Object>();
        while ((t = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            list.add(readValue(parser, mapFactory, t));
        }
        return list;
    }

    private static Object readValue(XContentParser parser, MapFactory mapFactory, XContentParser.Token t) throws IOException {
        if (t == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (t == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (t == XContentParser.Token.VALUE_NUMBER) {
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
        } else if (t == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue();
        } else if (t == XContentParser.Token.START_OBJECT) {
            return readMap(parser, mapFactory);
        } else if (t == XContentParser.Token.START_ARRAY) {
            return readList(parser, mapFactory, t);
        } else if (t == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            return parser.binaryValue();
        }
        return null;
    }
}
