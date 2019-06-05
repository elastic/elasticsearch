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

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class AbstractXContentParser implements XContentParser {

    // Currently this is not a setting that can be changed and is a policy
    // that relates to how parsing of things like "boost" are done across
    // the whole of Elasticsearch (eg if String "1.0" is a valid float).
    // The idea behind keeping it as a constant is that we can track
    // references to this policy decision throughout the codebase and find
    // and change any code that needs to apply an alternative policy.
    public static final boolean DEFAULT_NUMBER_COERCE_POLICY = true;

    private static void checkCoerceString(boolean coerce, Class<? extends Number> clazz) {
        if (!coerce) {
            //Need to throw type IllegalArgumentException as current catch logic in
            //NumberFieldMapper.parseCreateField relies on this for "malformed" value detection
            throw new IllegalArgumentException(clazz.getSimpleName() + " value passed as String");
        }
    }

    private final NamedXContentRegistry xContentRegistry;
    private final DeprecationHandler deprecationHandler;

    public AbstractXContentParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler) {
        this.xContentRegistry = xContentRegistry;
        this.deprecationHandler = deprecationHandler;
    }

    // The 3rd party parsers we rely on are known to silently truncate fractions: see
    //   http://fasterxml.github.io/jackson-core/javadoc/2.3.0/com/fasterxml/jackson/core/JsonParser.html#getShortValue()
    // If this behaviour is flagged as undesirable and any truncation occurs
    // then this method is called to trigger the"malformed" handling logic
    void ensureNumberConversion(boolean coerce, long result, Class<? extends Number> clazz) throws IOException {
        if (!coerce) {
            double fullVal = doDoubleValue();
            if (result != fullVal) {
                // Need to throw type IllegalArgumentException as current catch
                // logic in NumberFieldMapper.parseCreateField relies on this
                // for "malformed" value detection
                throw new IllegalArgumentException(fullVal + " cannot be converted to " + clazz.getSimpleName() + " without data loss");
            }
        }
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        switch (currentToken()) {
            case VALUE_BOOLEAN:
                return true;
            case VALUE_STRING:
                return Booleans.isBoolean(textCharacters(), textOffset(), textLength());
            default:
                return false;
        }
    }

    @Override
    public boolean booleanValue() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            return Booleans.parseBoolean(textCharacters(), textOffset(), textLength(), false /* irrelevant */);
        }
        return doBooleanValue();
    }

    protected abstract boolean doBooleanValue() throws IOException;

    @Override
    public short shortValue() throws IOException {
        return shortValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Short.class);

            double doubleValue = Double.parseDouble(text());

            if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + text() + "] is out of range for a short");
            }

            return (short) doubleValue;
        }
        short result = doShortValue();
        ensureNumberConversion(coerce, result, Short.class);
        return result;
    }

    protected abstract short doShortValue() throws IOException;

    @Override
    public int intValue() throws IOException {
        return intValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Integer.class);
            double doubleValue = Double.parseDouble(text());

            if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + text() + "] is out of range for an integer");
            }

            return (int) doubleValue;
        }
        int result = doIntValue();
        ensureNumberConversion(coerce, result, Integer.class);
        return result;
    }

    protected abstract int doIntValue() throws IOException;

    private static BigInteger LONG_MAX_VALUE_AS_BIGINTEGER = BigInteger.valueOf(Long.MAX_VALUE);
    private static BigInteger LONG_MIN_VALUE_AS_BIGINTEGER = BigInteger.valueOf(Long.MIN_VALUE);
    // weak bounds on the BigDecimal representation to allow for coercion
    private static BigDecimal BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);

    /** Return the long that {@code stringValue} stores or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value and {@code coerce} is false. */
    private static long toLong(String stringValue, boolean coerce) {
        try {
            return Long.parseLong(stringValue);
        } catch (NumberFormatException e) {
            // we will try again with BigDecimal
        }

        final BigInteger bigIntegerValue;
        try {
            final BigDecimal bigDecimalValue = new BigDecimal(stringValue);
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE) >= 0 ||
                bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE) <= 0) {
                throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
            }
            bigIntegerValue = coerce ? bigDecimalValue.toBigInteger() : bigDecimalValue.toBigIntegerExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Value [" + stringValue + "] has a decimal part");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
        }

        if (bigIntegerValue.compareTo(LONG_MAX_VALUE_AS_BIGINTEGER) > 0 || bigIntegerValue.compareTo(LONG_MIN_VALUE_AS_BIGINTEGER) < 0) {
            throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
        }

        assert bigIntegerValue.longValueExact() <= Long.MAX_VALUE; // asserting that no ArithmeticException is thrown
        return bigIntegerValue.longValue();
    }

    @Override
    public long longValue() throws IOException {
        return longValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Long.class);
            return toLong(text(), coerce);
        }
        long result = doLongValue();
        ensureNumberConversion(coerce, result, Long.class);
        return result;
    }

    protected abstract long doLongValue() throws IOException;

    @Override
    public float floatValue() throws IOException {
        return floatValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Float.class);
            return Float.parseFloat(text());
        }
        return doFloatValue();
    }

    protected abstract float doFloatValue() throws IOException;


    @Override
    public double doubleValue() throws IOException {
        return doubleValue(DEFAULT_NUMBER_COERCE_POLICY);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) {
            checkCoerceString(coerce, Double.class);
            return Double.parseDouble(text());
        }
        return doDoubleValue();
    }

    protected abstract double doDoubleValue() throws IOException;

    @Override
    public final String textOrNull() throws IOException {
        if (currentToken() == Token.VALUE_NULL) {
            return null;
        }
        return text();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        if (currentToken() == Token.VALUE_NULL) {
            return null;
        }
        return charBuffer();
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
    public Map<String, String> mapStrings() throws IOException {
        return readMapStrings(this);
    }

    @Override
    public <T> Map<String, T> map(
            Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser) throws IOException {
        return readGenericMap(this, mapFactory, mapValueParser);
    }

    @Override
    public List<Object> list() throws IOException {
        return readList(this);
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        return readListOrderedMap(this);
    }

    static final Supplier<Map<String, Object>> SIMPLE_MAP_FACTORY = HashMap::new;

    static final Supplier<Map<String, Object>> ORDERED_MAP_FACTORY = LinkedHashMap::new;

    static final Supplier<Map<String, String>> SIMPLE_MAP_STRINGS_FACTORY = HashMap::new;

    static Map<String, Object> readMap(XContentParser parser) throws IOException {
        return readMap(parser, SIMPLE_MAP_FACTORY);
    }

    static Map<String, Object> readOrderedMap(XContentParser parser) throws IOException {
        return readMap(parser, ORDERED_MAP_FACTORY);
    }

    static Map<String, String> readMapStrings(XContentParser parser) throws IOException {
        return readGenericMap(parser, SIMPLE_MAP_STRINGS_FACTORY, XContentParser::text);
    }

    static List<Object> readList(XContentParser parser) throws IOException {
        return readList(parser, SIMPLE_MAP_FACTORY);
    }

    static List<Object> readListOrderedMap(XContentParser parser) throws IOException {
        return readList(parser, ORDERED_MAP_FACTORY);
    }

    static Map<String, Object> readMap(XContentParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        return readGenericMap(parser, mapFactory, p -> readValue(p, mapFactory));
    }

    static <T> Map<String, T> readGenericMap(
            XContentParser parser,
            Supplier<Map<String, T>> mapFactory,
            CheckedFunction<XContentParser, T, IOException> mapValueParser) throws IOException {
        Map<String, T> map = mapFactory.get();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            parser.nextToken();
            T value = mapValueParser.apply(parser);
            map.put(fieldName, value);
        }
        return map;
    }

    static List<Object> readList(XContentParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        } else {
            throw new XContentParseException(parser.getTokenLocation(), "Failed to parse list:  expecting "
                    + XContentParser.Token.START_ARRAY + " but got " + token);
        }

        ArrayList<Object> list = new ArrayList<>();
        for (; token != null && token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
            list.add(readValue(parser, mapFactory));
        }
        return list;
    }

    public static Object readValue(XContentParser parser, Supplier<Map<String, Object>> mapFactory) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING: return parser.text();
            case VALUE_NUMBER: return parser.numberValue();
            case VALUE_BOOLEAN: return parser.booleanValue();
            case START_OBJECT: return readMap(parser, mapFactory);
            case START_ARRAY: return readList(parser, mapFactory);
            case VALUE_EMBEDDED_OBJECT: return parser.binaryValue();
            case VALUE_NULL:
            default: return null;
        }
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return xContentRegistry.parseNamedObject(categoryClass, name, this, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    @Override
    public abstract boolean isClosed();

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return deprecationHandler;
    }
}
