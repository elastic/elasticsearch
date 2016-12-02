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
package org.elasticsearch.client.highlevel;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TODO: started as a clone off ObjectPath in the yaml tests, maybe think about
 * extracting common functionality or rework this class to better fit the
 * purpose in the client project Holds an {@link XContent} object as {@link Map}
 */
public class XContentAccessor {

    protected final Object object;

    public static XContentAccessor createFromXContent(XContent xContent, String input) throws IOException {
        try (XContentParser parser = xContent.createParser(input)) {
            if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                return new XContentAccessor(parser.listOrderedMap());
            }
            return new XContentAccessor(parser.mapOrdered());
        }
    }

    public XContentAccessor(Map<String, Object> map) {
        this.object = map;
    }

    XContentAccessor(List<Object> list) {
        this.object = list;
    }

    public Object getObject() {
        return this.object;
    }

    /**
     * Returns the object corresponding to the provided path if present, null
     * otherwise
     */
    public Object evaluate(String path) {
        List<String> parts = parsePath(path);
        return evaluate(parts, object);
    }

    /**
     * Returns the String corresponding to the provided path if present, null otherwise
     */
    public String evaluateString(String path) {
        return (String) evaluate(path);
    }

    /**
     * Returns the String corresponding to the provided path if present, null otherwise
     */
    public Integer evaluateInteger(String path) {
        return (Integer) evaluate(path);
    }

    /**
     * Returns the Float corresponding to the provided path if present, null otherwise
     */
    public Float evaluateFloat(String path) {
        return (Float) evaluate(path);
    }

    /**
     * Returns the Double value corresponding to the provided path if present, null otherwise.
     */
    public Double evaluateDouble(String path) {
        return (Double) evaluate(path);
    }

    /**
     * Returns the object corresponding to the provided path if present, null
     * otherwise. If the object it an {@link Integer}, its long value is
     * returned. If it is neither {@link Integer}, {@link Long} or <tt>null</tt>
     * we throw an {@link IllegalArgumentException}
     */
    public Long evaluateLong(String path) {
        Object obj = evaluate(path);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        }
        if (obj instanceof Long) {
            return ((Long) obj);
        }
        throw new IllegalArgumentException("Object under [" + path + "] should be Integer, Long or null.");
    }

    @SuppressWarnings("unchecked")
    private static Object evaluate(List<String> keys, Object object) {
        if (keys.size() == 0 || object == null) {
            return object;
        }
        Object innerObject = null;
        String key = keys.get(0);
        if (object instanceof Map) {
            innerObject = ((Map<String, Object>) object).get(key);
        } else if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            try {
                innerObject = list.get(Integer.valueOf(key));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("element was a list, but [" + key + "] was not numeric", e);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                        "element was a list with " + list.size() + " elements, but [" + key + "] was out of bounds", e);
            }
        } else {
            throw new IllegalArgumentException("no object found for [" + key + "] within object of class [" + object.getClass() + "]");
        }
        // continue with rest of keys
        return evaluate(keys.subList(1, keys.size()), innerObject);
    }

    private static List<String> parsePath(String path) {
        List<String> list = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escape = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '\\') {
                escape = true;
                continue;
            }

            if (c == '.') {
                if (escape) {
                    escape = false;
                } else {
                    if (current.length() > 0) {
                        list.add(current.toString());
                        current.setLength(0);
                    }
                    continue;
                }
            }

            current.append(c);
        }

        if (current.length() > 0) {
            list.add(current.toString());
        }

        return list;
    }
}
