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
package org.elasticsearch.test.rest.json;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.Stash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Holds a json object and allows to extract specific values from it
 */
public class JsonPath {

    final String json;
    final Map<String, Object> jsonMap;

    public JsonPath(String json) throws IOException {
        this.json = json;
        this.jsonMap = convertToMap(json);
    }

    private static Map<String, Object> convertToMap(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(json)) {
            return parser.mapOrdered();
        }
    }

    /**
     * Returns the object corresponding to the provided path if present, null otherwise
     */
    public Object evaluate(String path) {
        return evaluate(path, Stash.EMPTY);
    }

    /**
     * Returns the object corresponding to the provided path if present, null otherwise
     */
    public Object evaluate(String path, Stash stash) {
        String[] parts = parsePath(path);
        Object object = jsonMap;
        for (String part : parts) {
            object = evaluate(part, object, stash);
            if (object == null) {
                return null;
            }
        }
        return object;
    }

    @SuppressWarnings("unchecked")
    private Object evaluate(String key, Object object, Stash stash) {
        if (stash.isStashedValue(key)) {
            key = stash.unstashValue(key).toString();
        }

        if (object instanceof Map) {
            return ((Map<String, Object>) object).get(key);
        }
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            try {
                return list.get(Integer.valueOf(key));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("element was a list, but [" + key + "] was not numeric", e);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException("element was a list with " + list.size() + " elements, but [" + key + "] was out of bounds", e);
            }
        }

        throw new IllegalArgumentException("no object found for [" + key + "] within object of class [" + object.getClass() + "]");
    }

    private String[] parsePath(String path) {
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

        return list.toArray(new String[list.size()]);
    }
}
