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

package org.elasticsearch.test;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

public final class XContentTestUtils {
    private XContentTestUtils() {

    }

    public static Map<String, Object> convertToMap(ToXContent part) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        part.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(builder.bytes(), false, builder.contentType()).v2();
    }


    /**
     * Compares to maps generated from XContentObjects. The order of elements in arrays is ignored.
     *
     * @return null if maps are equal or path to the element where the difference was found
     */
    public static String differenceBetweenMapsIgnoringArrayOrder(Map<String, Object> first, Map<String, Object> second) {
        return differenceBetweenMapsIgnoringArrayOrder("", first, second);
    }

    private static String differenceBetweenMapsIgnoringArrayOrder(String path, Map<String, Object> first, Map<String, Object> second) {
        if (first.size() != second.size()) {
            return path + ": sizes of the maps don't match: " + first.size() + " != " + second.size();
        }

        for (String key : first.keySet()) {
            String reason = differenceBetweenObjectsIgnoringArrayOrder(path + "/" + key, first.get(key), second.get(key));
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String differenceBetweenObjectsIgnoringArrayOrder(String path, Object first, Object second) {
        if (first == null) {
            if (second == null) {
                return null;
            } else {
                return path + ": first element is null, the second element is not null";
            }
        } else if (first instanceof List) {
            if (second instanceof List) {
                List<Object> secondList = new ArrayList<>((List<Object>) second);
                List<Object> firstList = (List<Object>) first;
                if (firstList.size() == secondList.size()) {
                    String reason = path + ": no matches found";
                    for (Object firstObj : firstList) {
                        boolean found = false;
                        for (Object secondObj : secondList) {
                            reason = differenceBetweenObjectsIgnoringArrayOrder(path + "/*", firstObj, secondObj);
                            if (reason == null) {
                                secondList.remove(secondObj);
                                found = true;
                                break;
                            }
                        }
                        if (found == false) {
                            return reason;
                        }
                    }
                    if (secondList.isEmpty()) {
                        return null;
                    } else {
                        return path + ": the second list is not empty";
                    }
                } else {
                    return path + ": sizes of the arrays don't match: " + firstList.size() + " != " + secondList.size();
                }
            } else {
                return path + ": the second element is not an array";
            }
        } else if (first instanceof Map) {
            if (second instanceof Map) {
                return differenceBetweenMapsIgnoringArrayOrder(path, (Map<String, Object>) first, (Map<String, Object>) second);
            } else {
                return path + ": the second element is not a map (got " + second +")";
            }
        } else {
            if (first.equals(second)) {
                return null;
            } else {
                return path + ": the elements don't match: [" + first + "] != [" + second + "]";
            }

        }
    }

    /**
     * This utility method takes an XContentParser and walks the xContent structure to find all
     * possible paths to where a new object or array starts. This can be used in tests that add random
     * xContent values to test parsing code for errors or to check their robustness against new fields.
     *
     * The path uses dot separated fieldnames and numbers for array indices, similar to what we do in
     * {@link ObjectPath}.
     *
     * As an example, the following json xContent:
     * <pre>
     *     {
     *         "foo" : "bar",
     *         "foo1" : [ 1, { "foo2" : "baz" }, 3, 4]
     *         "foo3" : {
     *             "foo4" : {
     *                  "foo5": "buzz"
     *             }
     *         }
     *     }
     * </pre>
     *
     * Would return the following list:
     *
     * <ul>
     *  <li>"" (the empty string is the path to the root object)</li>
     *  <li>"foo1.1"</li>
     *  <li>"foo3</li>
     *  <li>"foo3.foo4</li>
     * </ul>
     */
    static List<String> getInsertPaths(XContentParser parser) throws IOException{
        assert parser.currentToken() == null : "the parsers token cursor should be before the first token";
        assert parser.nextToken() == XContentParser.Token.START_OBJECT;
        return getInsertPaths(parser, new Stack<String>());
    }

    private static List<String> getInsertPaths(XContentParser parser, Stack<String> currentPath) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT || parser.currentToken() == XContentParser.Token.START_ARRAY :
            "should only be called when new objects or arrays start";
        List<String> validPaths = new ArrayList<>();
        // parser.currentName() can be null for root object and unnamed objects in arrays
        if (parser.currentName() != null) {
            currentPath.push(parser.currentName());
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            validPaths.add(String.join(".", currentPath.toArray(new String[currentPath.size()])));
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.START_OBJECT
                        || parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    validPaths.addAll(getInsertPaths(parser, currentPath));
                }
            }
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            int itemCount = 0;
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() == XContentParser.Token.START_OBJECT
                        || parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    currentPath.push(Integer.toString(itemCount));
                    validPaths.addAll(getInsertPaths(parser, currentPath));
                    currentPath.pop();
                }
                itemCount++;
            }
        }
        if (parser.currentName() != null) {
            currentPath.pop();
        }
        return validPaths;
    }

    /**
     * Inserts key/value pairs into xContent passed in as {@link BytesReference} and returns a new {@link XContentBuilder}
     * The paths argument uses dot separated fieldnames and numbers for array indices, similar to what we do in
     * {@link ObjectPath}.
     * The key/value arguments can suppliers that either return fixed or random values.
     */
    static XContentBuilder insertIntoXContent(XContent xContent, BytesReference original, List<String> paths, Supplier<String> key,
            Supplier<Object> value) throws IOException {
        ObjectPath object = ObjectPath.createFromXContent(xContent, original);
        for (String path : paths) {
            Map<String, Object> insertMap = object.evaluate(path);
            insertMap.put(key.get(), value.get());
        }
        return object.toXContentBuilder(xContent);
    }
}
