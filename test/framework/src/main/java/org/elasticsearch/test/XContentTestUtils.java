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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiOfLength;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentHelper.createParser;

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
     * This method takes the input xContent data and adds a random field value, inner object or array into each
     * json object. This can e.g. be used to test if parsers that handle the resulting xContent can handle the
     * augmented xContent correctly, for example when testing lenient parsing.
     *
     * If the xContent output contains objects that should be skipped of such treatment, an optional filtering
     * {@link Predicate} can be supplied that checks xContent paths that should be excluded from this treatment.
     *
     * This predicate should check the xContent path that we want to insert to and return <tt>true</tt> if the
     * path should be excluded. Paths are string concatenating field names and array indices, so e.g. in:
     *
     * <pre>
     * {
     *      "foo1 : {
     *          "bar" : [
     *              { ... },
     *              { ... },
     *              {
     *                  "baz" : {
     *                      // insert here
     *                  }
     *              }
     *          ]
     *      }
     * }
     * </pre>
     *
     * "foo1.bar.2.baz" would point to the desired insert location.
     *
     * To exclude inserting into the "foo1" object we would user a {@link Predicate} like
     * <pre>
     * {@code
     *      (path) -> path.endsWith("foo1")
     * }
     * </pre>
     *
     * or if we don't want any random insertions in the "foo1" tree we could use
     * <pre>
     * {@code
     *      (path) -> path.contains("foo1")
     * }
     * </pre>
     */
    public static BytesReference insertRandomFields(XContentType contentType, BytesReference xContent, Predicate<String> excludeFilter,
            Random random) throws IOException {
        List<String> insertPaths;

        // we can use NamedXContentRegistry.EMPTY here because we only traverse the xContent once and don't use it
        try (XContentParser parser = createParser(NamedXContentRegistry.EMPTY, xContent, contentType)) {
            parser.nextToken();
            List<String> possiblePaths = XContentTestUtils.getInsertPaths(parser, new Stack<>());
            if (excludeFilter == null) {
                insertPaths = possiblePaths;
            } else {
                insertPaths = new ArrayList<>();
                possiblePaths.stream().filter(excludeFilter.negate()).forEach(insertPaths::add);
            }
        }

        Supplier<Object> value = () -> {
            List<Object> randomValues = RandomObjects.randomStoredFieldValues(random, contentType).v1();
            if (random.nextBoolean()) {
                return randomValues.get(0);
            } else {
                if (random.nextBoolean()) {
                    return randomValues.stream().collect(Collectors.toMap(obj -> randomAsciiOfLength(random, 10), obj -> obj));
                } else {
                    return randomValues;
                }
            }
        };
        return XContentTestUtils
                .insertIntoXContent(contentType.xContent(), xContent, insertPaths, () -> randomAsciiOfLength(random, 10), value).bytes();
    }

    /**
     * This utility method takes an XContentParser and walks the xContent structure to find all
     * possible paths to where a new object or array starts. This can be used in tests that add random
     * xContent values to test parsing code for errors or to check their robustness against new fields.
     *
     * The path uses dot separated fieldnames and numbers for array indices, similar to what we do in
     * {@link ObjectPath}.
     *
     * The {@link Stack} passed in should initially be empty, it gets pushed to by recursive calls
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
    static List<String> getInsertPaths(XContentParser parser, Stack<String> currentPath) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT || parser.currentToken() == XContentParser.Token.START_ARRAY :
            "should only be called when new objects or arrays start";
        List<String> validPaths = new ArrayList<>();
        // parser.currentName() can be null for root object and unnamed objects in arrays
        if (parser.currentName() != null) {
            // dots in randomized field names need to be escaped, we use that character as the path separator
            currentPath.push(parser.currentName().replaceAll("\\.", "\\\\."));
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
