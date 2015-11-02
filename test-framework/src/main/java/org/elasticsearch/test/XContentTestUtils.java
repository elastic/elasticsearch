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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

public final class XContentTestUtils {
    private XContentTestUtils() {

    }

    public static Map<String, Object> convertToMap(ToXContent part) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        part.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(builder.bytes(), false).v2();
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
                return path + ": the second element is not a map";
            }
        } else {
            if (first.equals(second)) {
                return null;
            } else {
                return path + ": the elements don't match: [" + first + "] != [" + second + "]";
            }

        }
    }

}
