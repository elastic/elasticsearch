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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
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
     * Compares to maps generated from XContentObjects. The order of elements in arrays is ignored
     */
    public static boolean mapsEqualIgnoringArrayOrder(Map<String, Object> first, Map<String, Object> second) {
        if (first.size() != second.size()) {
            return false;
        }

        for (String key : first.keySet()) {
            if (objectsEqualIgnoringArrayOrder(first.get(key), second.get(key)) == false) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean objectsEqualIgnoringArrayOrder(Object first, Object second) {
        if (first == null ) {
            return second == null;
        } else if (first instanceof List) {
            if (second instanceof  List) {
                List<Object> secondList = Lists.newArrayList((List<Object>) second);
                List<Object> firstList = (List<Object>) first;
                if (firstList.size() == secondList.size()) {
                    for (Object firstObj : firstList) {
                        boolean found = false;
                        for (Object secondObj : secondList) {
                            if (objectsEqualIgnoringArrayOrder(firstObj, secondObj)) {
                                secondList.remove(secondObj);
                                found = true;
                                break;
                            }
                        }
                        if (found == false) {
                            return false;
                        }
                    }
                    return secondList.isEmpty();
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if (first instanceof Map) {
            if (second instanceof Map) {
                return mapsEqualIgnoringArrayOrder((Map<String, Object>) first, (Map<String, Object>) second);
            } else {
                return false;
            }
        } else {
            return first.equals(second);
        }
    }

}
