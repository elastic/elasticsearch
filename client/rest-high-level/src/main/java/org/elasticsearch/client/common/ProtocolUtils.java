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

package org.elasticsearch.client.common;

import java.util.Arrays;
import java.util.Map;

/**
 * Common utilities used for XPack protocol classes
 */
public final class ProtocolUtils {

    /**
     * Implements equals for a map of string arrays
     *
     * The map of string arrays is used in some XPack protocol classes but does't work with equal.
     */
    public static boolean equals(Map<String, String[]> a, Map<String, String[]> b) {
        if (a == null) {
            return b == null;
        }
        if (b == null) {
            return false;
        }
        if (a.size() != b.size()) {
            return false;
        }
        for (Map.Entry<String, String[]> entry : a.entrySet()) {
            String[] val = entry.getValue();
            String key = entry.getKey();
            if (val == null) {
                if (b.get(key) != null || b.containsKey(key) == false) {
                    return false;
                }
            } else {
                if (Arrays.equals(val, b.get(key)) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Implements hashCode for map of string arrays
     *
     * The map of string arrays does't work with hashCode.
     */
    public static int hashCode(Map<String, String[]> a) {
        int hash = 0;
        for (Map.Entry<String, String[]> entry : a.entrySet())
            hash += Arrays.hashCode(entry.getValue());
        return hash;
    }
}
