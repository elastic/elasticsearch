/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.json;

import org.codehaus.jackson.JsonNode;

/**
 * @author kimchy (shay.banon)
 */
public class JacksonNodes {

    public static float nodeFloatValue(JsonNode node) {
        if (node.isNumber()) {
            return node.getNumberValue().floatValue();
        }
        String value = node.getTextValue();
        return Float.parseFloat(value);
    }

    public static double nodeDoubleValue(JsonNode node) {
        if (node.isNumber()) {
            return node.getNumberValue().doubleValue();
        }
        String value = node.getTextValue();
        return Double.parseDouble(value);
    }

    public static int nodeIntegerValue(JsonNode node) {
        if (node.isNumber()) {
            return node.getNumberValue().intValue();
        }
        String value = node.getTextValue();
        return Integer.parseInt(value);
    }

    public static long nodeLongValue(JsonNode node) {
        if (node.isNumber()) {
            return node.getNumberValue().longValue();
        }
        String value = node.getTextValue();
        return Long.parseLong(value);
    }

    public static boolean nodeBooleanValue(JsonNode node) {
        if (node.isBoolean()) {
            return node.getBooleanValue();
        }
        String value = node.getTextValue();
        return !(value.equals("false") || value.equals("0") || value.equals("off"));
    }
}
