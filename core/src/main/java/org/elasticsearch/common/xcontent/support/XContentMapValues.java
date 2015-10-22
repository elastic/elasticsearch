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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class XContentMapValues {

    /**
     * Extracts raw values (string, int, and so on) based on the path provided returning all of them
     * as a single list.
     */
    public static List<Object> extractRawValues(String path, Map<String, Object> map) {
        List<Object> values = new ArrayList<>();
        String[] pathElements = Strings.splitStringToArray(path, '.');
        if (pathElements.length == 0) {
            return values;
        }
        extractRawValues(values, map, pathElements, 0);
        return values;
    }

    @SuppressWarnings({"unchecked"})
    private static void extractRawValues(List values, Map<String, Object> part, String[] pathElements, int index) {
        if (index == pathElements.length) {
            return;
        }

        String key = pathElements[index];
        Object currentValue = part.get(key);
        int nextIndex = index + 1;
        while (currentValue == null && nextIndex != pathElements.length) {
            key += "." + pathElements[nextIndex];
            currentValue = part.get(key);
            nextIndex++;
        }

        if (currentValue == null) {
            return;
        }

        if (currentValue instanceof Map) {
            extractRawValues(values, (Map<String, Object>) currentValue, pathElements, nextIndex);
        } else if (currentValue instanceof List) {
            extractRawValues(values, (List) currentValue, pathElements, nextIndex);
        } else {
            values.add(currentValue);
        }
    }

    @SuppressWarnings({"unchecked"})
    private static void extractRawValues(List values, List<Object> part, String[] pathElements, int index) {
        for (Object value : part) {
            if (value == null) {
                continue;
            }
            if (value instanceof Map) {
                extractRawValues(values, (Map<String, Object>) value, pathElements, index);
            } else if (value instanceof List) {
                extractRawValues(values, (List) value, pathElements, index);
            } else {
                values.add(value);
            }
        }
    }

    public static Object extractValue(String path, Map<String, Object> map) {
        String[] pathElements = Strings.splitStringToArray(path, '.');
        if (pathElements.length == 0) {
            return null;
        }
        return extractValue(pathElements, 0, map);
    }

    @SuppressWarnings({"unchecked"})
    private static Object extractValue(String[] pathElements, int index, Object currentValue) {
        if (index == pathElements.length) {
            return currentValue;
        }
        if (currentValue == null) {
            return null;
        }
        if (currentValue instanceof Map) {
            Map map = (Map) currentValue;
            String key = pathElements[index];
            Object mapValue = map.get(key);
            int nextIndex = index + 1;
            while (mapValue == null && nextIndex != pathElements.length) {
                key += "." + pathElements[nextIndex];
                mapValue = map.get(key);
                nextIndex++;
            }
            return extractValue(pathElements, nextIndex, mapValue);
        }
        if (currentValue instanceof List) {
            List valueList = (List) currentValue;
            List newList = new ArrayList(valueList.size());
            for (Object o : valueList) {
                Object listValue = extractValue(pathElements, index, o);
                if (listValue != null) {
                    newList.add(listValue);
                }
            }
            return newList;
        }
        return null;
    }

    public static Map<String, Object> filter(Map<String, Object> map, String[] includes, String[] excludes) {
        Map<String, Object> result = new HashMap<>();
        filter(map, result, includes == null ? Strings.EMPTY_ARRAY : includes, excludes == null ? Strings.EMPTY_ARRAY : excludes, new StringBuilder());
        return result;
    }

    private static void filter(Map<String, Object> map, Map<String, Object> into, String[] includes, String[] excludes, StringBuilder sb) {
        if (includes.length == 0 && excludes.length == 0) {
            into.putAll(map);
            return;
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            int mark = sb.length();
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(key);
            String path = sb.toString();

            if (Regex.simpleMatch(excludes, path)) {
                sb.setLength(mark);
                continue;
            }

            boolean exactIncludeMatch = false; // true if the current position was specifically mentioned
            boolean pathIsPrefixOfAnInclude = false; // true if potentially a sub scope can be included
            if (includes.length == 0) {
                // implied match anything
                exactIncludeMatch = true;
            } else {
                for (String include : includes) {
                    // check for prefix matches as well to see if we need to zero in, something like: obj1.arr1.* or *.field
                    // note, this does not work well with middle matches, like obj1.*.obj3
                    if (include.charAt(0) == '*') {
                        if (Regex.simpleMatch(include, path)) {
                            exactIncludeMatch = true;
                            break;
                        }
                        pathIsPrefixOfAnInclude = true;
                        continue;
                    }
                    if (include.startsWith(path)) {
                        if (include.length() == path.length()) {
                            exactIncludeMatch = true;
                            break;
                        } else if (include.length() > path.length() && include.charAt(path.length()) == '.') {
                            // include might may match deeper paths. Dive deeper.
                            pathIsPrefixOfAnInclude = true;
                            continue;
                        }
                    }
                    if (Regex.simpleMatch(include, path)) {
                        exactIncludeMatch = true;
                        break;
                    }
                }
            }

            if (!(pathIsPrefixOfAnInclude || exactIncludeMatch)) {
                // skip subkeys, not interesting.
                sb.setLength(mark);
                continue;
            }


            if (entry.getValue() instanceof Map) {
                Map<String, Object> innerInto = new HashMap<>();
                // if we had an exact match, we want give deeper excludes their chance
                filter((Map<String, Object>) entry.getValue(), innerInto, exactIncludeMatch ? Strings.EMPTY_ARRAY : includes, excludes, sb);
                if (exactIncludeMatch || !innerInto.isEmpty()) {
                    into.put(entry.getKey(), innerInto);
                }
            } else if (entry.getValue() instanceof List) {
                List<Object> list = (List<Object>) entry.getValue();
                List<Object> innerInto = new ArrayList<>(list.size());
                // if we had an exact match, we want give deeper excludes their chance
                filter(list, innerInto, exactIncludeMatch ? Strings.EMPTY_ARRAY : includes, excludes, sb);
                into.put(entry.getKey(), innerInto);
            } else if (exactIncludeMatch) {
                into.put(entry.getKey(), entry.getValue());
            }
            sb.setLength(mark);
        }
    }

    private static void filter(List<Object> from, List<Object> to, String[] includes, String[] excludes, StringBuilder sb) {
        if (includes.length == 0 && excludes.length == 0) {
            to.addAll(from);
            return;
        }

        for (Object o : from) {
            if (o instanceof Map) {
                Map<String, Object> innerInto = new HashMap<>();
                filter((Map<String, Object>) o, innerInto, includes, excludes, sb);
                if (!innerInto.isEmpty()) {
                    to.add(innerInto);
                }
            } else if (o instanceof List) {
                List<Object> innerInto = new ArrayList<>();
                filter((List<Object>) o, innerInto, includes, excludes, sb);
                if (!innerInto.isEmpty()) {
                    to.add(innerInto);
                }
            } else {
                to.add(o);
            }
        }
    }

    public static boolean isObject(Object node) {
        return node instanceof Map;
    }

    public static boolean isArray(Object node) {
        return node instanceof List;
    }

    public static String nodeStringValue(Object node, String defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return node.toString();
    }

    public static float nodeFloatValue(Object node, float defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeFloatValue(node);
    }

    public static float nodeFloatValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).floatValue();
        }
        return Float.parseFloat(node.toString());
    }

    public static double nodeDoubleValue(Object node, double defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeDoubleValue(node);
    }

    public static double nodeDoubleValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).doubleValue();
        }
        return Double.parseDouble(node.toString());
    }

    public static int nodeIntegerValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).intValue();
        }
        return Integer.parseInt(node.toString());
    }

    public static int nodeIntegerValue(Object node, int defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        if (node instanceof Number) {
            return ((Number) node).intValue();
        }
        return Integer.parseInt(node.toString());
    }

    public static short nodeShortValue(Object node, short defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeShortValue(node);
    }

    public static short nodeShortValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).shortValue();
        }
        return Short.parseShort(node.toString());
    }

    public static byte nodeByteValue(Object node, byte defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeByteValue(node);
    }

    public static byte nodeByteValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).byteValue();
        }
        return Byte.parseByte(node.toString());
    }

    public static long nodeLongValue(Object node, long defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeLongValue(node);
    }

    public static long nodeLongValue(Object node) {
        if (node instanceof Number) {
            return ((Number) node).longValue();
        }
        return Long.parseLong(node.toString());
    }

    public static boolean nodeBooleanValue(Object node, boolean defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeBooleanValue(node);
    }

    public static boolean nodeBooleanValue(Object node) {
        if (node instanceof Boolean) {
            return (Boolean) node;
        }
        if (node instanceof Number) {
            return ((Number) node).intValue() != 0;
        }
        String value = node.toString();
        return !(value.equals("false") || value.equals("0") || value.equals("off"));
    }

    public static TimeValue nodeTimeValue(Object node, TimeValue defaultValue) {
        if (node == null) {
            return defaultValue;
        }
        return nodeTimeValue(node);
    }

    public static TimeValue nodeTimeValue(Object node) {
        if (node instanceof Number) {
            return TimeValue.timeValueMillis(((Number) node).longValue());
        }
        return TimeValue.parseTimeValue(node.toString(), null, XContentMapValues.class.getSimpleName() + ".nodeTimeValue");
    }

    public static Map<String, Object> nodeMapValue(Object node, String desc) {
        if (node instanceof Map) {
            return (Map<String, Object>) node;
        } else {
            throw new ElasticsearchParseException(desc + " should be a hash but was of type: " + node.getClass());
        }
    }

    /**
     * Returns an array of string value from a node value.
     *
     * If the node represents an array the corresponding array of strings is returned.
     * Otherwise the node is treated as a comma-separated string.
     */
    public static String[] nodeStringArrayValue(Object node) {
        if (isArray(node)) {
            List list = (List) node;
            String[] arr = new String[list.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = nodeStringValue(list.get(i), null);
            }
            return arr;
        } else {
            return Strings.splitStringByCommaToArray(node.toString());
        }
    }
}
