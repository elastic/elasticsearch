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

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class DiscoveryNodeFilters {

    public static enum OpType {
        AND,
        OR
    }

    ;

    public static DiscoveryNodeFilters buildFromSettings(OpType opType, String prefix, Settings settings) {
        return buildFromKeyValue(opType, settings.getByPrefix(prefix).getAsMap());
    }

    public static DiscoveryNodeFilters buildFromKeyValue(OpType opType, Map<String, String> filters) {
        Map<String, String[]> bFilters = new HashMap<>();
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            String[] values = Strings.splitStringByCommaToArray(entry.getValue());
            if (values.length > 0) {
                bFilters.put(entry.getKey(), values);
            }
        }
        if (bFilters.isEmpty()) {
            return null;
        }
        return new DiscoveryNodeFilters(opType, bFilters);
    }

    private final Map<String, String[]> filters;

    private final OpType opType;

    DiscoveryNodeFilters(OpType opType, Map<String, String[]> filters) {
        this.opType = opType;
        this.filters = filters;
    }

    public boolean match(DiscoveryNode node) {
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            if ("_ip".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.getHostAddress())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else if ("_host".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.getHostName())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                    if (Regex.simpleMatch(value, node.getHostAddress())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else if ("_id".equals(attr)) {
                for (String value : values) {
                    if (node.id().equals(value)) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else if ("_name".equals(attr) || "name".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.name())) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            } else {
                String nodeAttributeValue = node.attributes().get(attr);
                if (nodeAttributeValue == null) {
                    if (opType == OpType.AND) {
                        return false;
                    } else {
                        continue;
                    }
                }
                for (String value : values) {
                    if (Regex.simpleMatch(value, nodeAttributeValue)) {
                        if (opType == OpType.OR) {
                            return true;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            return false;
                        }
                    }
                }
            }
        }
        if (opType == OpType.OR) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Generates a human-readable string for the DiscoverNodeFilters.
     * Example: {@code _id:"id1 OR blah",name:"blah OR name2"}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int entryCount = filters.size();
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            sb.append(attr);
            sb.append(":\"");
            int valueCount = values.length;
            for (String value : values) {
                sb.append(value);
                if (valueCount > 1) {
                    sb.append(" " + opType.toString() + " ");
                }
                valueCount--;
            }
            sb.append("\"");
            if (entryCount > 1) {
                sb.append(",");
            }
            entryCount--;
        }
        return sb.toString();
    }
}
