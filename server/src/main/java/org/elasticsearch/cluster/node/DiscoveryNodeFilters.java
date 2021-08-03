/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class DiscoveryNodeFilters {

    static final Set<String> NON_ATTRIBUTE_NAMES = Set.of("_ip", "_host_ip", "_publish_ip", "host", "_id", "_name", "name");

    public enum OpType {
        AND,
        OR
    }

    /**
     * Validates the IP addresses in a group of {@link Settings} by looking for the keys
     * "_ip", "_host_ip", and "_publish_ip" and ensuring each of their comma separated values
     * that has no wildcards is a valid IP address.
     */
    public static final BiConsumer<String, String> IP_VALIDATOR = (propertyKey, rawValue) -> {
        if (rawValue != null) {
            if (propertyKey.endsWith("._ip") || propertyKey.endsWith("._host_ip") || propertyKey.endsWith("_publish_ip")) {
                for (String value : Strings.tokenizeToStringArray(rawValue, ",")) {
                    if (Regex.isSimpleMatchPattern(value) == false && InetAddresses.isInetAddress(value) == false) {
                        throw new IllegalArgumentException("invalid IP address [" + value + "] for [" + propertyKey + "]");
                    }
                }
            }
        }
    };

    public static DiscoveryNodeFilters buildFromKeyValue(OpType opType, Map<String, String> filters) {
        Map<String, String[]> bFilters = new HashMap<>();
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            String[] values = Strings.tokenizeToStringArray(entry.getValue(), ",");
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

    private boolean matchByIP(String[] values, @Nullable String hostIp, @Nullable String publishIp) {
        for (String value : values) {
            boolean matchIp = Regex.simpleMatch(value, hostIp) || Regex.simpleMatch(value, publishIp);
            if (matchIp) {
                return matchIp;
            }
        }
        return false;
    }

    /**
     * Removes any filters that should not be considered, returning a new
     * {@link DiscoveryNodeFilters} object. If the filtered object has no
     * filters after trimming, {@code null} is returned.
     */
    @Nullable
    public static DiscoveryNodeFilters trimTier(@Nullable DiscoveryNodeFilters original) {
        if (original == null) {
            return null;
        }

        Map<String, String[]> newFilters = original.filters.entrySet().stream()
            // Remove all entries that use "_tier_preference", as these will be handled elsewhere
            .filter(entry -> {
                String attr = entry.getKey();
                return attr != null && attr.equals("_tier_preference") == false;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (newFilters.size() == 0) {
            return null;
        } else {
            return new DiscoveryNodeFilters(original.opType, newFilters);
        }
    }

    public boolean match(DiscoveryNode node) {
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            if ("_ip".equals(attr)) {
                // We check both the host_ip or the publish_ip
                String publishAddress = null;
                if (node.getAddress() instanceof TransportAddress) {
                    publishAddress = NetworkAddress.format(node.getAddress().address().getAddress());
                }

                boolean match = matchByIP(values, node.getHostAddress(), publishAddress);

                if (opType == OpType.AND) {
                    if (match) {
                        // If we match, we can check to the next filter
                        continue;
                    }
                    return false;
                }

                if (match && opType == OpType.OR) {
                    return true;
                }
            } else if ("_host_ip".equals(attr)) {
                // We check explicitly only the host_ip
                boolean match = matchByIP(values, node.getHostAddress(), null);
                if (opType == OpType.AND) {
                    if (match) {
                        // If we match, we can check to the next filter
                        continue;
                    }
                    return false;
                }

                if (match && opType == OpType.OR) {
                    return true;
                }
            } else if ("_publish_ip".equals(attr)) {
                // We check explicitly only the publish_ip
                String address = null;
                if (node.getAddress() instanceof TransportAddress) {
                    address = NetworkAddress.format(node.getAddress().address().getAddress());
                }

                boolean match = matchByIP(values, address, null);
                if (opType == OpType.AND) {
                    if (match) {
                        // If we match, we can check to the next filter
                        continue;
                    }
                    return false;
                }

                if (match && opType == OpType.OR) {
                    return true;
                }
            } else if ("_host".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.getHostName()) || Regex.simpleMatch(value, node.getHostAddress())) {
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
                    if (node.getId().equals(value)) {
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
                    if (Regex.simpleMatch(value, node.getName())) {
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
                String nodeAttributeValue = node.getAttributes().get(attr);
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
     *
     * @return true if this filter only contains attribute values, i.e., no node specific info.
     */
    public boolean isOnlyAttributeValueFilter() {
        return filters.keySet().stream().anyMatch(NON_ATTRIBUTE_NAMES::contains) == false;
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
                    sb.append(" ").append(opType.toString()).append(" ");
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
