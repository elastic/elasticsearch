/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.cluster.node;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class DiscoveryNodeFilters {

    public static final DiscoveryNodeFilters NO_FILTERS = new DiscoveryNodeFilters(ImmutableMap.<String, String[]>of());

    public static DiscoveryNodeFilters buildFromSettings(String prefix, Settings settings) {
        return buildFromKeyValue(settings.getByPrefix(prefix).getAsMap());
    }

    public static DiscoveryNodeFilters buildFromKeyValue(Map<String, String> filters) {
        Map<String, String[]> bFilters = new HashMap<String, String[]>();
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            bFilters.put(entry.getKey(), Strings.splitStringByCommaToArray(entry.getValue()));
        }
        if (bFilters.isEmpty()) {
            return NO_FILTERS;
        }
        return new DiscoveryNodeFilters(bFilters);
    }

    private final Map<String, String[]> filters;

    DiscoveryNodeFilters(Map<String, String[]> filters) {
        this.filters = filters;
    }

    public boolean match(DiscoveryNode node) {
        if (filters.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            if ("_ip".equals(attr)) {
                if (!(node.address() instanceof InetSocketTransportAddress)) {
                    return false;
                }
                InetSocketTransportAddress inetAddress = (InetSocketTransportAddress) node.address();
                for (String value : values) {
                    if (Regex.simpleMatch(value, inetAddress.address().getAddress().getHostAddress())) {
                        return true;
                    }
                }
                return false;
            } else if ("_id".equals(attr)) {
                for (String value : values) {
                    if (node.id().equals(value)) {
                        return true;
                    }
                }
                return false;
            } else if ("_name".equals(attr) || "name".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.name())) {
                        return true;
                    }
                }
                return false;
            } else {
                String nodeAttributeValue = node.attributes().get(attr);
                if (nodeAttributeValue == null) {
                    return false;
                }
                for (String value : values) {
                    if (Regex.simpleMatch(value, nodeAttributeValue)) {
                        return true;
                    }
                }
                return false;
            }
        }
        return true;
    }
}
