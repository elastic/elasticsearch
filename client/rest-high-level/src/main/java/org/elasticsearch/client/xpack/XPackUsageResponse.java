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

package org.elasticsearch.client.xpack;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Response object from calling the xpack usage api.
 *
 * Usage information for each feature is accessible through {@link #getUsages()}.
 */
public class XPackUsageResponse {

    private final Map<String, Map<String, Object>> usages;

    private XPackUsageResponse(Map<String, Map<String, Object>> usages) {
        this.usages = usages;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>)value;
    }

    /** Return a map from feature name to usage information for that feature. */
    public Map<String, Map<String, Object>> getUsages() {
        return usages;
    }

    public static XPackUsageResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> rawMap = parser.map();
        Map<String, Map<String, Object>> usages = rawMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> castMap(e.getValue())));
        return new XPackUsageResponse(usages);
    }
}
