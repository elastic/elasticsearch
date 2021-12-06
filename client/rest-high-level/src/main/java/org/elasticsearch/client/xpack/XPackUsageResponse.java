/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.xpack;

import org.elasticsearch.xcontent.XContentParser;

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
        return (Map<String, Object>) value;
    }

    /** Return a map from feature name to usage information for that feature. */
    public Map<String, Map<String, Object>> getUsages() {
        return usages;
    }

    public static XPackUsageResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> rawMap = parser.map();
        Map<String, Map<String, Object>> usages = rawMap.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> castMap(e.getValue())));
        return new XPackUsageResponse(usages);
    }
}
