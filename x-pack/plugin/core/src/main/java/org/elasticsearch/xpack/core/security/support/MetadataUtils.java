/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import java.util.Map;

public class MetadataUtils {

    public static final String RESERVED_PREFIX = "_";
    public static final String RESERVED_METADATA_KEY = RESERVED_PREFIX + "reserved";
    public static final String DEPRECATED_METADATA_KEY = RESERVED_PREFIX + "deprecated";
    public static final String DEPRECATED_REASON_METADATA_KEY = RESERVED_PREFIX + "deprecated_reason";
    public static final Map<String, Object> DEFAULT_RESERVED_METADATA = Map.of(RESERVED_METADATA_KEY, true);

    private MetadataUtils() {}

    public static boolean containsReservedMetadata(Map<String, Object> metadata) {
        for (String key : metadata.keySet()) {
            if (key.startsWith(RESERVED_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Object> getDeprecatedReservedMetadata(String reason) {
        return Map.of(RESERVED_METADATA_KEY, true, DEPRECATED_METADATA_KEY, true, DEPRECATED_REASON_METADATA_KEY, reason);
    }
}
