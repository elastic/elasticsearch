/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.support;

import java.util.Map;

public class MetadataUtils {

    public static final String RESERVED_PREFIX = "_";
    public static final String RESERVED_METADATA_KEY = RESERVED_PREFIX + "reserved";
    public static final Map<String, Object> DEFAULT_RESERVED_METADATA = Map.of(RESERVED_METADATA_KEY, true);

    private MetadataUtils() {
    }

    public static boolean containsReservedMetadata(Map<String, Object> metadata) {
        for (String key : metadata.keySet()) {
            if (key.startsWith(RESERVED_PREFIX)) {
                return true;
            }
        }
        return false;
    }
}
