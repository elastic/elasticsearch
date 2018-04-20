/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.support;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class MetadataUtils {

    public static final String RESERVED_PREFIX = "_";
    public static final String RESERVED_METADATA_KEY = RESERVED_PREFIX + "reserved";
    public static final Map<String, Object> DEFAULT_RESERVED_METADATA = Collections.singletonMap(RESERVED_METADATA_KEY, true);

    private MetadataUtils() {
    }

    public static void writeValue(StringBuilder sb, Object object) {
        if (object == null) {
            sb.append(object);
        } else if (object instanceof Map) {
            sb.append("{");
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) object).entrySet()) {
                sb.append(entry.getKey()).append("=");
                writeValue(sb, entry.getValue());
            }
            sb.append("}");

        } else if (object instanceof Collection) {
            sb.append("[");
            boolean first = true;
            for (Object item : (Collection) object) {
                if (!first) {
                    sb.append(",");
                }
                writeValue(sb, item);
                first = false;
            }
            sb.append("]");
        } else if (object.getClass().isArray()) {
            sb.append("[");
            for (int i = 0; i < Array.getLength(object); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                writeValue(sb, Array.get(object, i));
            }
            sb.append("]");
        } else {
            sb.append(object);
        }
    }

    public static void verifyNoReservedMetadata(Map<String, Object> metadata) {
        for (String key : metadata.keySet()) {
            if (key.startsWith(RESERVED_PREFIX)) {
                throw new IllegalArgumentException("invalid user metadata. [" + key + "] is a reserved for internal use");
            }
        }
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
