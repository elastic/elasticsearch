/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

/**
 * Utility for parsing HTTP Content-Range headers (RFC 7233).
 * Used by storage providers to extract total object size from range responses
 * when metadata requests (HEAD) are denied.
 * <p>
 * Parses the total size from headers of the form {@code bytes 0-0/12345}.
 * Returns null when the header is null, missing the total, or uses wildcard (*).
 */
public final class ContentRangeParser {

    private ContentRangeParser() {}

    public static Long parseTotalLength(String contentRange) {
        if (contentRange == null || contentRange.contains("/") == false) {
            return null;
        }
        String[] parts = contentRange.split("/");
        if (parts.length == 2 && "*".equals(parts[1]) == false) {
            try {
                return Long.parseLong(parts[1].trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
