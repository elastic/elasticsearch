/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.Map;

public enum IngestPipelineFieldAccessPattern {
    /**
     * Field names will be split on the `.` character into their contingent parts. Resolution will strictly check
     * for nested objects following the field path.
     */
    CLASSIC("classic"),
    /**
     * Field names will be split on the `.` character into their contingent parts. Resolution will flexibly check
     * for nested objects following the field path. If nested objects are not found for a key, the access pattern
     * will fall back to joining subsequent path elements together until it finds the next object that matches the
     * concatenated path. Allows for simple resolution of dotted field names.
     */
    FLEXIBLE("flexible");

    private final String key;

    IngestPipelineFieldAccessPattern(String key) {
        this.key = key;
    }

    private static final Map<String, IngestPipelineFieldAccessPattern> NAME_REGISTRY = Map.of(CLASSIC.key, CLASSIC, FLEXIBLE.key, FLEXIBLE);

    public static boolean isValidAccessPattern(String accessPatternName) {
        return NAME_REGISTRY.containsKey(accessPatternName);
    }

    public static IngestPipelineFieldAccessPattern getAccessPattern(String accessPatternName) {
        IngestPipelineFieldAccessPattern accessPattern = NAME_REGISTRY.get(accessPatternName);
        if (accessPattern == null) {
            throw new IllegalArgumentException("Invalid ingest pipeline access pattern name [" + accessPatternName + "] given");
        }
        return accessPattern;
    }
}
