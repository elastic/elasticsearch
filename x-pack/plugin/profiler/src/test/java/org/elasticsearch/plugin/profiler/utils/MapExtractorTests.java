/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.profiler.utils;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

public class MapExtractorTests extends ESTestCase {
    public void testReadNullPaths() {
        assertNull(MapExtractor.read(null, "a", "b", "c"));
        assertNull(MapExtractor.read(Map.of("a", Collections.singletonMap("b", null)), "a", "b", "c"));
    }

    public void testReadRegularPaths() {
        assertEquals(Long.valueOf(7), MapExtractor.read(Map.of("a", Map.of("b", 7L)), "a", "b"));
        assertEquals("a value", MapExtractor.read(Map.of("a", Map.of("b", "a value")), "a", "b"));
    }
}
