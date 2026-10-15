/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class ConfiguredTests extends ESTestCase {

    public void testEmptyConfigReturnsEmptyConsumedKeys() {
        Configured<String> result = Configured.fromKnownSubset("v", Map.of(), Set.of("a", "b"));
        assertEquals("v", result.value());
        assertThat(result.consumedKeys(), empty());
    }

    public void testNullConfigReturnsEmptyConsumedKeys() {
        Configured<String> result = Configured.fromKnownSubset("v", null, Set.of("a", "b"));
        assertEquals("v", result.value());
        assertThat(result.consumedKeys(), empty());
    }

    public void testPartialOverlapReturnsOnlyRecognizedKeys() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("a", 1);
        config.put("not_recognized", 2);
        config.put("b", 3);
        Configured<String> result = Configured.fromKnownSubset("v", config, Set.of("a", "b", "c"));
        assertEquals("v", result.value());
        assertThat(result.consumedKeys(), containsInAnyOrder("a", "b"));
    }

    public void testEmptyConsumedKeysAccepted() {
        Configured<String> result = new Configured<>("v", Set.of(), "", "");
        assertEquals("v", result.value());
        assertThat(result.consumedKeys(), empty());
    }

    public void testNullConsumedKeysCoercedToEmpty() {
        Configured<String> result = new Configured<>("v", null, null, null);
        assertEquals("v", result.value());
        assertThat(result.consumedKeys(), empty());
    }

    public void testEmptyFactoryHasNoConsumedKeys() {
        Configured<String> result = Configured.empty("v");
        assertEquals("v", result.value());
        assertThat(result.consumedKeys(), empty());
    }

    /**
     * Three participants' identities become one key component, so the fold must be injective: if two different
     * triples could produce one value, two reads that differ would share a record. Each part is length-prefixed,
     * so a part that happens to contain the prefix syntax cannot imitate a boundary.
     */
    public void testFoldingIdentitiesCannotCollide() {
        assertNotEquals(Configured.fold("a", "bc", ""), Configured.fold("ab", "c", ""));
        assertNotEquals(Configured.fold("", "a", "bc"), Configured.fold("", "ab", "c"));
        assertNotEquals(Configured.fold("1:a", "", ""), Configured.fold("", "a", ""));
        assertNotEquals(Configured.fold("a", "", ""), Configured.fold("", "", "a"));
        assertEquals(Configured.fold("a", "b", "c"), Configured.fold("a", "b", "c"));
        assertNotEquals("an absent part is not an empty one", Configured.fold("a", null, "c"), Configured.fold("a", "", "c"));
    }
}
