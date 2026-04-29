/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class DataSourceConfigDefinitionTests extends ESTestCase {

    public void testPlaintextDefaults() {
        DataSourceConfigDefinition def = DataSourceConfigDefinition.plaintext("region");
        assertEquals("region", def.name());
        assertFalse(def.secret());
        assertFalse(def.caseInsensitive());
    }

    public void testSecretDefaults() {
        DataSourceConfigDefinition def = DataSourceConfigDefinition.secret("access_key");
        assertEquals("access_key", def.name());
        assertTrue(def.secret());
        assertFalse(def.caseInsensitive());
    }

    public void testAsCaseInsensitiveOnPlaintext() {
        DataSourceConfigDefinition def = DataSourceConfigDefinition.plaintext("auth").asCaseInsensitive();
        assertEquals("auth", def.name());
        assertFalse(def.secret());
        assertTrue(def.caseInsensitive());
    }

    public void testAsCaseInsensitivePreservesSecretBit() {
        // The secret classification must survive the wither — losing it would silently expose credentials.
        DataSourceConfigDefinition def = DataSourceConfigDefinition.secret("password").asCaseInsensitive();
        assertEquals("password", def.name());
        assertTrue(def.secret());
        assertTrue(def.caseInsensitive());
    }

    public void testAsCaseInsensitiveReturnsCopy() {
        DataSourceConfigDefinition original = DataSourceConfigDefinition.plaintext("auth");
        DataSourceConfigDefinition modified = original.asCaseInsensitive();
        assertNotSame(original, modified);
        assertFalse(original.caseInsensitive());
        assertTrue(modified.caseInsensitive());
    }

    public void testMapOfKeysByName() {
        DataSourceConfigDefinition a = DataSourceConfigDefinition.plaintext("a");
        DataSourceConfigDefinition b = DataSourceConfigDefinition.secret("b");
        Map<String, DataSourceConfigDefinition> map = DataSourceConfigDefinition.mapOf(a, b);
        assertEquals(2, map.size());
        assertSame(a, map.get("a"));
        assertSame(b, map.get("b"));
    }

    public void testMapOfReturnsUnmodifiable() {
        Map<String, DataSourceConfigDefinition> map = DataSourceConfigDefinition.mapOf(DataSourceConfigDefinition.plaintext("a"));
        expectThrows(UnsupportedOperationException.class, () -> map.put("x", DataSourceConfigDefinition.plaintext("x")));
    }

    public void testMapOfRejectsDuplicateNames() {
        // Collectors.toUnmodifiableMap throws on duplicate keys without a merger — locks in fail-fast
        // behavior so a plugin author can't accidentally register the same field name twice with
        // different sensitivity classifications.
        DataSourceConfigDefinition first = DataSourceConfigDefinition.plaintext("region");
        DataSourceConfigDefinition second = DataSourceConfigDefinition.secret("region");
        expectThrows(IllegalStateException.class, () -> DataSourceConfigDefinition.mapOf(first, second));
    }
}
