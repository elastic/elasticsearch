/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.cache.DatasetIdentity;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class ConfigKeyValidatorTests extends ESTestCase {

    public void testNullConfigIsAccepted() {
        ConfigKeyValidator.check(null, List.of(Set.of()));
    }

    public void testEmptyConfigIsAccepted() {
        ConfigKeyValidator.check(Map.of(), List.of(Set.of()));
    }

    public void testFullyClaimedConfigIsAccepted() {
        Map<String, Object> config = Map.of("a", 1, "b", 2);
        ConfigKeyValidator.check(config, List.of(Set.of("a"), Set.of("b")));
    }

    public void testUnknownKeyIsRejected() {
        Map<String, Object> config = Map.of("a", 1, "typo", 2);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ConfigKeyValidator.check(config, List.of(Set.of("a")))
        );
        assertThat(e.getMessage(), allOf(containsString("typo"), containsString("unknown option ")));
    }

    public void testMultipleUnknownsReportedSorted() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("zebra_typo", 1);
        config.put("alpha_typo", 2);
        config.put("known", 3);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ConfigKeyValidator.check(config, List.of(Set.of("known")))
        );
        String msg = e.getMessage();
        assertThat(msg, allOf(containsString("alpha_typo"), containsString("zebra_typo")));
        assertTrue("alpha_typo should appear before zebra_typo in [" + msg + "]", msg.indexOf("alpha_typo") < msg.indexOf("zebra_typo"));
    }

    public void testRecognisedSetUnionMentionedInError() {
        Map<String, Object> config = Map.of("typo", 1, "a", 1, "b", 2);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ConfigKeyValidator.check(config, List.of(Set.of("a"), Set.of("b"), Set.of("c")))
        );
        assertThat(e.getMessage(), allOf(containsString("a"), containsString("b"), containsString("c")));
    }

    public void testSingleVsPluralWording() {
        IllegalArgumentException one = expectThrows(
            IllegalArgumentException.class,
            () -> ConfigKeyValidator.check(Map.of("typo", 1), List.of(Set.of()))
        );
        assertThat(one.getMessage(), containsString("unknown option ["));

        Map<String, Object> two = new HashMap<>();
        two.put("typo_a", 1);
        two.put("typo_b", 2);
        IllegalArgumentException many = expectThrows(
            IllegalArgumentException.class,
            () -> ConfigKeyValidator.check(two, List.of(Set.of()))
        );
        assertThat(many.getMessage(), containsString("unknown options ["));
    }

    public void testNoClaimedSetsRejectsAnyConfigKey() {
        Map<String, Object> config = Map.of("anything", 1);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ConfigKeyValidator.check(config, List.of()));
        assertThat(e.getMessage(), allOf(containsString("anything"), containsString("no options are recognised in this context")));
    }

    /**
     * A user-typed {@code _definition_version} must not survive into a relation's config. The check deliberately
     * skips framework keys, so nothing else rejects one a user supplied, and the value is read straight out of the
     * map by {@link DatasetIdentity#definitionVersionOf} — so a forged value equal to a registered dataset's version
     * would address that dataset's entries.
     * <p>
     * Both halves are asserted against the production reader rather than against the map, so this fails if the strip
     * stops happening AND if that reader stops honouring it.
     */
    public void testAForgedDefinitionVersionDoesNotReachACacheKey() {
        Map<String, Object> typed = new HashMap<>();
        typed.put("format", "csv");
        typed.put(DefinitionVersion.CONFIG_KEY, "deadbeefdeadbeefdeadbeefdeadbeef");

        assertEquals(
            "the reader honours whatever is in the map, which is why the map must be stripped",
            "deadbeefdeadbeefdeadbeefdeadbeef",
            DatasetIdentity.definitionVersionOf(typed)
        );
        assertEquals("", DatasetIdentity.definitionVersionOf(ConfigKeyValidator.withoutFrameworkKeys(typed)));
    }

    /** Stripping is by the prefix, not by a list of names, and it leaves the user's own settings alone. */
    public void testFrameworkKeysAreStrippedByPrefixAndNothingElseIs() {
        Map<String, Object> typed = new HashMap<>();
        typed.put("format", "csv");
        typed.put("header_row", true);
        typed.put("_datasource", Map.of("endpoint", "http://example"));
        typed.put("_some_key_added_later", "x");

        Map<String, Object> stripped = ConfigKeyValidator.withoutFrameworkKeys(typed);
        assertEquals(Set.of("format", "header_row"), stripped.keySet());
        assertEquals("csv", stripped.get("format"));
        assertEquals(true, stripped.get("header_row"));
    }

    /** A map with no framework key is returned as-is, so the common path allocates nothing. */
    public void testAMapWithNoFrameworkKeyIsReturnedUnchanged() {
        Map<String, Object> typed = Map.of("format", "csv");
        assertSame(typed, ConfigKeyValidator.withoutFrameworkKeys(typed));
        assertNull(ConfigKeyValidator.withoutFrameworkKeys(null));
    }
}
