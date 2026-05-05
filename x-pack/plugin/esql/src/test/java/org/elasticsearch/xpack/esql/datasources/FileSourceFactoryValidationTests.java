/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link FileSourceFactory#validateConfigKeys(Map, Set, Set)}: the planning-time
 * gate that rejects unknown WITH-clause options after storage and format layers have each had
 * a chance to claim their keys.
 */
public class FileSourceFactoryValidationTests extends ESTestCase {

    public void testNullConfigIsAccepted() {
        FileSourceFactory.validateConfigKeys(null, Set.of(), Set.of());
    }

    public void testEmptyConfigIsAccepted() {
        FileSourceFactory.validateConfigKeys(Map.of(), Set.of(), Set.of());
    }

    public void testFullyClaimedConfigIsAccepted() {
        Map<String, Object> config = Map.of("access_key", "ak", "delimiter", "|", "format", "csv");
        FileSourceFactory.validateConfigKeys(config, Set.of("access_key"), Set.of("delimiter"));
    }

    public void testStorageOnlyClaimsAreAccepted() {
        Map<String, Object> config = Map.of("access_key", "ak", "secret_key", "sk");
        FileSourceFactory.validateConfigKeys(config, Set.of("access_key", "secret_key"), Set.of());
    }

    public void testFormatOnlyClaimsAreAccepted() {
        Map<String, Object> config = Map.of("delimiter", "|", "header_row", false);
        FileSourceFactory.validateConfigKeys(config, Set.of(), Set.of("delimiter", "header_row"));
    }

    public void testCoordinatorKeysAreAlwaysAccepted() {
        // 'format' (coordinator-level format override) and the ErrorPolicy keys must pass through
        // even when neither storage nor format claims them.
        Map<String, Object> config = Map.of("format", "csv", "max_errors", "100", "max_error_ratio", "0.1", "error_mode", "skip_row");
        FileSourceFactory.validateConfigKeys(config, Set.of(), Set.of());
    }

    public void testUnknownKeyIsRejected() {
        Map<String, Object> config = Map.of("access_key", "ak", "headres_row", false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.validateConfigKeys(config, Set.of("access_key"), Set.of())
        );
        assertThat(e.getMessage(), allOf(containsString("headres_row"), containsString("unknown option")));
    }

    public void testMultipleUnknownKeysAreReportedSorted() {
        // Use LinkedHashMap with deliberate insertion order so we can prove the message sorts.
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("zebra_typo", true);
        config.put("alpha_typo", "x");
        config.put("access_key", "ak");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.validateConfigKeys(config, Set.of("access_key"), Set.of())
        );
        assertThat(e.getMessage(), containsString("[alpha_typo, zebra_typo]"));
    }

    public void testErrorMessageMentionsRecognisedKeys() {
        Map<String, Object> config = Map.of("typo", true, "access_key", "ak", "delimiter", "|");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.validateConfigKeys(config, Set.of("access_key"), Set.of("delimiter"))
        );
        // The recognised set is the union of consumed sets plus coordinator keys, sorted.
        assertThat(
            e.getMessage(),
            allOf(
                containsString("access_key"),
                containsString("delimiter"),
                containsString("error_mode"),
                containsString("max_errors"),
                containsString("max_error_ratio"),
                containsString("format")
            )
        );
    }

    public void testSingleVsPluralWording() {
        // Singular form for one unknown.
        IllegalArgumentException one = expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.validateConfigKeys(Map.of("typo", true), Set.of(), Set.of())
        );
        assertThat(one.getMessage(), containsString("unknown option ["));

        // Plural form for two or more.
        Map<String, Object> two = new HashMap<>();
        two.put("typo_a", 1);
        two.put("typo_b", 2);
        IllegalArgumentException many = expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.validateConfigKeys(two, Set.of(), Set.of())
        );
        assertThat(many.getMessage(), containsString("unknown options ["));
    }

    public void testCoordinatorKeysExposed() {
        // The COORDINATOR_KEYS constant must include 'format' plus all ErrorPolicy keys —
        // the validation contract depends on this. A missing entry would cause a real WITH-clause
        // option (e.g. error_mode) to be flagged as unknown.
        assertTrue("format must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(FileSourceFactory.CONFIG_FORMAT));
        for (String key : ErrorPolicy.CONFIG_KEYS) {
            assertTrue("ErrorPolicy key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }
}
