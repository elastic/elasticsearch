/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.glob.ExclusionConfig.CONFIG_FILE_EXCLUSIONS;
import static org.elasticsearch.xpack.esql.datasources.glob.ExclusionConfig.CONFIG_FILE_INCLUSIONS;

/**
 * The two halves of the settings contract: {@link ExclusionConfig#fromConfig} runs on every query against every
 * already-stored dataset and must never throw, and {@link ExclusionConfig#validate} runs once at registration and
 * must name what is wrong. A value the validator accepts is exactly a value the reader uses as written — these
 * tests are what keeps the two from drifting into a state where a stored dataset behaves unlike its registration.
 */
public class ExclusionConfigTests extends ESTestCase {

    // -- fromConfig: the one defaulting site --

    public void testAbsentKeysResolveToTheDefault() {
        assertEquals(ExclusionConfig.DEFAULT, ExclusionConfig.fromConfig(null));
        assertEquals(ExclusionConfig.DEFAULT, ExclusionConfig.fromConfig(Map.of()));
        assertEquals(ExclusionConfig.DEFAULT, ExclusionConfig.fromConfig(Map.of("partition_detection", "hive")));
    }

    public void testEachKeyDefaultsIndependently() {
        ExclusionConfig onlyExclusions = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("*.tmp")));
        assertEquals(List.of("*.tmp"), onlyExclusions.fileExclusions());
        assertEquals("the untouched key keeps its default", ExclusionConfig.DEFAULT_FILE_INCLUSIONS, onlyExclusions.fileInclusions());

        ExclusionConfig onlyInclusions = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_INCLUSIONS, List.of("keep_*")));
        assertEquals(ExclusionConfig.DEFAULT_FILE_EXCLUSIONS, onlyInclusions.fileExclusions());
        assertEquals(List.of("keep_*"), onlyInclusions.fileInclusions());
    }

    /** The empty list is a legitimate value — "exclude nothing" — not a malformed one that falls back. */
    public void testEmptyListIsHonoured() {
        ExclusionConfig config = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_EXCLUSIONS, List.of()));
        assertEquals(List.of(), config.fileExclusions());
        assertTrue(config.compile().keeps("_SUCCESS"));
    }

    /**
     * Every shape of malformed stored value degrades that key to its default, silently. Reading must not fail on a
     * value stored before these checks existed; registration is where a malformed value is named and refused.
     */
    public void testMalformedStoredValuesDegradeToTheDefaultWithoutThrowing() {
        for (Object malformed : Arrays.asList(
            "_*",                                  // a scalar where a list belongs
            List.of("_*", 42),                     // a non-string element
            List.of("_temporary/**"),              // an entry that is a path, not a segment name
            List.of("**"),                         // recursion, which a segment name cannot express
            List.of("["),                          // an entry that does not compile as a glob
            List.of("")                            // an empty entry
        )) {
            Map<String, Object> config = new HashMap<>();
            config.put(CONFIG_FILE_EXCLUSIONS, malformed);
            ExclusionConfig resolved = ExclusionConfig.fromConfig(config);
            assertEquals(
                "malformed [" + malformed + "] must fall back to the default",
                ExclusionConfig.DEFAULT_FILE_EXCLUSIONS,
                resolved.fileExclusions()
            );
            resolved.compile();
        }
    }

    /** A null value in the list is a non-String element, and must degrade rather than trip a null check. */
    public void testNullElementDegradesToTheDefault() {
        Map<String, Object> config = new HashMap<>();
        config.put(CONFIG_FILE_EXCLUSIONS, Arrays.asList("_*", null));
        assertEquals(ExclusionConfig.DEFAULT_FILE_EXCLUSIONS, ExclusionConfig.fromConfig(config).fileExclusions());
    }

    // -- validate: strict at registration --

    public void testValidateAcceptsWellFormedSettings() {
        ExclusionConfig.validate(null);
        ExclusionConfig.validate(Map.of());
        ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("_*", ".*", "*.tmp"), CONFIG_FILE_INCLUSIONS, List.of("_*=*")));
        ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of()));
    }

    public void testValidateRejectsPathEntriesByNamingTheRule() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("_temporary/**")))
        );
        assertEquals(
            "[file_exclusions] must contain only single path-segment name globs — entries cannot be empty or contain "
                + "'/' or '**', got [_temporary/**]",
            e.getMessage()
        );
    }

    public void testValidateRejectsRecursionAndEmptyEntries() {
        expectThrows(IllegalArgumentException.class, () -> ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("**"))));
        expectThrows(IllegalArgumentException.class, () -> ExclusionConfig.validate(Map.of(CONFIG_FILE_INCLUSIONS, List.of(""))));
    }

    public void testValidateRejectsAnUncompilableGlob() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExclusionConfig.validate(Map.of(CONFIG_FILE_INCLUSIONS, List.of("[")))
        );
        assertTrue(e.getMessage(), e.getMessage().startsWith("[file_inclusions] must contain only valid glob patterns ("));
        assertTrue(e.getMessage(), e.getMessage().endsWith(", got [[]"));
    }

    /**
     * A shape problem is the list-validator's to report, so this returns silently rather than adding a second error
     * about the same setting.
     */
    public void testValidateLeavesShapeProblemsToTheCaller() {
        Map<String, Object> scalar = new HashMap<>();
        scalar.put(CONFIG_FILE_EXCLUSIONS, "_*");
        ExclusionConfig.validate(scalar);

        Map<String, Object> nonString = new HashMap<>();
        nonString.put(CONFIG_FILE_EXCLUSIONS, List.of(42));
        ExclusionConfig.validate(nonString);
    }

    // -- the compiled evaluator --

    public void testInclusionsWinOnTheSegmentTheyMatch() {
        ExclusionConfig.Matchers matchers = ExclusionConfig.DEFAULT.compile();
        assertTrue(matchers.keeps("_dept=alpha/part1.csv"));
        assertFalse(matchers.keeps("_dept=alpha/_SUCCESS"));
        assertFalse(matchers.keeps("_hidden/part1.csv"));
    }

    /** An inclusion rescues only the segment it matches — another segment's exclusion still drops the object. */
    public void testInclusionDoesNotRescueOtherSegments() {
        ExclusionConfig config = new ExclusionConfig(List.of("_*"), List.of("_keep"));
        ExclusionConfig.Matchers matchers = config.compile();
        assertTrue(matchers.keeps("_keep/data.csv"));
        assertFalse(matchers.keeps("_keep/_other/data.csv"));
    }

    public void testCustomExclusionsReplaceRatherThanAugmentTheDefault() {
        ExclusionConfig.Matchers matchers = ExclusionConfig.fromConfig(
            Map.of(CONFIG_FILE_EXCLUSIONS, List.of("*.tmp"), CONFIG_FILE_INCLUSIONS, List.of())
        ).compile();
        assertFalse(matchers.keeps("staging/a.tmp"));
        assertTrue("the convention is not augmented onto a custom list", matchers.keeps("_SUCCESS"));
    }

    public void testConfigIsImmutableAgainstCallerMutation() {
        List<String> mutable = new ArrayList<>(List.of("_*"));
        ExclusionConfig config = new ExclusionConfig(mutable, List.of());
        mutable.add("*.parquet");
        assertEquals(List.of("_*"), config.fileExclusions());
    }
}
