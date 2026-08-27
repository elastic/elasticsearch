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
import static org.hamcrest.Matchers.containsString;

/**
 * The two halves of the settings contract: {@link ExclusionConfig#fromConfig} runs on every query against every
 * already-stored dataset and must never throw, and {@link ExclusionConfig#validate} runs once at registration and
 * must name what is wrong. A value the validator accepts is exactly a value the reader uses as written.
 */
public class ExclusionConfigTests extends ESTestCase {

    // -- fromConfig: the one defaulting site --

    public void testAbsentKeyResolvesToTheDefault() {
        assertEquals(ExclusionConfig.DEFAULT, ExclusionConfig.fromConfig(null));
        assertEquals(ExclusionConfig.DEFAULT, ExclusionConfig.fromConfig(Map.of()));
        assertEquals(ExclusionConfig.DEFAULT, ExclusionConfig.fromConfig(Map.of("partition_detection", "hive")));
    }

    public void testAnExplicitListIsUsedAsStored() {
        ExclusionConfig config = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("**/*.tmp")));
        assertEquals(List.of("**/*.tmp"), config.fileExclusions());
    }

    /** The empty list is a legitimate value — exclude nothing — not a malformed one that falls back. */
    public void testEmptyListIsHonoured() {
        ExclusionConfig config = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_EXCLUSIONS, List.of()));
        assertEquals(List.of(), config.fileExclusions());
        assertTrue(config.compile().keeps("_SUCCESS"));
    }

    /**
     * Every shape of malformed stored value degrades to the default, silently. Reading must not fail on a value
     * stored before these checks existed; registration is where a malformed value is named and refused.
     */
    public void testMalformedStoredValuesDegradeToTheDefaultWithoutThrowing() {
        for (Object malformed : Arrays.asList("**/_*", List.of("**/_*", 42), List.of("a[b"), List.of("{a"), List.of(""))) {
            Map<String, Object> config = new HashMap<>();
            config.put(CONFIG_FILE_EXCLUSIONS, malformed);
            ExclusionConfig resolved = ExclusionConfig.fromConfig(config);
            assertEquals(
                "malformed [" + malformed + "] must fall back",
                ExclusionConfig.DEFAULT_FILE_EXCLUSIONS,
                resolved.fileExclusions()
            );
            resolved.compile();
        }
    }

    public void testNullElementDegradesToTheDefault() {
        Map<String, Object> config = new HashMap<>();
        config.put(CONFIG_FILE_EXCLUSIONS, Arrays.asList("**/_*", null));
        assertEquals(ExclusionConfig.DEFAULT_FILE_EXCLUSIONS, ExclusionConfig.fromConfig(config).fileExclusions());
    }

    // -- validate: strict at registration --

    public void testValidateAcceptsWellFormedSettings() {
        ExclusionConfig.validate(null);
        ExclusionConfig.validate(Map.of());
        ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("**/_*", "**/.*", "**/_temporary/**")));
        ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of()));
    }

    public void testValidateRejectsAnUnparseablePattern() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("a[b")))
        );
        assertThat(e.getMessage(), containsString("must contain only valid patterns"));
        assertThat(e.getMessage(), containsString("unterminated character class"));
    }

    /** Entries are ordinary resource patterns, so a path-shaped one is legal — that is the point of the change. */
    public void testEntriesMayNameDirectories() {
        ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("**/_temporary/**")));
        ExclusionConfig.NameFilter filter = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("**/_temporary/**")))
            .compile();
        assertFalse(filter.keeps("_temporary/task_0/part.parquet"));
        assertFalse(filter.keeps("a/b/_temporary/part.parquet"));
        assertTrue(filter.keeps("a/_temporaryX/part.parquet"));
    }

    /** A shape problem is the list validator's to report, so this returns silently rather than doubling up. */
    public void testValidateLeavesShapeProblemsToTheCaller() {
        Map<String, Object> scalar = new HashMap<>();
        scalar.put(CONFIG_FILE_EXCLUSIONS, "**/_*");
        ExclusionConfig.validate(scalar);

        Map<String, Object> nonString = new HashMap<>();
        nonString.put(CONFIG_FILE_EXCLUSIONS, List.of(42));
        ExclusionConfig.validate(nonString);

        ExclusionConfig.validate(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("")));
    }

    // -- the compiled filter, and the partition-safety property the default rests on --

    /**
     * The default matches only the FINAL segment, because {@code *} cannot cross a separator. Partition values live
     * in directory names and never in the file name, so no partition directory can be touched — under any
     * {@code partition_detection} mode, which is what the previous {@code _*=*} carve-out only managed for hive.
     */
    public void testTheDefaultNeverTouchesADirectory() {
        ExclusionConfig.NameFilter filter = ExclusionConfig.DEFAULT.compile();

        assertFalse("a marker file", filter.keeps("_SUCCESS"));
        assertFalse("a marker nested under partitions", filter.keeps("year=2024/_SUCCESS"));
        assertFalse("a sidecar", filter.keeps("a/b/.part-0.parquet.crc"));

        assertTrue("hive partition directory starting with _", filter.keeps("_dept=alpha/part1.csv"));
        assertTrue("template partition value starting with _", filter.keeps("_foo/part1.csv"));
        assertTrue("bare hive null sentinel", filter.keeps("__HIVE_DEFAULT_PARTITION__/part1.csv"));
        assertTrue("hive null sentinel", filter.keeps("col=__HIVE_DEFAULT_PARTITION__/part1.csv"));
        assertTrue("a dot directory's contents are read by default", filter.keeps(".hidden/data.parquet"));
        assertTrue("ordinary data", filter.keeps("year=2024/month=01/part-0.parquet"));
        assertTrue("a name merely containing an underscore", filter.keeps("foo_bar.parquet"));
    }

    /**
     * The two directory rules in the default. They hold real data files — a failed job's part-files, a Delta
     * transaction log — so a file-name rule cannot reach them, and a wildcard over directory names would also
     * swallow partitions. Naming them is the only shape that covers the junk without endangering data.
     */
    public void testTheDefaultCoversTheTwoNamedJunkDirectories() {
        ExclusionConfig.NameFilter filter = ExclusionConfig.DEFAULT.compile();

        assertFalse("a failed job's leftovers", filter.keeps("_temporary/task_0/part.parquet"));
        assertFalse("nested anywhere in the tree", filter.keeps("year=2024/_temporary/task_0/part.parquet"));
        assertFalse("a delta transaction log", filter.keeps("_delta_log/00001.json"));

        assertTrue("a directory that merely starts the same way", filter.keeps("_temporaryX/part.parquet"));
        assertTrue("and no partition directory is touched", filter.keeps("_dept=alpha/part1.csv"));
        assertTrue("including a template value", filter.keeps("_foo/part1.csv"));
    }

    public void testCustomEntriesReplaceRatherThanAugmentTheDefault() {
        ExclusionConfig.NameFilter filter = ExclusionConfig.fromConfig(Map.of(CONFIG_FILE_EXCLUSIONS, List.of("**/*.tmp"))).compile();
        assertFalse(filter.keeps("staging/a.tmp"));
        assertTrue("the convention is not augmented onto a custom list", filter.keeps("_SUCCESS"));
    }

    public void testNullComponentIsRejected() {
        expectThrows(NullPointerException.class, () -> new ExclusionConfig(null));
    }

    public void testConfigIsImmutableAgainstCallerMutation() {
        List<String> mutable = new ArrayList<>(List.of("**/_*"));
        ExclusionConfig config = new ExclusionConfig(mutable);
        mutable.add("**/*.parquet");
        assertEquals(List.of("**/_*"), config.fileExclusions());
    }
}
