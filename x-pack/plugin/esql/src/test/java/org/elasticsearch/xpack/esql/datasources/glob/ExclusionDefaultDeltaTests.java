/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Exactly how the default exclusion differs from the fixed predicate it replaced, path by path.
 *
 * <p>This file used to assert the two were identical. That was the right bar while the default was a
 * re-expression of the old convention, and it is the wrong bar now: the old convention was wrong. It matched every
 * path segment, so it dropped partition directories beginning with {@code _}, and rescued them with a second list
 * carving out {@code _*=*} — a proxy for "this is a hive partition" that held only for {@code partition_detection:
 * hive}. Under {@code template} the directories are bare values with no {@code =}, nothing was rescued, and a
 * partition named {@code _foo/} disappeared along with its rows.
 *
 * <p>The default's file-name rules match only the final path segment, so they cannot touch a directory at all —
 * which is what makes every partition directory safe under every detection mode. Junk directories that hold real
 * data files are covered separately, by naming them: {@code _temporary} and {@code _delta_log}. Naming is the only
 * shape that reaches a directory without also reaching a partition, and the difference between the two is why the
 * default has both kinds of rule.
 */
public class ExclusionDefaultDeltaTests extends ESTestCase {

    private static final ExclusionConfig.NameFilter DEFAULT_FILTER = ExclusionConfig.DEFAULT.compile();

    /** Frozen pre-rewrite predicate. DO NOT EDIT — it is the "before" this delta is measured against. */
    private static boolean oldConventionExcluded(String relativePath) {
        if (relativePath.endsWith("/")) {
            return true;
        }
        int start = 0;
        for (int i = 0; i <= relativePath.length(); i++) {
            if (i == relativePath.length() || relativePath.charAt(i) == '/') {
                if (i > start) {
                    char first = relativePath.charAt(start);
                    if (first == '.') {
                        return true;
                    }
                    if (first == '_') {
                        int eqIdx = relativePath.indexOf('=', start);
                        boolean isPartitionSegment = eqIdx >= start && eqIdx < i;
                        if (isPartitionSegment == false) {
                            return true;
                        }
                    }
                }
                start = i + 1;
            }
        }
        return false;
    }

    private static boolean nowExcluded(String relativePath) {
        // The placeholder skip is listing normalization rather than exclusion policy, and is composed here exactly
        // as doExpandGlob composes it.
        return relativePath.isEmpty() || relativePath.endsWith("/") || DEFAULT_FILTER.keeps(relativePath) == false;
    }

    private enum Delta {
        /** Both agree. */
        SAME,
        /** The old convention dropped it; the default now reads it. */
        KEPT_NOW,
        /** The old convention read it; the default now drops it. The only such family — see the case below. */
        DROPPED_NOW
    }

    private record Case(String path, Delta delta, String why) {}

    private static final List<Case> CASES = List.of(
        // -- unchanged: markers and sidecars are file names, which is what the default still matches
        new Case("_SUCCESS", Delta.SAME, "a marker file"),
        new Case("year=2024/_SUCCESS", Delta.SAME, "a marker nested under partitions"),
        new Case("_metadata", Delta.SAME, "a metadata file"),
        new Case(".part-r-00001.parquet.crc", Delta.SAME, "a sidecar"),
        new Case("file.parquet", Delta.SAME, "ordinary data"),
        new Case("year=2024/data.parquet", Delta.SAME, "ordinary partitioned data"),
        new Case("a/b/c/d.parquet", Delta.SAME, "ordinary nested data"),
        new Case("subdir/", Delta.SAME, "a placeholder key, still normalized away"),
        new Case(
            "",
            Delta.DROPPED_NOW,
            "the placeholder for the listing prefix itself. The old convention read it — the empty string does not "
                + "end in a slash and its segment walk finds nothing — so the console's folder marker reached the "
                + "reader and failed the query on an object nobody referenced. Dropped deliberately"
        ),
        new Case(
            "_dept=alpha/part1.csv",
            Delta.SAME,
            "a hive partition directory: kept before by the carve-out, kept now by never being looked at"
        ),

        // -- THE BUG FIX: partition directories the carve-out could not rescue
        new Case("_foo/part.csv", Delta.KEPT_NOW, "a template partition value starting with _, silently dropped before"),
        new Case("__HIVE_DEFAULT_PARTITION__/part.csv", Delta.KEPT_NOW, "hive's bare null sentinel as a template value"),
        new Case(".2024/part.csv", Delta.KEPT_NOW, "a partition value starting with a dot"),

        // -- junk directories the default still covers, by naming them rather than wildcarding directory names
        new Case("_temporary/task_0/part.parquet", Delta.SAME, "a failed job's leftovers, named explicitly in the default"),
        new Case("_delta_log/00001.json", Delta.SAME, "delta log entries, named explicitly in the default"),
        new Case(".hidden/data.parquet", Delta.KEPT_NOW, "a dot directory's contents, now read until named"),
        new Case("year=2024/.git/config", Delta.KEPT_NOW, "a nested dot directory's contents"),

        // -- the one family that moves the other way, found by the randomized delta rather than by reasoning
        new Case(
            "_dept=alpha",
            Delta.DROPPED_NOW,
            "a FILE whose name starts with _ and holds an =. The old carve-out was written for directories but "
                + "applied to every segment, so it rescued this in leaf position too; the default now reads it as a "
                + "marker. Hive's detector does bind an extensionless dot-free k=v leaf as a partition, so this is a "
                + "real if unlikely shape rather than a purely theoretical one"
        )
    );

    public void testEveryDifferenceIsAccountedFor() {
        List<String> surprises = new ArrayList<>();
        for (Case c : CASES) {
            boolean before = oldConventionExcluded(c.path());
            boolean after = nowExcluded(c.path());
            Delta actual = before == after ? Delta.SAME : (before ? Delta.KEPT_NOW : Delta.DROPPED_NOW);
            if (actual != c.delta()) {
                surprises.add(
                    "[" + c.path() + "] recorded " + c.delta() + " but was before=" + before + " after=" + after + " — " + c.why()
                );
            }
        }
        assertTrue("the delta drifted from what is recorded:\n  " + String.join("\n  ", surprises), surprises.isEmpty());
    }

    /**
     * The change is overwhelmingly in the keeping direction, and every exception belongs to one family: a FILE
     * whose name starts with {@code _} and contains an {@code =}. The old carve-out was written to protect hive
     * partition DIRECTORIES but was applied to every segment, so it also rescued leaves of that shape. Anything
     * newly dropped outside that family would be a dataset losing a file it used to read, which this change must
     * not do — so the fuzz asserts the shape rather than a blanket "nothing".
     */
    public void testTheOnlyNewlyDroppedFamilyIsALeafShapedLikeAPartition() {
        for (int i = 0; i < 3000; i++) {
            String path = randomRelativePath();
            if (oldConventionExcluded(path) == false && nowExcluded(path)) {
                String leaf = path.substring(path.lastIndexOf('/') + 1);
                assertTrue("newly dropped outside the known family: [" + path + "]", leaf.startsWith("_") && leaf.indexOf('=') >= 0);
            }
        }
    }

    /** The property the whole design rests on: the default cannot touch a directory, so no partition is at risk. */
    public void testTheDefaultNeverMatchesOnADirectorySegment() {
        for (int i = 0; i < 3000; i++) {
            String directory = randomSegment();
            String path = directory + "/data.parquet";
            assertTrue("a directory named [" + directory + "] must not drop its data file", nowExcluded(path) == false);
        }
    }

    private String randomRelativePath() {
        int segments = randomIntBetween(1, 4);
        List<String> parts = new ArrayList<>(segments);
        for (int i = 0; i < segments; i++) {
            parts.add(randomSegment());
        }
        String path = String.join("/", parts);
        return randomInt(9) == 0 ? path + "/" : path;
    }

    private String randomSegment() {
        int length = randomIntBetween(1, 6);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(randomFrom('_', '.', '=', '_', '.', '=', 'a', 'b', 'z', '0', '9', '-'));
        }
        return sb.toString();
    }
}
