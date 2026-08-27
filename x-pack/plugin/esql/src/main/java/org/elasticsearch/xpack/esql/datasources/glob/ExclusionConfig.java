/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Which listed objects a glob expansion drops, as a list of patterns the user can set on a dataset.
 *
 * <p>An entry is an ordinary resource pattern in the same language as {@code resource}, compiled by the same
 * {@link GlobMatcher} and matched against the same string: the object's path relative to the listing prefix. There
 * is no second dialect and no separate matching rule — an exclusion entry is the same kind of object as the
 * pattern that selected the file in the first place.
 *
 * <p>The default is {@code ["**}{@code /_*", "**}{@code /.*"]}: names beginning with {@code _} or {@code .}, which
 * is the Spark and Hive convention for markers, sidecars and job leftovers — {@code _SUCCESS}, {@code _metadata},
 * {@code .part-0.crc}.
 *
 * <p><b>Why that spelling and not {@code _*}.</b> A full-match pattern of {@code **}{@code /_*} matches exactly the
 * paths whose FINAL segment starts with an underscore, because {@code *} cannot cross a separator. It therefore
 * cannot touch a directory, which is what makes it safe: partition values live in directory names and never in the
 * file name. The previous default matched every segment, so it dropped {@code _dept=alpha/} too, and needed a
 * second list carving out {@code _*=*} to rescue it. That carve-out was a proxy for "this is a partition
 * directory" that only held for {@code partition_detection: hive}; under {@code template} the directories are bare
 * values with no {@code =}, nothing was rescued, and a partition named {@code _foo/} was silently dropped along
 * with its rows. Matching only the leaf makes the whole question disappear rather than answering it.
 *
 * <p>The cost, stated plainly: junk <em>directories</em> are no longer excluded by default. A failed Spark job's
 * {@code _temporary/} holds real data files, and they will be read until the dataset names it —
 * {@code "**}{@code /_temporary/**"} — which is one line and, unlike a wildcard over directory names, cannot
 * swallow somebody's partition.
 *
 * <p>Exclusion applies to wildcard discovery only. An object named explicitly — a resource with no wildcard, a
 * member of a comma-separated resource, or an enumerable brace pattern — is always read, because naming an object
 * is a request to read it.
 */
public record ExclusionConfig(List<String> fileExclusions) {

    public static final String CONFIG_FILE_EXCLUSIONS = "file_exclusions";

    /** The keys {@link #fromConfig} reads. */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_FILE_EXCLUSIONS);

    public static final List<String> DEFAULT_FILE_EXCLUSIONS = List.of("**/_*", "**/.*");

    public static final ExclusionConfig DEFAULT = new ExclusionConfig(DEFAULT_FILE_EXCLUSIONS);

    /** Pre-compiled matchers for {@link #DEFAULT}, the case on every expansion that configures nothing. */
    private static final NameFilter DEFAULT_FILTER = DEFAULT.compileUnchecked();

    public ExclusionConfig {
        Objects.requireNonNull(fileExclusions, "fileExclusions cannot be null");
        fileExclusions = List.copyOf(fileExclusions);
    }

    /**
     * Resolves the setting into one config, leniently: this runs on every query against every already-stored
     * dataset, so it never throws on a stored value. A malformed value falls back to the default; malformed values
     * are rejected at registration instead, see {@link #validate}.
     *
     * <p>Because this screens every entry through {@link #validEntry}, {@link #compile} cannot throw on a value
     * this produced, and the listing cache identity always names the config that actually drove the listing.
     */
    public static ExclusionConfig fromConfig(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return DEFAULT;
        }
        Object value = config.get(CONFIG_FILE_EXCLUSIONS);
        if (value == null) {
            return DEFAULT;
        }
        List<String> entries = asStringList(value);
        if (entries == null) {
            // A value stored before these checks existed, or one this node's validation would reject. Reading must
            // not fail on it; the registration path rejects it, and validate() reports it with an actionable message.
            return DEFAULT;
        }
        for (String entry : entries) {
            if (validEntry(entry) == false) {
                return DEFAULT;
            }
        }
        return new ExclusionConfig(entries);
    }

    /** The value as a list of strings, or {@code null} when it is not one. */
    @Nullable
    private static List<String> asStringList(Object value) {
        if (value instanceof List<?> list) {
            List<String> entries = new ArrayList<>(list.size());
            for (Object element : list) {
                if (element instanceof String s) {
                    entries.add(s);
                } else {
                    return null;
                }
            }
            return entries;
        }
        return null;
    }

    /**
     * The one predicate {@link #fromConfig} screens with and {@link #validate} reports on, so a value the
     * registration path accepts is exactly a value the read path uses as written.
     */
    private static boolean validEntry(String glob) {
        if (glob == null || glob.isEmpty()) {
            return false;
        }
        try {
            new GlobMatcher(glob);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Registration-time validation. Deliberately stricter than {@link #fromConfig}, which must keep reading
     * datasets stored before these checks existed: a new registration is the only place a malformed entry can
     * still be caught and named.
     */
    public static void validate(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return;
        }
        Object value = config.get(CONFIG_FILE_EXCLUSIONS);
        if (value == null) {
            return;
        }
        List<String> entries = asStringList(value);
        if (entries == null) {
            // A shape problem is already reported by the caller, which validates this key as a string list and
            // produces an actionable message. Reporting here would append a second error on the same setting.
            return;
        }
        List<String> problems = new ArrayList<>();
        for (String entry : entries) {
            if (entry.isEmpty()) {
                // Emptiness is a shape problem, already reported by the caller's string-list check.
                continue;
            }
            try {
                new GlobMatcher(entry);
            } catch (RuntimeException e) {
                problems.add("[" + CONFIG_FILE_EXCLUSIONS + "] must contain only valid patterns (" + e.getMessage() + ")");
            }
        }
        if (problems.isEmpty() == false) {
            throw new IllegalArgumentException(String.join("; ", problems));
        }
    }

    /** Compiles the entries once, for reuse across every candidate object of one expansion. */
    public NameFilter compile() {
        return this.equals(DEFAULT) ? DEFAULT_FILTER : compileUnchecked();
    }

    private NameFilter compileUnchecked() {
        List<GlobMatcher> matchers = new ArrayList<>(fileExclusions.size());
        for (String glob : fileExclusions) {
            matchers.add(new GlobMatcher(glob));
        }
        return new NameFilter(List.copyOf(matchers));
    }

    /** The compiled form, evaluated once per candidate object during a listing. */
    public static final class NameFilter {

        private final List<GlobMatcher> exclusions;

        private NameFilter(List<GlobMatcher> exclusions) {
            this.exclusions = exclusions;
        }

        /**
         * Whether a listed object survives. The relative path is matched whole, exactly as the {@code resource}
         * pattern that selected it was matched — one language, one semantics, one string.
         */
        public boolean keeps(String relativePath) {
            for (int i = 0; i < exclusions.size(); i++) {
                if (exclusions.get(i).matches(relativePath)) {
                    return false;
                }
            }
            return true;
        }
    }
}
