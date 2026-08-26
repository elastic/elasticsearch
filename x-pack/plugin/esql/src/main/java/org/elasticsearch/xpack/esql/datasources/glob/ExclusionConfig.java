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
 * Which listed objects a glob expansion keeps, as two lists of globs the user can set on a dataset.
 *
 * <p>A listed object is dropped when any segment of its relative path matches an entry in
 * {@code file_exclusions}, unless that same segment also matches an entry in {@code file_inclusions} —
 * inclusions win. Entries match a <em>single path-segment name</em>, never a path, which is what lets one
 * pattern ({@code _*}) catch a hidden file and a hidden directory at any depth; entries may therefore not
 * contain {@code /} or {@code **}, and both are rejected at registration.
 *
 * <p>The defaults are an exact re-expression of the fixed hidden-object convention this replaced: exclude
 * names beginning with {@code _} or {@code .}, but keep a {@code _}-prefixed segment carrying a {@code =},
 * so Hive partition directories such as {@code _dept=alpha/} survive while {@code _SUCCESS},
 * {@code .part-r-00001.crc}, {@code _delta_log/…} and {@code _temporary/…} do not. Expressing the carve-out
 * needs the second list: a glob cannot say "unless".
 *
 * <p>Exclusion applies to wildcard discovery only. An explicitly named object — brace expansion, a
 * non-pattern segment of a comma list, a pattern the filter hints resolved to one concrete path, or a
 * single named file — is always attempted, because naming an object is a request to read it.
 */
public record ExclusionConfig(List<String> fileExclusions, List<String> fileInclusions) {

    public static final String CONFIG_FILE_EXCLUSIONS = "file_exclusions";
    public static final String CONFIG_FILE_INCLUSIONS = "file_inclusions";

    /** The keys {@link #fromConfig} reads. */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_FILE_EXCLUSIONS, CONFIG_FILE_INCLUSIONS);

    public static final List<String> DEFAULT_FILE_EXCLUSIONS = List.of("_*", ".*");
    public static final List<String> DEFAULT_FILE_INCLUSIONS = List.of("_*=*");

    public static final ExclusionConfig DEFAULT = new ExclusionConfig(DEFAULT_FILE_EXCLUSIONS, DEFAULT_FILE_INCLUSIONS);

    /** Pre-compiled matchers for {@link #DEFAULT}, the case on every expansion that configures nothing. */
    private static final Matchers DEFAULT_MATCHERS = DEFAULT.compileUnchecked();

    public ExclusionConfig {
        Objects.requireNonNull(fileExclusions, "fileExclusions cannot be null");
        Objects.requireNonNull(fileInclusions, "fileInclusions cannot be null");
        fileExclusions = List.copyOf(fileExclusions);
        fileInclusions = List.copyOf(fileInclusions);
    }

    /**
     * Resolves the settings into one config, leniently: this runs on every query against every already-stored
     * dataset, so it never throws on a stored value. Each key resolves independently — absent, or malformed in
     * any way, falls back to that key's default; a well-formed list is used as stored, the empty list included,
     * which is the legitimate "exclude nothing" value. Malformed values are rejected at registration instead,
     * see {@link #validate}.
     *
     * <p>Because this screens every entry through {@link #validEntry}, {@link #compile} cannot throw on a value
     * this produced, and the listing cache identity therefore always names the config that actually drove the
     * listing rather than one the expansion silently declined to use.
     */
    public static ExclusionConfig fromConfig(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return DEFAULT;
        }
        List<String> exclusions = resolveKey(config, CONFIG_FILE_EXCLUSIONS, DEFAULT_FILE_EXCLUSIONS);
        List<String> inclusions = resolveKey(config, CONFIG_FILE_INCLUSIONS, DEFAULT_FILE_INCLUSIONS);
        if (exclusions == DEFAULT_FILE_EXCLUSIONS && inclusions == DEFAULT_FILE_INCLUSIONS) {
            return DEFAULT;
        }
        return new ExclusionConfig(exclusions, inclusions);
    }

    private static List<String> resolveKey(Map<String, Object> config, String key, List<String> fallback) {
        Object value = config.get(key);
        if (value == null) {
            return fallback;
        }
        List<String> entries = asStringList(value);
        if (entries == null) {
            // A value stored before these checks existed, or one this node's validation would reject. Reading must
            // not fail on it; the registration path rejects it, and validate() reports it with an actionable message.
            return fallback;
        }
        for (String entry : entries) {
            if (validEntry(entry) == false) {
                return fallback;
            }
        }
        return entries;
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
        if (glob.isEmpty() || glob.indexOf('/') >= 0 || glob.contains("**")) {
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
        validateKey(config, CONFIG_FILE_EXCLUSIONS);
        validateKey(config, CONFIG_FILE_INCLUSIONS);
    }

    private static void validateKey(Map<String, Object> config, String field) {
        Object value = config.get(field);
        if (value == null) {
            return;
        }
        List<String> entries = asStringList(value);
        if (entries == null) {
            // A shape problem is already reported by the caller, which validates this key as a string list and
            // produces an actionable message. Reporting here would append a second error on the same setting.
            return;
        }
        for (String entry : entries) {
            if (entry.isEmpty()) {
                // Emptiness is a shape problem, already reported by the caller's string-list check. Reporting it
                // here too would put two errors on one setting for one mistake.
                continue;
            }
            if (entry.indexOf('/') >= 0 || entry.contains("**")) {
                throw new IllegalArgumentException(
                    "["
                        + field
                        + "] must contain only single path-segment name globs — entries cannot contain '/' or '**', got ["
                        + entry
                        + "]"
                );
            }
            try {
                new GlobMatcher(entry);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                    "[" + field + "] must contain only valid glob patterns (" + e.getMessage() + "), got [" + entry + "]"
                );
            }
        }
    }

    /**
     * Compiles both lists once, for reuse across every candidate object of one expansion. Never throws on a
     * config {@link #fromConfig} produced.
     */
    public Matchers compile() {
        return this.equals(DEFAULT) ? DEFAULT_MATCHERS : compileUnchecked();
    }

    private Matchers compileUnchecked() {
        return new Matchers(compileAll(fileExclusions), compileAll(fileInclusions));
    }

    private static List<GlobMatcher> compileAll(List<String> globs) {
        List<GlobMatcher> matchers = new ArrayList<>(globs.size());
        for (String glob : globs) {
            matchers.add(new GlobMatcher(glob));
        }
        return List.copyOf(matchers);
    }

    /** The compiled form, evaluated once per candidate object during a listing. */
    public static final class Matchers {

        private final List<GlobMatcher> exclusions;
        private final List<GlobMatcher> inclusions;

        private Matchers(List<GlobMatcher> exclusions, List<GlobMatcher> inclusions) {
            this.exclusions = exclusions;
            this.inclusions = inclusions;
        }

        /**
         * Whether a listed object survives. Walks the relative path segment by segment — the same walk the fixed
         * predicate this replaced performed — so a hidden directory drops its whole subtree without the caller
         * needing a recursive pattern. Empty segments (a leading, trailing or doubled {@code /}) carry no name and
         * are skipped.
         */
        public boolean keeps(String relativePath) {
            if (exclusions.isEmpty()) {
                return true;
            }
            int start = 0;
            for (int i = 0; i <= relativePath.length(); i++) {
                if (i == relativePath.length() || relativePath.charAt(i) == '/') {
                    if (i > start) {
                        String segment = relativePath.substring(start, i);
                        if (matchesAny(exclusions, segment) && matchesAny(inclusions, segment) == false) {
                            return false;
                        }
                    }
                    start = i + 1;
                }
            }
            return true;
        }

        private static boolean matchesAny(List<GlobMatcher> matchers, String segment) {
            for (int i = 0; i < matchers.size(); i++) {
                if (matchers.get(i).matches(segment)) {
                    return true;
                }
            }
            return false;
        }
    }
}
