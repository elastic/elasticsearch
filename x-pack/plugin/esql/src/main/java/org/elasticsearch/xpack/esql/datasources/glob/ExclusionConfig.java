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
 * <p>The default covers two shapes. Two file-name rules, {@code **}{@code /_*} and {@code **}{@code /.*}, for the
 * Spark and Hive convention of marking non-data with a leading {@code _} or {@code .} — {@code _SUCCESS},
 * {@code _metadata}, {@code .part-0.crc}. And two directory rules naming {@code _temporary/} and
 * {@code _delta_log/}, which hold real data files that are not the dataset's data: a failed job's part-files, and
 * a Delta transaction log.
 *
 * <p><b>Why that spelling and not {@code _*}.</b> A full-match pattern of {@code **}{@code /_*} matches exactly the
 * paths whose FINAL segment starts with an underscore, because {@code *} cannot cross a separator. It therefore
 * cannot touch a directory, and partition values live in directory names. That is the partition-safety argument:
 * the file-name rules are not permitted anywhere a partition directory could be, under any
 * {@code partition_detection} mode, so there is no exception to carve out and no mode to special-case.
 *
 * <p>One residual, recorded rather than hidden. {@code HivePartitionDetector} walks every segment including the
 * leaf, and binds an extensionless, dot-free {@code key=value} file name as a partition. A file named
 * {@code _dept=alpha} is therefore both a bindable partition shape and a name the default drops. Vanishingly rare
 * — a real data file has an extension, which disqualifies it — but it is the one place a partition value is not a
 * directory name.
 *
 * <p><b>Why those two directories are named rather than matched by a wildcard.</b> A directory rule with a
 * wildcard, {@code **}{@code /_*}{@code /**}, would catch every junk directory and also {@code _dept=alpha/} and
 * any template partition value starting with an underscore — the same trampling the file-name rules exist to
 * avoid. Naming a directory cannot do that: {@code **}{@code /_temporary/**} removes real data only if a partition
 * is literally called {@code _temporary}. Coverage where it is safe, and nowhere else.
 *
 * <p>Exclusion applies to wildcard discovery only. An object named explicitly — a resource with no wildcard, a
 * member of a comma-separated resource, or an enumerable brace pattern — is always read, because naming an object
 * is a request to read it.
 */
public record ExclusionConfig(List<String> fileExclusions) {

    public static final String CONFIG_FILE_EXCLUSIONS = "file_exclusions";

    /** The keys {@link #fromConfig} reads. */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_FILE_EXCLUSIONS);

    public static final List<String> DEFAULT_FILE_EXCLUSIONS = List.of("**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**");

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
        // Never null: asStringList only yields String elements, and a null element makes it return null instead.
        if (glob.isEmpty()) {
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
            return excludedBy(relativePath) == null;
        }

        /**
         * The first entry that drops this object, or {@code null} when it survives. Callers that report an
         * exclusion to the user need the rule responsible, not just the verdict: "excluded" on its own leaves
         * the user to guess which of their patterns did it.
         */
        @Nullable
        public String excludedBy(String relativePath) {
            for (int i = 0; i < exclusions.size(); i++) {
                GlobMatcher exclusion = exclusions.get(i);
                if (exclusion.matches(relativePath)) {
                    return exclusion.glob();
                }
            }
            return null;
        }
    }
}
