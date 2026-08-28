/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed {@code GLIBC_*} or {@code GLIBCXX_*} dynamic symbol version from an ELF shared library.
 */
public record LinuxSymbolVersion(Kind kind, int major, int minor, int patch) implements Comparable<LinuxSymbolVersion> {

    /** Symbol family used for Linux runtime ABI checks. */
    public enum Kind {
        /** C library ({@code libc.so.6}) version symbols. */
        GLIBC,
        /** GNU libstdc++ version symbols. */
        GLIBCXX
    }

    private static final Pattern SYMBOL_VERSION = Pattern.compile("(GLIBC|GLIBCXX)_(\\d+)\\.(\\d+)(?:\\.(\\d+))?");

    /** Parses an objdump symbol token such as {@code GLIBC_2.28}; empty when the token is unrecognized. */
    public static Optional<LinuxSymbolVersion> parse(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        Matcher matcher = SYMBOL_VERSION.matcher(token.trim());
        if (matcher.matches() == false) {
            return Optional.empty();
        }
        Kind parsedKind = Kind.valueOf(matcher.group(1));
        int parsedMajor = Integer.parseInt(matcher.group(2));
        int parsedMinor = Integer.parseInt(matcher.group(3));
        int parsedPatch = matcher.group(4) == null ? 0 : Integer.parseInt(matcher.group(4));
        return Optional.of(new LinuxSymbolVersion(parsedKind, parsedMajor, parsedMinor, parsedPatch));
    }

    /** Returns {@code true} when this version is strictly greater than {@code maximum}. */
    public boolean exceeds(LinuxSymbolVersion maximum) {
        if (kind != maximum.kind) {
            throw new IllegalArgumentException("cannot compare " + kind + " against " + maximum.kind);
        }
        return compareTo(maximum) > 0;
    }

    /** Compares major, then minor, then patch; both values must share the same {@link Kind}. */
    @Override
    public int compareTo(LinuxSymbolVersion other) {
        if (kind != other.kind) {
            throw new IllegalArgumentException("cannot compare " + kind + " against " + other.kind);
        }
        int majorCompare = Integer.compare(major, other.major);
        if (majorCompare != 0) {
            return majorCompare;
        }
        int minorCompare = Integer.compare(minor, other.minor);
        if (minorCompare != 0) {
            return minorCompare;
        }
        return Integer.compare(patch, other.patch);
    }

    /** Returns the canonical {@code GLIBC_*} / {@code GLIBCXX_*} form, omitting a zero patch. */
    @Override
    public String toString() {
        if (patch == 0) {
            return kind.name() + "_" + major + "." + minor;
        }
        return kind.name() + "_" + major + "." + minor + "." + patch;
    }

    /** Like {@link #parse(String)} but throws when the token is missing or the kind does not match. */
    public static LinuxSymbolVersion parseRequired(String raw, Kind expectedKind) {
        return parse(raw).filter(version -> version.kind() == expectedKind)
            .orElseThrow(() -> new IllegalArgumentException("invalid " + expectedKind + " version: " + raw));
    }

    /** Trims and uppercases a raw policy or symbol token. */
    public static String normalize(String raw) {
        return raw.trim().toUpperCase(Locale.ROOT);
    }
}
