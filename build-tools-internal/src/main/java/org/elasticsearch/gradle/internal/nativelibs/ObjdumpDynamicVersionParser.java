/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BinaryOperator;

/**
 * Parses {@code objdump -p} output and selects an {@code objdump}/{@code llvm-objdump} executable.
 */
final class ObjdumpDynamicVersionParser {

    private ObjdumpDynamicVersionParser() {}

    /**
     * Scans {@code objdump -p} output for the highest referenced {@code GLIBC_*} and {@code GLIBCXX_*}
     * versions.
     *
     * <p>Whitespace-separated tokens are parsed; for example {@code GLIBCXX_3.4} in
     * {@code 0x08922974 0x00 03 GLIBCXX_3.4}. Unrecognized tokens are ignored.
     */
    static Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> highestReferencedVersions(String objdumpOutput) {
        Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> highest = new EnumMap<>(LinuxSymbolVersion.Kind.class);
        BinaryOperator<LinuxSymbolVersion> keepHighest = BinaryOperator.maxBy(LinuxSymbolVersion::compareTo);
        for (String token : objdumpOutput.split("\\s+")) {
            LinuxSymbolVersion.parse(token).ifPresent(version -> highest.merge(version.kind(), version, keepHighest));
        }
        return highest;
    }

    /** Describes each referenced version that exceeds the supplied glibc / libstdc++ policy maxima. */
    static List<String> findViolations(
        Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> referenced,
        LinuxSymbolVersion maxGlibc,
        LinuxSymbolVersion maxGlibcxx
    ) {
        List<String> violations = new ArrayList<>();
        LinuxSymbolVersion glibc = referenced.get(LinuxSymbolVersion.Kind.GLIBC);
        if (glibc != null && glibc.exceeds(maxGlibc)) {
            violations.add(
                "requires "
                    + glibc
                    + " but maximum supported is "
                    + maxGlibc
                    + " (RHEL 8 ships glibc "
                    + maxGlibc.major()
                    + "."
                    + maxGlibc.minor()
                    + ")"
            );
        }
        LinuxSymbolVersion glibcxx = referenced.get(LinuxSymbolVersion.Kind.GLIBCXX);
        if (glibcxx != null && glibcxx.exceeds(maxGlibcxx)) {
            violations.add(
                "requires " + glibcxx + " but maximum supported is " + maxGlibcxx + " (RHEL 8 ships libstdc++ up to " + maxGlibcxx + ")"
            );
        }
        return violations;
    }

    /** Returns the first candidate that successfully runs {@code --version}. */
    static Optional<String> resolveObjdumpExecutable(List<String> candidates) {
        for (String candidate : candidates) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }
            ProcessBuilder processBuilder = new ProcessBuilder(candidate, "--version");
            processBuilder.redirectErrorStream(true);
            try {
                Process process = processBuilder.start();
                int exitCode = process.waitFor();
                if (exitCode == 0) {
                    return Optional.of(candidate);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            } catch (Exception e) {
                // try next candidate
            }
        }
        return Optional.empty();
    }

    /** Builds the objdump search order: configured path, then {@code objdump}, then {@code llvm-objdump}. */
    static Set<String> defaultObjdumpCandidates(String configuredExecutable) {
        Set<String> candidates = new TreeSet<>();
        if (configuredExecutable != null && configuredExecutable.isBlank() == false) {
            candidates.add(configuredExecutable);
        }
        candidates.add("objdump");
        candidates.add("llvm-objdump");
        return candidates;
    }

    /** User-facing message when no objdump executable could be resolved. */
    static String formatObjdumpMissingMessage(Set<String> candidates) {
        return "Unable to locate objdump (tried "
            + String.join(", ", candidates)
            + "). Install binutils or LLVM objdump to verify Linux native library ABI.";
    }

    /** Returns {@code true} for {@code linux-aarch64} and {@code linux-x64} {@code .so} paths. */
    static boolean isLinuxSharedLibrary(String path) {
        if (path == null || path.endsWith(".so") == false) {
            return false;
        }
        String normalized = path.replace('\\', '/').toLowerCase(Locale.ROOT);
        return normalized.contains("/linux-aarch64/") || normalized.contains("/linux-x64/");
    }
}
