/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Fails the build when Linux {@code .so} files require glibc or libstdc++ newer than the supported
 * minimum (default: RHEL 8 — glibc {@value NativeLibrariesLinuxAbiPlugin#DEFAULT_MAX_GLIBC_VERSION},
 * {@code GLIBCXX_}{@value NativeLibrariesLinuxAbiPlugin#DEFAULT_MAX_GLIBCXX_VERSION}).
 */
@CacheableTask
public abstract class VerifyNativeLibraryLinuxAbiTask extends DefaultTask {

    /**
     * Platform tree or individual {@code .so} files. Only {@code linux-aarch64} and
     * {@code linux-x64} shared libraries are checked; other paths are ignored.
     */
    @SkipWhenEmpty
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getNativeLibraries();

    /** Maximum glibc version, for example {@code 2.28}. */
    @Input
    public abstract Property<String> getMaxGlibcVersion();

    /** Maximum libstdc++ GLIBCXX version, for example {@code 3.4.25}. */
    @Input
    public abstract Property<String> getMaxGlibcxxVersion();

    /** Optional explicit objdump executable; defaults to {@code objdump} then {@code llvm-objdump}. */
    @Optional
    @Input
    public abstract Property<String> getObjdumpExecutable();

    /** Marker written when verification succeeds; enables Gradle task caching. */
    @OutputFile
    public abstract RegularFileProperty getResultMarker();

    /** Gradle exec service used to invoke objdump. */
    @Inject
    protected abstract ExecOperations getExecOperations();

    /** Runs {@code objdump -p} on each Linux {@code .so} and fails on ABI policy violations. */
    @TaskAction
    public void verify() throws IOException {
        List<File> sharedLibraries = getNativeLibraries().getFiles()
            .stream()
            .filter(File::isFile)
            .filter(file -> ObjdumpDynamicVersionParser.isLinuxSharedLibrary(file.getPath()))
            .sorted()
            .toList();
        if (sharedLibraries.isEmpty()) {
            writeMarker();
            return;
        }

        LinuxSymbolVersion maxGlibc = parsePolicyVersion(getMaxGlibcVersion().get(), LinuxSymbolVersion.Kind.GLIBC);
        LinuxSymbolVersion maxGlibcxx = parsePolicyVersion(getMaxGlibcxxVersion().get(), LinuxSymbolVersion.Kind.GLIBCXX);

        Set<String> candidates = ObjdumpDynamicVersionParser.defaultObjdumpCandidates(getObjdumpExecutable().getOrNull());
        String objdump = ObjdumpDynamicVersionParser.resolveObjdumpExecutable(List.copyOf(candidates))
            .orElseThrow(() -> new GradleException(ObjdumpDynamicVersionParser.formatObjdumpMissingMessage(candidates)));

        List<String> violations = new ArrayList<>();
        for (File sharedLibrary : sharedLibraries) {
            String objdumpOutput = runObjdump(objdump, sharedLibrary);
            Map<LinuxSymbolVersion.Kind, LinuxSymbolVersion> referenced = ObjdumpDynamicVersionParser.highestReferencedVersions(
                objdumpOutput
            );
            for (String violation : ObjdumpDynamicVersionParser.findViolations(referenced, maxGlibc, maxGlibcxx)) {
                violations.add(sharedLibrary.getPath() + ": " + violation);
            }
        }

        if (violations.isEmpty() == false) {
            throw new GradleException(
                "Linux native libraries exceed the minimum supported ABI (RHEL 8):\n"
                    + violations.stream().map(v -> "  - " + v).collect(Collectors.joining("\n"))
            );
        }

        writeMarker();
    }

    /** Parses a task policy string such as {@code 2.28} into a {@link LinuxSymbolVersion}. */
    private static LinuxSymbolVersion parsePolicyVersion(String raw, LinuxSymbolVersion.Kind kind) {
        String normalized = LinuxSymbolVersion.normalize(raw);
        if (normalized.startsWith(kind.name() + "_") == false) {
            normalized = kind.name() + "_" + normalized;
        }
        return LinuxSymbolVersion.parseRequired(normalized, kind);
    }

    /**
     * Runs {@code objdump -p}, which prints ELF private headers (including {@code Version References}
     * with {@code GLIBC_*}/{@code GLIBCXX_*} symbols) without disassembly — output is small enough to
     * hold in memory as a string.
     */
    private String runObjdump(String objdump, File sharedLibrary) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ExecResult result = getExecOperations().exec(spec -> {
            spec.setExecutable(objdump);
            spec.args("-p", sharedLibrary.getAbsolutePath());
            spec.setStandardOutput(output);
            spec.setIgnoreExitValue(true);
        });
        if (result.getExitValue() != 0) {
            throw new GradleException(
                "Failed to inspect " + sharedLibrary.getPath() + " with " + objdump + " (exit code " + result.getExitValue() + ")"
            );
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    /** Records successful verification for incremental build caching. */
    private void writeMarker() throws IOException {
        File marker = getResultMarker().getAsFile().get();
        Files.createDirectories(marker.getParentFile().toPath());
        Files.writeString(marker.toPath(), "ok\n", StandardCharsets.UTF_8);
    }
}
