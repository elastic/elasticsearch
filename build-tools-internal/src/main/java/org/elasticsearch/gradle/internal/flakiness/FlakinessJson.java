/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Jackson (de)serialization of the flakiness contracts. Jackson is already on the {@code build-tools-internal}
 * classpath and natively supports records, so the contract records map 1:1 to their JSON with no custom
 * codecs.
 */
public final class FlakinessJson {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private FlakinessJson() {}

    /** The {@code flakiness-refs.json} envelope (contract 1). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RefsFile(String mergeBase, List<FlakinessRef> refs) {}

    /**
     * The resolved {@link BaseTarget}s plus the {@code unresolved} refs (folded verbatim into the plan by the
     * scan step). Produced in-memory by {@link FlakinessTargets#merge} from the per-project
     * {@link ProjectTargetsFile}s.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BaseTargetsFile(List<BaseTarget> targets, List<FlakinessPlan.Unresolved> unresolved) {}

    /**
     * One project's whole flakiness model, carried as a task {@code @Input} string (see
     * {@link FlakinessProjectResolvePlugin}). Task inputs are the channel that survives the configuration-cache
     * boundary, which is why the model travels this way rather than through shared mutable state.
     *
     * <p>{@code Path} components round-trip through Jackson's built-in {@code java.nio.file.Path} handlers
     * (written as {@code file:} URIs), so the same records the pure resolver already consumes are reused
     * verbatim - no parallel string-only DTOs.
     *
     * <p>The model is captured in full for <b>every</b> project, not just the ones that own a ref. Expanding an
     * abstract base is a repo-wide bytecode question whose answers land in arbitrary projects, and running one
     * of those answers needs its owning source set's {@code Test} tasks - so there is no useful "this project
     * is irrelevant" shortcut to take at configuration time.
     *
     * @param classDirs     this project's compiled-output directories the scan step must read (test source
     *                      sets plus {@code main}; see {@link FlakinessProjectModel#scannedClassDirs})
     * @param bwcTestPlugin whether {@code elasticsearch.bwc-test} is applied; informational only (the
     *                      disposition is derived from the {@code Test} tasks themselves, never from this)
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProjectModel(
        String projectPath,
        Path projectDir,
        List<SourceSetInfo> sourceSets,
        List<TestTaskInfo> testTasks,
        List<Path> classDirs,
        boolean bwcTestPlugin
    ) {}

    /** A resolved target together with the index of the ref that produced it (see {@link ProjectModel}). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RefTarget(int refIndex, BaseTarget target) {}

    /**
     * One project's share of the resolve answer, folded together by {@link FlakinessTargets#merge}.
     *
     * <p>Only {@code resolved} is about the refs. The other two are what every project contributes regardless:
     * <ul>
     *   <li>{@code classDirs} - the bytecode the scan must read, so the class hierarchy spans the whole repo
     *       (see {@link FlakinessTargets#classDirs});</li>
     *   <li>{@code dispositions} - how each of this project's test source sets can be re-run, so the scan can
     *       run a subclass it finds here even though the ref pointed somewhere else entirely (see
     *       {@link SourceSetDisposition} and {@link FlakinessTargets#dispositionsByClassDir}).</li>
     * </ul>
     * Together they are what makes cross-project abstract-base expansion work without any cross-project model
     * access at configuration time: each project reports only its own facts, and the scan joins them.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ProjectTargetsFile(
        String projectPath,
        List<RefTarget> resolved,
        List<Path> classDirs,
        List<SourceSetDisposition> dispositions
    ) {}

    public static RefsFile parseRefs(String json) {
        try {
            return MAPPER.readValue(json, RefsFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse flakiness-refs.json", e);
        }
    }

    public static String writeProjectModel(ProjectModel model) {
        try {
            return MAPPER.writeValueAsString(model);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize the flakiness project model", e);
        }
    }

    public static ProjectModel parseProjectModel(String json) {
        try {
            return MAPPER.readValue(json, ProjectModel.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse the flakiness project model", e);
        }
    }

    public static String writeProjectTargets(ProjectTargetsFile file) {
        try {
            return MAPPER.writeValueAsString(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize project-targets.json", e);
        }
    }

    public static ProjectTargetsFile parseProjectTargets(String json) {
        try {
            return MAPPER.readValue(json, ProjectTargetsFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse project-targets.json", e);
        }
    }

    public static String writePlan(FlakinessPlan plan) {
        try {
            return MAPPER.writeValueAsString(plan);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize flakiness-plan.json", e);
        }
    }
}
