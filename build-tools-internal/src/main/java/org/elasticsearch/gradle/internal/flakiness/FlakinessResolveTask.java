/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * The resolve half of the {@code resolve - compile - scan} Gradle flow. Reads {@code flakiness-refs.json},
 * reads the assembled project model from the {@link FlakinessModelService} <em>at execution time</em> (by
 * which point every configured project has contributed its own {@link ProjectInfo}), resolves each ref to a
 * {@link BaseTarget} via the pure {@link RefResolver}, and writes two hand-off files:
 * <ul>
 *   <li>{@code flakiness-base-targets.json} - the rich targets (+ unresolved refs) the scan step consumes;</li>
 *   <li>{@code flakiness-compile-tasks.txt} - the distinct compile task paths of the non-bwc targets, which
 *       the compile step invokes plainly (its exit code is the sole {@code build_failed} signal).</li>
 * </ul>
 *
 * <p>Reading the service in the task action (not at configuration time) is the crux of the lifecycle-correct
 * design: populate-at-config in each project, read-at-execution here. To turn the prototype's silent-empty
 * failure mode (JAVA_RESOLVER_NOTES.md P1a) into a loud one, the action fails if the model is empty while
 * there are refs to resolve.
 */
public abstract class FlakinessResolveTask extends DefaultTask {

    /**
     * The whole {@code flakiness-refs.json} text, supplied lazily (via a file-contents provider) as an input.
     * {@code @Optional} because when the file is absent the provider is empty; rather than let Gradle fail
     * with the opaque "property 'refsJson' doesn't have a configured value", the action turns that into a
     * clear, actionable error (see {@link #resolve()}).
     */
    @Input
    @Optional
    public abstract Property<String> getRefsJson();

    /** The refs file path, purely for a clear error message when the file is missing. */
    @Internal
    public abstract Property<String> getRefsPath();

    /**
     * Repo root, used to resolve repo-relative changed-file paths and for the class-ref filesystem probe.
     * {@code @Internal} on purpose: the resolver touches arbitrary source files under it, so fingerprinting
     * it as an input would hash the whole repo (see JAVA_RESOLVER_NOTES.md P3).
     */
    @Internal
    public abstract DirectoryProperty getRepoRoot();

    /** The assembled project model, populated by each project at configuration time (see class javadoc). */
    @ServiceReference(FlakinessModelService.NAME)
    public abstract Property<FlakinessModelService> getModelService();

    @OutputFile
    public abstract RegularFileProperty getBaseTargetsFile();

    @OutputFile
    public abstract RegularFileProperty getCompileTasksFile();

    @TaskAction
    public void resolve() throws IOException {
        if (getRefsJson().isPresent() == false) {
            String path = getRefsPath().getOrElse("flakiness-refs.json");
            throw new GradleException(
                "flakiness-refs.json not found at "
                    + path
                    + "; the resolve step expects the gather/bootstrap step to have written it. For a "
                    + "standalone run, pass -Pflakiness.refs=<path> pointing at a refs file."
            );
        }
        FlakinessJson.RefsFile refsFile = FlakinessJson.parseRefs(getRefsJson().get());
        List<ProjectInfo> projects = getModelService().get().projects();

        if (projects.isEmpty() && refsFile.refs().isEmpty() == false) {
            // The whole point of the rework: prove populate->read worked. An empty model with refs to
            // resolve means the per-project configuration-time population did not run (the exact
            // silent-empty trap the prototype fell into), so fail loudly instead of writing 0 targets.
            throw new GradleException(
                "FlakinessModelService is empty but there are "
                    + refsFile.refs().size()
                    + " refs to resolve. Every test project should have contributed its model during "
                    + "configuration under -Pflakiness.resolve; an empty model indicates the service was not "
                    + "populated (see JAVA_RESOLVER_NOTES.md)."
            );
        }

        Path repoRoot = getRepoRoot().get().getAsFile().toPath();
        RefResolver.Resolution resolution = new RefResolver(repoRoot, projects).resolve(refsFile.refs());

        FlakinessJson.BaseTargetsFile out = new FlakinessJson.BaseTargetsFile(resolution.targets(), resolution.unresolved());
        File baseTargets = getBaseTargetsFile().get().getAsFile();
        baseTargets.getParentFile().mkdirs();
        Files.writeString(baseTargets.toPath(), FlakinessJson.writeBaseTargetsFile(out));

        List<String> compileTasks = compileTaskPaths(resolution.targets());
        File compileTasksFile = getCompileTasksFile().get().getAsFile();
        compileTasksFile.getParentFile().mkdirs();
        Files.writeString(compileTasksFile.toPath(), String.join("\n", compileTasks));

        getLogger().lifecycle(
            "flakiness resolve: {} refs -> {} base targets, {} unresolved (across {} projects), {} compile tasks",
            refsFile.refs().size(),
            resolution.targets().size(),
            resolution.unresolved().size(),
            projects.size(),
            compileTasks.size()
        );
        if (compileTasks.isEmpty() == false) {
            getLogger().info("compile tasks: {}", compileTasks);
        }
    }

    /**
     * The distinct compile task paths of the runnable (non-bwc) targets, deterministically ordered. A bwc
     * target is skipped downstream, so there is nothing to compile for it. Extracted as a pure static method
     * so it is unit-testable without Gradle.
     */
    static List<String> compileTaskPaths(List<BaseTarget> targets) {
        TreeSet<String> compileTasks = new TreeSet<>();
        for (BaseTarget t : targets) {
            if (t.bwc() == false && t.compileTaskPath() != null) {
                compileTasks.add(t.compileTaskPath());
            }
        }
        return new ArrayList<>(compileTasks);
    }
}
