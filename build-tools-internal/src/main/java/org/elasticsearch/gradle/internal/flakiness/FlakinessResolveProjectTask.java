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
import java.util.stream.Collectors;

/**
 * The <b>per-project</b> resolve task: it resolves {@code flakiness-refs.json} against <em>only its own
 * project's</em> model and writes that project's share of the answer. It is registered in every project with
 * test sources and invoked unqualified, so each project self-selects (see {@link FlakinessProjectResolvePlugin}).
 *
 * <h2>How the model reaches the action</h2>
 * The project's whole model arrives as a single {@code @Input} string ({@link #getProjectModelJson()}), which
 * the registration side supplies as a {@code Provider}. Gradle computes the execution-time value of a
 * task-input provider when it <em>stores</em> the configuration cache entry - i.e. after the entire
 * configuration phase, once every plugin and build script has run - so the captured values are the final,
 * post-mutation ones, while nothing but a {@code String} is serialized into the entry. The task action itself
 * touches no Gradle model at all.
 *
 * <p>A project that owns none of the refs carries an empty model ({@code ownsRefs == false}) and writes an
 * empty result; that is the cheap path the unqualified, run-everywhere invocation depends on.
 *
 * <h2>Outputs</h2>
 * Two files under the shared {@link FlakinessProjectResolvePlugin#TARGETS_DIR}, named after this project:
 * <ul>
 *   <li>{@code <project>.json} - a {@link FlakinessJson.ProjectTargetsFile}, carrying each resolved target
 *       together with the <em>index</em> of the ref that produced it. The index is what lets
 *       {@code flakinessScan} restore the original ref ordering and decide which refs no project could
 *       resolve - a decision no single project can make on its own;</li>
 *   <li>{@code <project>.compile-tasks.txt} - the compile task paths of this project's runnable targets, one
 *       per line. The orchestration step concatenates these to build the compile invocation that runs
 *       <em>between</em> resolve and scan; keeping it a plain text file means that glue stays a {@code cat}
 *       rather than JSON parsing in shell.</li>
 * </ul>
 *
 * <p>Note the resolver is deliberately run one ref at a time: {@link RefResolver} reports a class ref it
 * cannot find as {@code unresolved}, but "not in <em>this</em> project" is not "not anywhere", so per-project
 * unresolved verdicts must be discarded and recomputed globally (by {@link FlakinessScanTask}).
 */
public abstract class FlakinessResolveProjectTask extends DefaultTask {

    /**
     * This project's whole flakiness model, serialized (see {@link FlakinessJson.ProjectModel}). Supplied as
     * a provider so Gradle evaluates it at configuration-cache store time - the post-mutation read this
     * feature depends on - and so the serialized entry holds a plain {@code String} rather than any live
     * Gradle object. Being an {@code @Input} also means a model change correctly invalidates the task.
     */
    @Input
    public abstract Property<String> getProjectModelJson();

    /**
     * The whole {@code flakiness-refs.json} text, supplied lazily via a file-contents provider.
     * {@code @Optional} so a missing file produces a clear error from the action rather than Gradle's opaque
     * "doesn't have a configured value".
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
     * it as an input would hash the whole repo (JAVA_RESOLVER_NOTES.md P3).
     */
    @Internal
    public abstract DirectoryProperty getRepoRoot();

    /** Max {@code Test} tasks a single target may fan out to ({@code -Pflakiness.taskCap}). */
    @Input
    public abstract Property<Integer> getTaskCap();

    @OutputFile
    public abstract RegularFileProperty getTargetsFile();

    /** The compile task paths of this project's runnable targets, one per line (empty when it owns none). */
    @OutputFile
    public abstract RegularFileProperty getCompileTasksFile();

    /**
     * The captured model, written out verbatim. Not consumed by anything downstream - it exists so the
     * post-mutation correctness of the store-time capture is inspectable (e.g. that a bwc project's bare
     * {@code javaRestTest} really is {@code enabled: false} and the {@code v<version>#bwcTest} family really
     * is present with the javaRestTest {@code testClassesDirs}).
     */
    @OutputFile
    public abstract RegularFileProperty getModelFile();

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
        FlakinessJson.ProjectModel model = FlakinessJson.parseProjectModel(getProjectModelJson().get());

        write(getModelFile().get().getAsFile(), getProjectModelJson().get());

        List<FlakinessJson.RefTarget> resolved = new ArrayList<>();
        if (model.ownsRefs()) {
            ProjectInfo project = new ProjectInfo(model.projectPath(), model.projectDir(), model.sourceSets());
            Path repoRoot = getRepoRoot().get().getAsFile().toPath();
            // Single-project resolver: no cross-project model, no build service, nothing but the plain records
            // that were carried in through the @Input above.
            RefResolver resolver = new RefResolver(repoRoot, List.of(project), path -> model.testTasks(), getTaskCap().get());
            List<FlakinessRef> refs = refsFile.refs();
            for (int i = 0; i < refs.size(); i++) {
                // One ref at a time so each target can be attributed to its ref; the per-ref "unresolved"
                // verdict is intentionally ignored here (see class javadoc) and recomputed globally by scan.
                // TODO jozala - how does it filter the tests are in the right task (is it mapped with the directories?) - check the CSV
                // tests (do they break the assumption?)
                // TODO jozala - maybe I can check the Gradle test task if it can resolve the test classes that are used by that test???
                for (BaseTarget target : resolver.resolve(List.of(refs.get(i))).targets()) {
                    resolved.add(new FlakinessJson.RefTarget(i, target));
                }
            }
        }

        write(
            getTargetsFile().get().getAsFile(),
            FlakinessJson.writeProjectTargets(new FlakinessJson.ProjectTargetsFile(model.projectPath(), resolved))
        );
        // Newline-TERMINATED, not newline-separated: the orchestration step simply `cat`s every project's
        // file together, so a missing trailing newline would glue two task paths into one word.
        List<String> compileTasks = FlakinessTargets.compileTaskPaths(resolved.stream().map(FlakinessJson.RefTarget::target).toList());
        write(getCompileTasksFile().get().getAsFile(), compileTasks.stream().map(t -> t + "\n").collect(Collectors.joining()));

        if (model.ownsRefs() == false) {
            getLogger().info("flakiness resolve[{}]: owns none of the {} refs", model.projectPath(), refsFile.refs().size());
            return;
        }
        getLogger().lifecycle(
            "flakiness resolve[{}]: {} refs -> {} targets (model: {} source sets, {} Test tasks)",
            model.projectPath(),
            refsFile.refs().size(),
            resolved.size(),
            model.sourceSets().size(),
            model.testTasks().size()
        );
        for (FlakinessJson.RefTarget rt : resolved) {
            BaseTarget t = rt.target();
            if (t.runnable()) {
                getLogger().lifecycle("  ref[{}] {} {} -> {}", rt.refIndex(), t.kind(), identityOf(t), t.runnableTasks());
            } else {
                getLogger().lifecycle("  ref[{}] {} {} -> skip ({})", rt.refIndex(), t.kind(), identityOf(t), t.skipReason());
            }
        }
    }

    private static void write(File file, String content) throws IOException {
        file.getParentFile().mkdirs();
        Files.writeString(file.toPath(), content);
    }

    private static String identityOf(BaseTarget t) {
        return t.fqcn() != null ? t.fqcn() : t.suitePath() != null ? t.suitePath() : t.sourceSet();
    }
}
