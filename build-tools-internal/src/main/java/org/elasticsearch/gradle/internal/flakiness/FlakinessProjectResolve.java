/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Registers the per-project resolve task ({@code flakinessResolveProject}) on every project that has test
 * sources. The task is invoked <b>unqualified</b> ({@code ./gradlew -Pflakiness.resolve flakinessResolveProject}),
 * so Gradle runs it in every project that registered it and each project decides <em>for itself</em> whether
 * it owns any of the refs - there is no caller-side project guessing and no cross-project access.
 *
 * <h2>Self-selection, and the cheap exit</h2>
 * A project owns a ref when the ref's file lies under one of <em>its own</em> source sets' {@code srcDirs}.
 * That is exactly the question {@link RefResolver} already answers, so the ownership probe simply runs the
 * real resolver against this project's source-set model with an <b>empty {@code Test}-task lookup</b>
 * ({@code path -> List.of()}). The resolver only consults {@code Test} tasks <em>after</em> it has decided a
 * ref belongs to one of the project's source sets, so the probe costs a few path comparisons plus (for class
 * refs) one {@code isRegularFile} probe per source dir - and never realizes a single {@code Test} task.
 *
 * <p>That matters: realizing {@code tasks.withType(Test)} is the expensive part of the capture (636 tasks in
 * {@code :qa:packaging} alone), and the unqualified invocation runs this in hundreds of projects. A project
 * that owns nothing emits an empty model ({@link FlakinessJson.ProjectModel#ownsRefs()} {@code false}) and
 * writes an empty result.
 *
 * <p>Self-selection also disambiguates <b>nested</b> projects correctly, which a directory-prefix heuristic
 * cannot: {@code :x-pack:plugin:logsdb} and {@code :x-pack:plugin:logsdb:qa:rolling-upgrade} have nested
 * project directories but disjoint {@code srcDirs}, so exactly one of them claims a given test file.
 *
 * <h2>Why this is configuration-cache compatible</h2>
 * The whole model is captured into a single {@code Provider<String>} used as the task's {@code @Input}:
 * <ol>
 *   <li><b>Timing.</b> Gradle asks a task-input provider for its execution-time value while <em>storing</em>
 *       the configuration cache entry - after the configuration phase has completely finished. Iterating
 *       {@code tasks.withType(Test)} at that moment realizes the tasks, running every pending configuration
 *       action on them, so {@code enabled} and {@code testClassesDirs} are the final values. In particular
 *       {@code elasticsearch.bwc-test}'s {@code tasks.named("javaRestTest") { enabled = false }} and its
 *       {@code testClassesDirs = sourceSets.javaRestTest.output.classesDirs} reassignment have both been
 *       applied, and the whole {@code v<version>#bwcTest} family exists.</li>
 *   <li><b>Serializability.</b> Because the provider is <em>replaced by its computed value</em> at store
 *       time, the entry contains a plain {@code String}. The lambda below closes over the live
 *       {@code Project}, but that closure is never serialized, and the task action never sees it.</li>
 * </ol>
 *
 * <p>The refs are read <em>inside</em> that provider (the ownership decision depends on them), which makes
 * {@code flakiness-refs.json} a configuration-cache input: a changed refs file invalidates the entry and the
 * ownership decision is recomputed. That is the correct trade - a frozen ownership decision served from a
 * stale entry would silently resolve nothing for a newly-touched project.
 *
 * <p>Everything read here belongs to <em>this</em> project: no {@code getRootProject()},
 * {@code getAllprojects()}, {@code getSubprojects()} or cross-project task lookup, so the shape stays
 * isolated-projects-clean. The repo root comes from {@code ProjectLayout.getSettingsDirectory()}, the
 * isolation-safe replacement for {@code Project.getRootDir()}.
 */
public final class FlakinessProjectResolve {

    public static final String TASK_NAME = "flakinessResolveProject";

    /**
     * Directory, relative to the repo (settings) root, where every project drops its share of the answer:
     * {@code <projectPath>.json} (the resolved targets) plus {@code <projectPath>.compile-tasks.txt} (the
     * compile task paths of its runnable targets).
     *
     * <p>It is deliberately <em>one shared directory</em> rather than each project's own build directory: the
     * consumers - {@code flakinessScan} and the orchestration shell - must discover the files without knowing
     * the project set, and globbing {@code **}{@code /build/flakiness/*.json} across the repo would mean
     * walking every build output directory in the tree. Each project writes its own uniquely named files, so
     * the tasks never overlap.
     */
    public static final String TARGETS_DIR = "build/flakiness/project-targets";

    /** Where each project dumps the model it captured, for inspection only. */
    public static final String MODEL_FILE = "flakiness/project-model.json";

    private static final String BWC_TEST_PLUGIN = "elasticsearch.bwc-test";

    private FlakinessProjectResolve() {}

    /**
     * Register {@code flakinessResolveProject} on this project. Callers must have already checked the
     * {@code -Pflakiness.resolve} gate, so a normal build never even reaches here.
     */
    public static void register(Project project, String refsPath, int taskCap) {
        Directory repoRoot = project.getLayout().getSettingsDirectory();
        Provider<String> refsJson = project.getProviders().fileContents(repoRoot.file(refsPath)).getAsText();

        // Evaluated at configuration-cache store time (see class javadoc), never at plain configuration time
        // and never at execution time.
        Provider<String> modelJson = project.provider(() -> FlakinessJson.writeProjectModel(snapshot(project, refsJson.getOrNull())));

        String base = TARGETS_DIR + "/" + fileBaseName(project.getPath());
        project.getTasks().register(TASK_NAME, FlakinessResolveProjectTask.class, t -> {
            t.setGroup("flakiness");
            t.setDescription("Resolve flakiness-refs.json against this project's own model (configuration-cache compatible)");
            t.getProjectModelJson().set(modelJson);
            t.getRefsJson().set(refsJson);
            t.getRefsPath().set(refsPath);
            t.getRepoRoot().set(repoRoot);
            t.getTaskCap().set(taskCap);
            t.getTargetsFile().set(repoRoot.file(base + ".json"));
            t.getCompileTasksFile().set(repoRoot.file(base + ".compile-tasks.txt"));
            t.getModelFile().set(project.getLayout().getBuildDirectory().file(MODEL_FILE));
        });
    }

    /**
     * A filesystem-safe, collision-free file name for a Gradle project path: {@code :x-pack:plugin:logsdb} ->
     * {@code x-pack.plugin.logsdb}, and the root project ({@code :}) -> {@code root}. Project path segments
     * cannot contain {@code :}, so the mapping is injective.
     */
    static String fileBaseName(String projectPath) {
        String stripped = projectPath.replace(':', '.');
        while (stripped.startsWith(".")) {
            stripped = stripped.substring(1);
        }
        return stripped.isEmpty() ? "root" : stripped;
    }

    /**
     * Snapshot this project's flakiness model, but only in full if this project actually owns one of the refs
     * (see the class javadoc). Invoked from the provider above, i.e. at configuration-cache store time, which
     * is what makes the {@code Test}-task facts post-mutation correct. Reuses {@link FlakinessProjectModel}'s
     * existing per-source-set and per-{@code Test}-task readers.
     */
    static FlakinessJson.ProjectModel snapshot(Project project, String refsJson) {
        String projectPath = project.getPath();
        Path projectDir = project.getProjectDir().toPath();
        Path repoRoot = project.getLayout().getSettingsDirectory().getAsFile().toPath();

        List<SourceSetInfo> sourceSets = candidateSourceSets(project);
        if (ownsAnyRef(repoRoot, new ProjectInfo(projectPath, projectDir, sourceSets), refsJson) == false) {
            // The cheap exit: no Test task is realized, and the serialized model stays a few dozen bytes.
            project.getLogger().info("flakiness: {} owns no ref; skipping Test-task realization", projectPath);
            return new FlakinessJson.ProjectModel(projectPath, projectDir, List.of(), List.of(), false, false);
        }

        project.getLogger().info("flakiness: capturing model for {} (realizing its Test tasks)", projectPath);
        List<TestTaskInfo> testTasks = FlakinessProjectModel.testTaskSnapshot(project);
        project.getLogger().info("flakiness: {} realized {} Test tasks", projectPath, testTasks.size());
        return new FlakinessJson.ProjectModel(
            projectPath,
            projectDir,
            sourceSets,
            testTasks,
            project.getPluginManager().hasPlugin(BWC_TEST_PLUGIN),
            true
        );
    }

    /** This project's flakiness-relevant test source sets. Cheap: no task is realized. */
    private static List<SourceSetInfo> candidateSourceSets(Project project) {
        List<SourceSetInfo> sourceSets = new ArrayList<>();
        JavaPluginExtension java = project.getExtensions().findByType(JavaPluginExtension.class);
        if (java != null) {
            for (SourceSet ss : java.getSourceSets()) {
                if (FlakinessProjectModel.CANDIDATE_SOURCE_SETS.contains(ss.getName())) {
                    sourceSets.add(FlakinessProjectModel.sourceSetInfo(project, ss));
                }
            }
        }
        return sourceSets;
    }

    /**
     * Whether any ref resolves into one of this project's source sets, decided by the real
     * {@link RefResolver} against an empty {@code Test}-task lookup - so the probe answers ownership without
     * realizing anything. Returning {@code false} here is what makes the unqualified invocation affordable.
     */
    static boolean ownsAnyRef(Path repoRoot, ProjectInfo project, String refsJson) {
        if (project.sourceSets().isEmpty() || refsJson == null || refsJson.isBlank()) {
            return false;
        }
        List<FlakinessRef> refs = FlakinessJson.parseRefs(refsJson).refs();
        if (refs.isEmpty()) {
            return false;
        }
        return new RefResolver(repoRoot, List.of(project), path -> List.of(), 0).resolve(refs).targets().isEmpty() == false;
    }
}
