/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The per-project half of flakiness resolution: registers {@code flakinessResolveProject} on the project it is
 * applied to. Applied to every test project by
 * {@link org.elasticsearch.gradle.internal.ElasticsearchTestBasePlugin}; the root-project half - just
 * {@code flakinessScan} - is {@link FlakinessResolvePlugin}.
 *
 * <p>Like {@link FlakinessResolvePlugin}, it is gated on {@code -Pflakiness.resolve}
 * ({@link FlakinessProperties#enabled}) and returns immediately from {@link #apply} without it, so a normal
 * build pays nothing beyond instantiating the plugin.
 *
 * <p>The task is invoked <b>unqualified</b> ({@code ./gradlew -Pflakiness.resolve flakinessResolveProject}),
 * so Gradle runs it in every project that registered it and each project decides <em>for itself</em> whether
 * it owns any of the refs - there is no caller-side project guessing and no cross-project access.
 *
 * <h2>Self-selection</h2>
 * A project owns a ref when the ref's file lies under one of <em>its own</em> source sets' {@code srcDirs},
 * which is exactly the question {@link RefResolver} answers. Self-selection also disambiguates <b>nested</b>
 * projects correctly, which a directory-prefix heuristic cannot: {@code :x-pack:plugin:logsdb} and
 * {@code :x-pack:plugin:logsdb:qa:rolling-upgrade} have nested project directories but disjoint
 * {@code srcDirs}, so exactly one of them claims a given test file.
 *
 * <h2>Why every project captures its full model</h2>
 * An earlier version short-circuited here: a project that owned no ref skipped realizing its {@code Test}
 * tasks and emitted an empty model. That shortcut is gone, because <em>owning no ref does not make a project
 * irrelevant</em>. Expanding an abstract test base is a repo-wide bytecode question, and its answers - the
 * concrete subclasses - routinely live in projects no ref pointed at. Running one of those subclasses needs
 * its own source set's {@code Test} tasks, so every project now reports a {@link SourceSetDisposition} per
 * candidate test source set and the scan step joins them by compiled-output directory.
 *
 * <p>The cost of dropping the shortcut is realizing {@code tasks.withType(Test)} everywhere rather than in the
 * handful of owning projects - it is what {@link FlakinessProjectModel#testTaskSnapshot} is for, and it was
 * measured before being adopted (see JAVA_RESOLVER_NOTES.md). Projects with no candidate test source set still
 * skip it, since they have nothing a flakiness run could execute.
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
public class FlakinessProjectResolvePlugin implements Plugin<Project> {

    public static final String TASK_NAME = "flakinessResolveProject";

    /**
     * Directory, relative to the repo (settings) root, where every project drops its share of the answer as
     * {@code <projectPath>.json} (its resolved targets, plus its {@code classDirs}).
     *
     * <p>It is deliberately <em>one shared directory</em> rather than each project's own build directory:
     * {@code flakinessScan} must discover the files without knowing the project set, and globbing
     * {@code **}{@code /build/flakiness/*.json} across the repo would mean walking every build output
     * directory in the tree. Each project writes its own uniquely named file, so the tasks never overlap.
     */
    public static final String TARGETS_DIR = "build/flakiness/project-targets";

    /** Where each project dumps the model it captured, for inspection only. */
    public static final String MODEL_FILE = "flakiness/project-model.json";

    private static final String BWC_TEST_PLUGIN = "elasticsearch.bwc-test";

    @Override
    public void apply(Project project) {
        if (FlakinessProperties.enabled(project) == false) {
            return; // inert unless explicitly enabled by the resolve/scan Buildkite steps
        }

        String refsPath = FlakinessProperties.refsPath(project);
        int taskCap = FlakinessProperties.taskCap(project);

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
            t.getModelFile().set(project.getLayout().getBuildDirectory().file(MODEL_FILE));
        });
    }

    /**
     * A filesystem-safe, collision-free file name for a Gradle project path: {@code :x-pack:plugin:logsdb} ->
     * {@code x-pack.plugin.logsdb}, and the root project ({@code :}) -> {@code root}.
     *
     * <p>The mapping must be <b>injective</b>: every project writes into one shared directory, so two paths
     * mapping to the same name would make one project's task silently overwrite the other's output. Project
     * path segments cannot contain {@code :}, but they <em>can</em> contain {@code .} (segment names come from
     * directory names), so replacing {@code :} with {@code .} alone is not injective - {@code :libs:x.y} and
     * {@code :libs:x:y} would collide. The {@code .} (and the {@code %} of the escape itself) are therefore
     * percent-encoded first. No Elasticsearch project has a dotted name today, so in practice every real name
     * is left unchanged.
     */
    static String fileBaseName(String projectPath) {
        // Escape '%' before '.', so decoding stays unambiguous.
        String stripped = projectPath.replace("%", "%25").replace(".", "%2E").replace(':', '.');
        while (stripped.startsWith(".")) {
            stripped = stripped.substring(1);
        }
        return stripped.isEmpty() ? "root" : stripped;
    }

    // TODO jozala - Gradle Tooling API - check if it can help with gathering data about the test tasks in a project without realizing them.
    /**
     * Snapshot this project's flakiness model, but only in full if this project actually owns one of the refs
     * (see the class javadoc). Invoked from the provider above, i.e. at configuration-cache store time, which
     * is what makes the {@code Test}-task facts post-mutation correct. Reuses {@link FlakinessProjectModel}'s
     * existing per-source-set and per-{@code Test}-task readers.
     */
    static FlakinessJson.ProjectModel snapshot(Project project, String refsJson) {
        String projectPath = project.getPath();
        Path projectDir = project.getProjectDir().toPath();

        List<SourceSetInfo> sourceSets = candidateSourceSets(project);
        List<Path> classDirs = FlakinessProjectModel.scannedClassDirs(project);
        List<TestTaskInfo> testTasks = sourceSets.isEmpty() ? List.of() : FlakinessProjectModel.testTaskSnapshot(project);
        project.getLogger()
            .info("flakiness: captured {} ({} source sets, {} Test tasks)", projectPath, sourceSets.size(), testTasks.size());
        return new FlakinessJson.ProjectModel(
            projectPath,
            projectDir,
            sourceSets,
            testTasks,
            classDirs,
            project.getPluginManager().hasPlugin(BWC_TEST_PLUGIN)
        );
    }

    /** This project's flakiness-relevant test source sets. Cheap: no task is realized. */
    private static List<SourceSetInfo> candidateSourceSets(Project project) {
        List<SourceSetInfo> sourceSets = new ArrayList<>();
        JavaPluginExtension java = project.getExtensions().findByType(JavaPluginExtension.class);
        if (java != null) {
            for (SourceSet ss : java.getSourceSets()) {
                if (FlakinessProjectModel.CANDIDATE_SOURCE_SETS.contains(ss.getName())) {
                    sourceSets.add(FlakinessProjectModel.sourceSetInfo(ss));
                }
            }
        }
        return sourceSets;
    }

}
