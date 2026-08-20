/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import org.elasticsearch.gradle.internal.flakiness.FlakinessPlan.Unresolved;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Pure resolution of heterogeneous {@link FlakinessRef}s to {@link BaseTarget}s using the authoritative
 * project model ({@link ProjectInfo} snapshots taken from each project's own configuration). This is the
 * authoritative replacement for the TypeScript
 * {@code detectors/changed-files.ts} (classify half), {@code detectors/locator.ts}, and
 * {@code detectors/bwc.ts}.
 *
 * <p>Resolution is done entirely against the model's real {@code srcDirs} / {@code outputDir} /
 * {@code compileTaskPath} - it no longer assumes the {@code src/&lt;ss&gt;/java} layout, so a project with a
 * non-standard source layout resolves correctly.
 *
 * <p>Every produced target is also given its <b>disposition</b>: {@link TestTaskSelector} names the enabled
 * {@code Test} tasks that really run the target's source-set output (so a project that disables the bare
 * conventional task - bwc, packaging - resolves to its real tasks or to a precise skip reason, instead of
 * silently emitting a task Gradle reports {@code SKIPPED}). The {@code Test}-task facts are supplied by a
 * per-project lookup. {@link FlakinessProjectResolvePlugin} deliberately passes an <em>empty</em> lookup when it
 * only needs to know whether this project owns a ref, so the ownership probe realizes no {@code Test} task.
 *
 * <p>Two resolution paths:
 * <ul>
 *   <li><b>changed-file</b> refs carry a repo-relative path; the owning project is the longest
 *       {@code projectDir}-prefix match, and the source set / kind / fqcn come from which of that project's
 *       source-set {@code srcDirs} actually contains the file. A changed file not under any recognised test
 *       source dir is silently ignored (matching today's behaviour) - it is not surfaced as
 *       {@code unresolved}.</li>
 *   <li><b>unmute</b> / <b>explicit</b> refs carry only a class (and optional method); the owning source set
 *       is the one whose java {@code srcDirs} actually contain {@code &lt;pkg&gt;/&lt;Name&gt;.java} on disk (a
 *       filesystem probe - see JAVA_RESOLVER_NOTES.md P3). A ref that resolves to no source file is surfaced
 *       as {@code unresolved} with reason {@code "no-source-file"}.</li>
 * </ul>
 */
public final class RefResolver {

    private static final Pattern YAML_METHOD = Pattern.compile("^test \\{yaml=.+\\}$");
    private static final String YAML_METHOD_PREFIX = "test {yaml=";
    // Java method identifier heuristic used to split "Class.method" specs (camelCase starting lowercase).
    private static final Pattern METHOD_TAIL = Pattern.compile("^[a-z][A-Za-z0-9_]*$");

    // The yaml suite layout: suites live under <resourceDir>/rest-api-spec/test/<suitePath>.yml. This is an
    // ES-wide convention baked into ESClientYamlSuiteTestCase itself (not a per-project source-layout
    // assumption), so it is safe to encode here.
    private static final String YAML_SUITE_SUBDIR = "rest-api-spec/test/";
    private static final String YAML_SUFFIX = ".yml";

    // Ordered source-set -> kind mapping for Java files. yamlRestTest is special-cased (case vs runner).
    private static final Map<String, String> JAVA_SOURCE_SET_KIND = new LinkedHashMap<>();
    static {
        JAVA_SOURCE_SET_KIND.put(Kinds.SS_TEST, Kinds.TEST);
        JAVA_SOURCE_SET_KIND.put(Kinds.SS_INTERNAL_CLUSTER_TEST, Kinds.INTERNAL_CLUSTER_TEST);
        JAVA_SOURCE_SET_KIND.put(Kinds.SS_JAVA_REST_TEST, Kinds.JAVA_REST_TEST);
        JAVA_SOURCE_SET_KIND.put(Kinds.SS_YAML_REST_TEST, Kinds.YAML_REST_TEST_RUNNER);
    }

    private final Path repoRoot;
    private final List<ProjectInfo> projects;
    private final Function<String, List<TestTaskInfo>> testTasks;
    private final int taskCap;

    /**
     * @param testTasks per-project-path lookup of the project's post-configuration {@code Test} tasks
     * @param taskCap   max tasks a single target may fan out to (see {@link TestTaskSelector#DEFAULT_TASK_CAP})
     */
    public RefResolver(Path repoRoot, List<ProjectInfo> projects, Function<String, List<TestTaskInfo>> testTasks, int taskCap) {
        this.repoRoot = repoRoot.toAbsolutePath().normalize();
        // Longest projectDir first so a nested project wins over its ancestor.
        this.projects = projects.stream()
            .sorted(Comparator.comparingInt((ProjectInfo p) -> p.projectDir().toAbsolutePath().normalize().toString().length()).reversed())
            .toList();
        this.testTasks = testTasks;
        this.taskCap = taskCap;
    }

    /** The result of resolving a batch of refs: the base targets plus any refs that could not be resolved. */
    public record Resolution(List<BaseTarget> targets, List<Unresolved> unresolved) {}

    public Resolution resolve(List<FlakinessRef> refs) {
        List<BaseTarget> targets = new ArrayList<>();
        List<Unresolved> unresolved = new ArrayList<>();
        for (FlakinessRef ref : refs) {
            switch (ref.source()) {
                case FlakinessRef.SOURCE_CHANGED_FILE -> resolveChangedFile(ref).ifPresent(targets::add);
                case FlakinessRef.SOURCE_UNMUTE, FlakinessRef.SOURCE_EXPLICIT -> {
                    Optional<BaseTarget> t = resolveClassRef(ref);
                    if (t.isPresent()) {
                        targets.add(t.get());
                    } else {
                        unresolved.add(new Unresolved(ref, "no-source-file"));
                    }
                }
                default -> unresolved.add(new Unresolved(ref, "unknown-source"));
            }
        }
        return new Resolution(dedupe(targets), unresolved);
    }

    private Optional<BaseTarget> resolveChangedFile(FlakinessRef ref) {
        String path = ref.path();
        if (path == null) {
            return Optional.empty();
        }
        Path abs = repoRoot.resolve(path).toAbsolutePath().normalize();
        ProjectInfo owner = ownerOf(abs);
        if (owner == null) {
            return Optional.empty();
        }
        // Java test file: <javaSrcDir>/<pkg>/<Name>.java. Iterate source sets in a fixed kind order so
        // resolution is deterministic even in the (improbable) case of overlapping source dirs.
        for (Map.Entry<String, String> e : JAVA_SOURCE_SET_KIND.entrySet()) {
            Optional<SourceSetInfo> maybe = owner.sourceSet(e.getKey());
            if (maybe.isEmpty()) {
                continue;
            }
            SourceSetInfo ss = maybe.get();
            for (Path srcDir : ss.javaSrcDirs()) {
                Path rel = relativeUnder(srcDir, abs);
                if (rel != null && rel.toString().endsWith(".java")) {
                    if (ss.name().equals(Kinds.SS_YAML_REST_TEST)) {
                        // A changed yaml runner Java file re-runs the whole source set; no fqcn.
                        return Optional.of(target(owner, ss, Kinds.YAML_REST_TEST_RUNNER, null, null, null));
                    }
                    String fqcn = stripSuffix(rel.toString(), ".java").replace('/', '.').replace('\\', '.');
                    return Optional.of(target(owner, ss, e.getValue(), fqcn, null, null));
                }
            }
        }
        // Yaml suite resource: <resourceDir>/rest-api-spec/test/<suitePath>.yml
        Optional<SourceSetInfo> yaml = owner.sourceSet(Kinds.SS_YAML_REST_TEST);
        if (yaml.isPresent()) {
            for (Path resDir : yaml.get().resourceSrcDirs()) {
                Path rel = relativeUnder(resDir, abs);
                if (rel == null) {
                    continue;
                }
                String relStr = rel.toString().replace('\\', '/');
                if (relStr.startsWith(YAML_SUITE_SUBDIR) && relStr.endsWith(YAML_SUFFIX)) {
                    String suitePath = stripSuffix(relStr.substring(YAML_SUITE_SUBDIR.length()), YAML_SUFFIX);
                    return Optional.of(target(owner, yaml.get(), Kinds.YAML_REST_TEST_SUITE, null, suitePath, null));
                }
            }
        }
        return Optional.empty();
    }

    private Optional<BaseTarget> resolveClassRef(FlakinessRef ref) {
        ClassMethod cm = classMethodOf(ref);
        if (cm == null || cm.className() == null || cm.className().isBlank()) {
            return Optional.empty();
        }
        String suffix = cm.className().replace('.', '/') + ".java";
        // Iterate projects (longest projectDir first) and their java source sets in a fixed order so the
        // resolution is deterministic when, improbably, two projects would both contain the file.
        for (ProjectInfo p : projects) {
            for (String ssName : JAVA_SOURCE_SET_KIND.keySet()) {
                Optional<SourceSetInfo> maybe = p.sourceSet(ssName);
                if (maybe.isEmpty()) {
                    continue;
                }
                SourceSetInfo ss = maybe.get();
                for (Path srcDir : ss.javaSrcDirs()) {
                    Path candidate = srcDir.resolve(suffix);
                    if (Files.isRegularFile(candidate)) {
                        if (ss.name().equals(Kinds.SS_YAML_REST_TEST)) {
                            if (cm.method() != null && YAML_METHOD.matcher(cm.method()).matches()) {
                                return Optional.of(target(p, ss, Kinds.YAML_REST_TEST_CASE, cm.className(), null, cm.method()));
                            }
                            return Optional.of(target(p, ss, Kinds.YAML_REST_TEST_RUNNER, null, null, null));
                        }
                        return Optional.of(target(p, ss, JAVA_SOURCE_SET_KIND.get(ssName), cm.className(), null, null));
                    }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Build the target and resolve its disposition. The conventional bare task name for every kind we handle
     * is the source-set name itself ({@code test}/{@code internalClusterTest}/{@code javaRestTest}/
     * {@code yamlRestTest}), which is what {@link TestTaskSelector} treats as canonical when it is enabled.
     */
    private BaseTarget target(ProjectInfo p, SourceSetInfo ss, String kind, String fqcn, String suitePath, String yamlTest) {
        TestTaskSelector.Selection selection = TestTaskSelector.select(
            ss.name(),
            ss.outputDir(),
            testTasks.apply(p.projectPath()),
            taskCap
        );
        return new BaseTarget(
            p.projectPath(),
            ss.name(),
            kind,
            fqcn,
            suitePath,
            yamlTest,
            ss.compileTaskPath(),
            ss.outputDir() == null ? null : ss.outputDir().toString(),
            selection.taskPaths(),
            selection.candidateCount(),
            selection.skipReason()
        );
    }

    private ProjectInfo ownerOf(Path abs) {
        for (ProjectInfo p : projects) {
            Path dir = p.projectDir().toAbsolutePath().normalize();
            if (abs.equals(dir) || abs.startsWith(dir)) {
                return p; // projects are sorted longest-dir-first, so the first match is the deepest owner
            }
        }
        return null;
    }

    /** Repo-relative-ish remainder of {@code file} under {@code dir}, or {@code null} if not under it. */
    private static Path relativeUnder(Path dir, Path file) {
        Path d = dir.toAbsolutePath().normalize();
        if (file.startsWith(d) == false) {
            return null;
        }
        return d.relativize(file);
    }

    private static String stripSuffix(String s, String suffix) {
        return s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : s;
    }

    /**
     * Collapse targets that address the same (project, kind, identity). Package-private rather than private
     * so the fold of the per-project answers ({@link FlakinessTargets#merge}) applies exactly the same rule.
     */
    static List<BaseTarget> dedupe(List<BaseTarget> targets) {
        Map<String, BaseTarget> seen = new LinkedHashMap<>();
        for (BaseTarget t : targets) {
            String identity = t.yamlTest() != null ? t.yamlTest()
                : t.fqcn() != null ? t.fqcn()
                : t.suitePath() != null ? t.suitePath()
                : "";
            seen.putIfAbsent(t.gradleProject() + "|" + t.kind() + "|" + identity, t);
        }
        return new ArrayList<>(seen.values());
    }

    record ClassMethod(String className, String method) {}

    /** Extract (class, method) from a ref: unmute refs carry them directly; explicit refs parse a spec. */
    private static ClassMethod classMethodOf(FlakinessRef ref) {
        if (FlakinessRef.SOURCE_EXPLICIT.equals(ref.source())) {
            return ref.spec() == null ? null : parseSpec(ref.spec().trim());
        }
        return new ClassMethod(ref.className(), ref.method());
    }

    /**
     * Parse an explicit spec string, mirroring {@code detectors/explicit-list.ts#parseSpec}:
     * {@code Class."test {yaml=...}"}, {@code Class.method}, or bare {@code Class}.
     */
    static ClassMethod parseSpec(String spec) {
        int yamlIdx = spec.indexOf("." + YAML_METHOD_PREFIX);
        if (yamlIdx != -1) {
            return new ClassMethod(spec.substring(0, yamlIdx), spec.substring(yamlIdx + 1));
        }
        int lastDot = spec.lastIndexOf('.');
        if (lastDot != -1) {
            String tail = spec.substring(lastDot + 1);
            if (METHOD_TAIL.matcher(tail).matches()) {
                return new ClassMethod(spec.substring(0, lastDot), tail);
            }
        }
        return new ClassMethod(spec, null);
    }
}
