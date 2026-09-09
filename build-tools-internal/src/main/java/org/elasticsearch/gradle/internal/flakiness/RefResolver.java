/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Pure resolution of heterogeneous {@link FlakinessRef}s to {@link BaseTarget}s using the authoritative
 * project model ({@link ProjectInfo} snapshots taken from each project's own configuration). This is the
 * authoritative replacement for the TypeScript
 * {@code detectors/changed-files.ts} (classify half), {@code detectors/locator.ts}, and
 * {@code detectors/bwc.ts}.
 *
 * <p>Resolution is done against the model's real {@code srcDirs} / {@code outputDir} rather than the
 * {@code src/&lt;ss&gt;/java} layout, so a project with a non-standard source layout resolves correctly. The
 * one path assumption that remains is the project-directory early-out in {@code resolveChangedFile}, which is
 * documented there.
 *
 * <p>Every produced target is also given its <b>disposition</b>: {@link TestTaskSelector} names the enabled
 * {@code Test} tasks that really run the target's source-set output (so a project that disables the bare
 * conventional task - bwc, packaging - resolves to its real tasks or to a precise skip reason, instead of
 * silently emitting a task Gradle reports {@code SKIPPED}).
 *
 * <p>The resolver is deliberately <b>single-project</b>: it is constructed with one {@link ProjectInfo} and
 * that project's own {@code Test} tasks, which is exactly what {@code flakinessResolveProject} has to hand.
 * Folding several projects' answers together is a separate, later concern - see
 * {@link FlakinessTargets#merge}, which is also where the global {@code unresolved} verdict is computed,
 * because "not in <em>this</em> project" is not "not anywhere".
 *
 * <p>Two resolution paths:
 * <ul>
 *   <li><b>changed-file</b> refs carry a repo-relative path; it must lie under this project's directory, and
 *       the source set / kind / fqcn come from which of the project's source-set {@code srcDirs} actually
 *       contains the file. That last check is what disambiguates nested projects, which a directory-prefix
 *       test cannot: {@code :x-pack:plugin:logsdb} and {@code :x-pack:plugin:logsdb:qa:rolling-upgrade} have
 *       nested directories but disjoint {@code srcDirs}, so only one of them claims a given file. A changed
 *       file not under any recognised test source dir is silently ignored (matching today's behaviour) - it is
 *       not surfaced as {@code unresolved}.</li>
 *   <li><b>unmute</b> / <b>explicit</b> refs carry only a class (and optional method); the owning source set
 *       is the one whose java {@code srcDirs} actually contain {@code &lt;pkg&gt;/&lt;Name&gt;.java} on disk (a
 *       filesystem probe - see JAVA_RESOLVER_NOTES.md P3). A ref that resolves to no source file is surfaced
 *       as {@code unresolved} with reason {@code "no-source-file"}.</li>
 * </ul>
 */
public final class RefResolver {

    /** Unresolved reason: the ref names a class, but no source file for it exists in any source set. */
    public static final String REASON_NO_SOURCE_FILE = "no-source-file";

    /**
     * Unresolved reason: the ref's {@code source} discriminator is absent or not one this resolver knows.
     * That is a contract defect between the TypeScript bootstrap (which writes {@code flakiness-refs.json})
     * and this resolver, so it is reported rather than skipped - a dropped ref would read as "nothing to run".
     */
    public static final String REASON_UNKNOWN_SOURCE = "unknown-source";

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

    /**
     * The wire kind a Java class in the named source set resolves to, or {@code null} for a source set this
     * resolver does not handle. Exposed so the per-project resolve task can label its
     * {@link SourceSetDisposition}s with the same mapping the resolver itself uses, rather than duplicating it.
     */
    public static String javaKindOf(String sourceSetName) {
        return JAVA_SOURCE_SET_KIND.get(sourceSetName);
    }

    private final Path repoRoot;
    private final ProjectInfo project;
    private final List<TestTaskInfo> testTasks;
    private final Path projectDir;
    private final int taskCap;

    /**
     * @param project   the one project to resolve against (see the class javadoc on single-project scope)
     * @param testTasks that project's post-configuration {@code Test} tasks. Pass {@link List#of()} to resolve
     *                  source-set membership without a disposition - every target then comes back with
     *                  {@link TestTaskSelector#REASON_NO_RUNNABLE_TASK}, which is how
     *                  {@code FlakinessOwnershipTests} asserts membership without fabricating
     *                  {@code Test}-task fixtures
     * @param taskCap   max tasks a single target may fan out to (see {@link TestTaskSelector#DEFAULT_TASK_CAP})
     */
    public RefResolver(Path repoRoot, ProjectInfo project, List<TestTaskInfo> testTasks, int taskCap) {
        this.repoRoot = repoRoot.toAbsolutePath().normalize();
        this.project = project;
        this.testTasks = testTasks;
        this.projectDir = project.projectDir().toAbsolutePath().normalize();
        this.taskCap = taskCap;
    }

    /**
     * Resolve <b>one</b> ref against this project: the target it names, or empty when this project does not
     * claim it.
     *
     * <p>One ref per call, deliberately. A batch signature could not express the answer the caller needs: a
     * flat list of targets loses which ref produced which target, and that mapping is not recoverable
     * afterwards because {@code target -> ref} is not a function - an {@code unmute} and an {@code explicit}
     * ref naming the same class dedupe to a single target. {@code FlakinessTargets#merge} needs the mapping
     * for two things, so the caller pairs each answer with its ref index as it goes:
     * <ul>
     *   <li>the global {@code unresolved} verdict - a ref is unresolved exactly when <em>no</em> project
     *       produced a target for its index;</li>
     *   <li>restoring the refs file's ordering across projects.</li>
     * </ul>
     *
     * <p>It deliberately reports <b>no reason</b> when it returns empty. This project failing to claim a ref is
     * not a verdict on the ref - "not in <em>this</em> project" is not "not anywhere" - so the reason would be
     * a value this class is not entitled to produce. {@code FlakinessTargets#merge} owns that classification,
     * using {@link #REASON_NO_SOURCE_FILE} / {@link #REASON_UNKNOWN_SOURCE}, because it is the only place with
     * the global view. Empty therefore covers all three of: no source file here, a source this resolver does
     * not know, and a changed file under no recognised test source dir (which is silently ignored rather than
     * reported, since it is simply not a test).
     */
    public Optional<BaseTarget> resolve(FlakinessRef ref) {
        // A null source would make the switch throw NPE. A refs file with a missing/misspelled `source` is a
        // malformed input, not a programming error, so it resolves to nothing here and merge reports it as
        // unknown-source.
        String source = ref.source();
        if (source == null) {
            return Optional.empty();
        }
        return switch (source) {
            case FlakinessRef.SOURCE_CHANGED_FILE -> resolveChangedFile(ref);
            case FlakinessRef.SOURCE_UNMUTE, FlakinessRef.SOURCE_EXPLICIT -> resolveClassRef(ref);
            default -> Optional.empty();
        };
    }

    private Optional<BaseTarget> resolveChangedFile(FlakinessRef ref) {
        String path = ref.path();
        if (path == null) {
            return Optional.empty();
        }
        Path abs = repoRoot.resolve(path).toAbsolutePath().normalize();
        // An early-out, not the membership decision: a path under this project but outside every srcDir also
        // falls through to Optional.empty() below. It is kept because it reproduces the old multi-project
        // ownerOf() lookup exactly. It is NOT implied by the srcDirs checks, though: a source set's srcDirs
        // may point outside its own project - esql-datasource-parquet-rs/qa adds a sibling project's
        // directory to javaRestTest.resources - and such a path is rejected here before those checks see it.
        // That is harmless only because those directories hold .csv-spec files, so the yaml-suite matcher
        // (rest-api-spec/test/*.yml) would not have matched them anyway. Widening this is a behaviour change,
        // so it is deliberately left alone.
        if (abs.equals(projectDir) == false && abs.startsWith(projectDir) == false) {
            return Optional.empty();
        }
        // Java test file: <javaSrcDir>/<pkg>/<Name>.java. Iterate source sets in a fixed kind order so
        // resolution is deterministic even in the (improbable) case of overlapping source dirs.
        for (Map.Entry<String, String> e : JAVA_SOURCE_SET_KIND.entrySet()) {
            Optional<SourceSetInfo> maybe = project.sourceSet(e.getKey());
            if (maybe.isEmpty()) {
                continue;
            }
            SourceSetInfo ss = maybe.get();
            for (Path srcDir : ss.javaSrcDirs()) {
                Path rel = relativeUnder(srcDir, abs);
                if (rel != null && rel.toString().endsWith(".java")) {
                    if (ss.name().equals(Kinds.SS_YAML_REST_TEST)) {
                        // A changed yaml runner Java file re-runs the whole source set; no fqcn.
                        return Optional.of(target(ss, Kinds.YAML_REST_TEST_RUNNER, null, null, null));
                    }
                    String fqcn = stripSuffix(rel.toString(), ".java").replace('/', '.').replace('\\', '.');
                    return Optional.of(target(ss, e.getValue(), fqcn, null, null));
                }
            }
        }
        // Yaml suite resource: <resourceDir>/rest-api-spec/test/<suitePath>.yml
        Optional<SourceSetInfo> yaml = project.sourceSet(Kinds.SS_YAML_REST_TEST);
        if (yaml.isPresent()) {
            for (Path resDir : yaml.get().resourceSrcDirs()) {
                Path rel = relativeUnder(resDir, abs);
                if (rel == null) {
                    continue;
                }
                String relStr = rel.toString().replace('\\', '/');
                if (relStr.startsWith(YAML_SUITE_SUBDIR) && relStr.endsWith(YAML_SUFFIX)) {
                    String suitePath = stripSuffix(relStr.substring(YAML_SUITE_SUBDIR.length()), YAML_SUFFIX);
                    return Optional.of(target(yaml.get(), Kinds.YAML_REST_TEST_SUITE, null, suitePath, null));
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
        // Iterate this project's java source sets in a fixed order so resolution is deterministic even in the
        // (improbable) case of overlapping source dirs.
        for (String ssName : JAVA_SOURCE_SET_KIND.keySet()) {
            Optional<SourceSetInfo> maybe = project.sourceSet(ssName);
            if (maybe.isEmpty()) {
                continue;
            }
            SourceSetInfo ss = maybe.get();
            for (Path srcDir : ss.javaSrcDirs()) {
                Path candidate = srcDir.resolve(suffix);
                if (Files.isRegularFile(candidate)) {
                    if (ss.name().equals(Kinds.SS_YAML_REST_TEST)) {
                        if (cm.method() != null && YAML_METHOD.matcher(cm.method()).matches()) {
                            return Optional.of(target(ss, Kinds.YAML_REST_TEST_CASE, cm.className(), null, cm.method()));
                        }
                        return Optional.of(target(ss, Kinds.YAML_REST_TEST_RUNNER, null, null, null));
                    }
                    return Optional.of(target(ss, JAVA_SOURCE_SET_KIND.get(ssName), cm.className(), null, null));
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
    private BaseTarget target(SourceSetInfo ss, String kind, String fqcn, String suitePath, String yamlTest) {
        TestTaskSelector.Selection selection = TestTaskSelector.select(ss.name(), ss.outputDir(), testTasks, taskCap);
        return new BaseTarget(
            project.projectPath(),
            ss.name(),
            kind,
            fqcn,
            suitePath,
            yamlTest,
            selection.taskPaths(),
            selection.candidateCount(),
            selection.skipReason()
        );
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
