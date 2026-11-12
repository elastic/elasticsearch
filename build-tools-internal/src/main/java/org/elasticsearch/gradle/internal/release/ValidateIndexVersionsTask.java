/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ParserConfiguration.LanguageLevel;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.MethodCallExpr;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.transport.TransportVersionResourcesService;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.provider.Property;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.release.AbstractVersionsTask.INDEX_VERSIONS_FILE_PATH;
import static org.elasticsearch.gradle.internal.release.AbstractVersionsTask.INDEX_VERSIONS_RECORD;

/**
 * Validates the ids in {@code IndexVersions.java}. Index version gates such as {@code onOrAfter(X)} assume that an index with a higher id
 * has every change gated at or below that id. A release branch breaks that assumption when it adds an id that skips ids main has, or that
 * main uses for a different change: 9.5.2 shipped {@code 9_111_0_00} without main's {@code 9_108_0_00} to {@code 9_110_0_00}.
 *
 * <p>Two checks run:
 * <ul>
 *   <li>Once the branch's {@code x.y.0} is released (listed in {@code IndexVersions.csv}), no id may have a higher server version part
 *   than the id {@code x.y.0} shipped with, so the branch can only add patch ids.</li>
 *   <li>In a PR build whose target branch is a release branch or the serverless patch branch, every id must exist on main with the same
 *   name, and every main id below the branch's highest id must exist on the branch, except patch ids of older branches.
 *   A base id the change adds must not follow a patch id, and a change that adds ids must raise the branch's highest id. Reads main, and
 *   the target branch at the commit the change branched from, from git.</li>
 * </ul>
 */
public abstract class ValidateIndexVersionsTask extends DefaultTask {

    // 9.5.2 shipped 9_111_0_00 without main's 9_108_0_00 to 9_110_0_00. IndexSettings#createdInStateful95 on main relies on 9.5 never
    // defining an id above 9_111_0_99, and main reads 9.5 indices wrongly if 9.5 defines any id between 9.5.0's 9_107_0_00 and 9_111_0_00.
    static final int FIRST_BASE_ID_9_5 = 9_107_0_00;
    static final Integer FROZEN_BASE_ID_9_5 = 9_111_0_00;
    static final Set<Integer> MISSING_FROM_MAIN_ON_9_5 = Set.of(9_108_0_00, 9_109_0_00, 9_110_0_00);

    // IndexVersions#FIRST_DETACHED_INDEX_VERSION. Lower ids use the format of releases before 8.11 and are never the id new indices get.
    static final int FIRST_DETACHED_ID = 8_500_000;

    // appended once after all errors, since every error has the same cause and fix
    private static final String HOW_TO_FIX =
        "Do not renumber an index version to match main. On a release branch, add a patch id on main first and"
            + " then backport that exact line. See docs/internal/Versioning.md#backporting-an-index-version";

    private final Path rootDir;

    @Inject
    public ValidateIndexVersionsTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
    }

    @ServiceReference("transportVersionResources")
    abstract Property<TransportVersionResourcesService> getGit();

    /** The serverless patch branch, from {@code org.elasticsearch.transport.patchBranch}; unset on repositories without one. */
    @Input
    @Optional
    public abstract Property<String> getPatchBranchName();

    @TaskAction
    public void validate() throws IOException {
        TransportVersionResourcesService git = getGit().get();
        List<String> errors = check(
            VersionProperties.getElasticsearchVersion(),
            Files.readString(rootDir.resolve(INDEX_VERSIONS_FILE_PATH)),
            Files.readString(rootDir.resolve(INDEX_VERSIONS_RECORD)),
            // only PR builds compare with main, so a change on main never blocks a release branch's builds after merge
            System.getenv("BUILDKITE_PULL_REQUEST_BASE_BRANCH"),
            getPatchBranchName().getOrNull(),
            () -> git.getFileFromElasticBranch("main", INDEX_VERSIONS_FILE_PATH),
            // the target as the change branched from it, since CI builds the change without merging the target's later commits into it
            () -> git.getFileFromBaseRef(INDEX_VERSIONS_FILE_PATH)
        );
        if (errors.isEmpty() == false) {
            throw new GradleException(String.join(System.lineSeparator(), errors) + System.lineSeparator() + HOW_TO_FIX);
        }
    }

    /** Runs every check. {@code main} and {@code targetBase} return {@code IndexVersions.java} of main and of the target branch where the change branched off, and are only called for targets compared with main. */
    static List<String> check(
        Version version,
        String branchSource,
        String releasedVersionsCsv,
        String target,
        String patchBranch,
        Supplier<String> main,
        Supplier<String> targetBase
    ) {
        Map<Integer, String> branch = readDefinitions(branchSource);
        if (branch.isEmpty()) {
            return List.of("Found no index versions in IndexVersions.java, so this task can no longer parse it.");
        }
        boolean on95 = version.getMajor() == 9 && version.getMinor() == 5;
        Integer frozenBaseId = on95 ? FROZEN_BASE_ID_9_5 : firstReleaseBaseId(releasedVersionsCsv, version);
        List<String> errors = new ArrayList<>(checkOnlyPatchIdsAfterRelease(branch.keySet(), frozenBaseId));
        for (var definition : branch.entrySet()) {
            if (on95 && definition.getKey() > FIRST_BASE_ID_9_5 && definition.getKey() < FROZEN_BASE_ID_9_5) {
                errors.add(
                    String.format(
                        Locale.ROOT,
                        "Index version [%s] must stay off 9.5, which went from [%s] to [%s]. Add a patch id of [%s] instead.",
                        label(definition),
                        format(FIRST_BASE_ID_9_5),
                        format(FROZEN_BASE_ID_9_5),
                        format(FROZEN_BASE_ID_9_5)
                    )
                );
            }
        }
        if (comparedWithMain(target, patchBranch)) {
            errors.addAll(checkAgainstMain(branch, readDefinitions(main.get()), on95 ? MISSING_FROM_MAIN_ON_9_5 : Set.of()));
            errors.addAll(checkAddedIds(branch.keySet(), readDefinitions(targetBase.get()).keySet()));
        }
        return errors;
    }

    /**
     * Maps each index version id to its constant's name. The Lucene version is left out: main rewrites it when Lucene drops old constants, and
     * a change that reuses an id for a different change shows up as a different name.
     */
    static Map<Integer, String> readDefinitions(String source) {
        Map<Integer, String> definitions = new TreeMap<>();
        // StaticJavaParser defaults to Java 11 syntax, which rejects e.g. pattern instanceof in IndexVersions.java
        ParseResult<CompilationUnit> parsed = new JavaParser(new ParserConfiguration().setLanguageLevel(LanguageLevel.JAVA_21)).parse(
            source
        );
        CompilationUnit unit = parsed.getResult()
            .filter(u -> parsed.isSuccessful())
            .orElseThrow(() -> new ParseProblemException(parsed.getProblems()));
        for (VariableDeclarator variable : unit.findAll(VariableDeclarator.class)) {
            variable.getInitializer()
                .filter(MethodCallExpr.class::isInstance)
                .map(MethodCallExpr.class::cast)
                .filter(call -> call.getNameAsString().equals("def") && call.getArguments().isEmpty() == false)
                .filter(call -> call.getArgument(0).isIntegerLiteralExpr())
                .ifPresent(
                    call -> definitions.put(call.getArgument(0).asIntegerLiteralExpr().asNumber().intValue(), variable.getNameAsString())
                );
        }
        return definitions;
    }

    /** The base id of the index version that {@code x.y.0} of the given version shipped with, or null if {@code x.y.0} is unreleased. */
    static Integer firstReleaseBaseId(String releasedVersionsCsv, Version version) {
        String prefix = version.getMajor() + "." + version.getMinor() + ".0,";
        for (String line : releasedVersionsCsv.split("\n")) {
            if (line.startsWith(prefix)) {
                int id = Integer.parseInt(line.substring(prefix.length()).strip());
                return id - id % 100;
            }
        }
        return null;
    }

    static List<String> checkOnlyPatchIdsAfterRelease(Iterable<Integer> ids, Integer frozenBaseId) {
        List<String> errors = new ArrayList<>();
        if (frozenBaseId == null) {
            return errors;
        }
        for (int id : ids) {
            if (id / 1000 > frozenBaseId / 1000) {
                errors.add(
                    String.format(
                        Locale.ROOT,
                        "Index version [%s] is above [%s], the base id this released branch is frozen at."
                            + " Add a patch id of [%s], up to [%s], instead.",
                        format(id),
                        format(frozenBaseId),
                        format(frozenBaseId),
                        format(frozenBaseId + 99)
                    )
                );
            }
        }
        return errors;
    }

    static List<String> checkAgainstMain(Map<Integer, String> branch, Map<Integer, String> main, Set<Integer> allowedMissing) {
        List<String> errors = new ArrayList<>();
        int highestBaseId = 0;
        int highestId = 0;
        for (var definition : branch.entrySet()) {
            String onMain = main.get(definition.getKey());
            if (onMain == null) {
                errors.add("Index version [" + label(definition) + "] is not on main.");
            } else if (onMain.equals(definition.getValue()) == false) {
                errors.add(
                    "Index version [" + label(definition) + "] is named [" + onMain + "] on main. If main renamed it, backport that change."
                );
            }
            if (definition.getKey() % 100 == 0) {
                highestBaseId = Math.max(highestBaseId, definition.getKey());
            }
            highestId = Math.max(highestId, definition.getKey());
        }
        // Every main id below the branch's highest id must be on the branch. Patch ids below the branch's highest base id belong to older
        // branches, so they are exempt; patch ids above it are this branch's own, and skipping one is the same bug as skipping a base id.
        for (var definition : main.entrySet()) {
            int id = definition.getKey();
            boolean required = id < highestId && (id % 100 == 0 || id > highestBaseId);
            if (required && branch.containsKey(id) == false && allowedMissing.contains(id) == false) {
                errors.add(
                    "Index version [" + label(definition) + "] from main is missing, but this branch has [" + format(highestId) + "]."
                );
            }
        }
        return errors;
    }

    /**
     * A base id added to the branch must not follow a patch id above the target's highest base id, since main checks that patch's change with {@code between(PATCH, NEXT_BASE)}.
     * A change that adds ids must also raise the branch's highest id, which new indices are created with, or main cannot tell them apart from indices created before the change.
     */
    static List<String> checkAddedIds(Set<Integer> branch, Set<Integer> targetBase) {
        int targetHighestBase = targetBase.stream().filter(id -> id % 100 == 0).max(Integer::compare).orElse(0);
        int firstPatch = branch.stream()
            .filter(id -> id % 100 != 0 && id > targetHighestBase)
            .min(Integer::compare)
            .orElse(Integer.MAX_VALUE);
        List<String> errors = new ArrayList<>();
        for (int id : branch) {
            if (id % 100 == 0 && id > firstPatch && targetBase.contains(id) == false) {
                errors.add(
                    String.format(
                        Locale.ROOT,
                        "Index version [%s] cannot follow patch id [%s]: main checks that patch's change with between([%s], ...),"
                            + " which excludes [%s]. Add a patch id instead.",
                        format(id),
                        format(firstPatch),
                        format(firstPatch),
                        format(id)
                    )
                );
            }
        }
        // a change that also removes ids corrects a wrong id, as 9.1 and 9.2 did by replacing a copied base id with a patch id
        List<Integer> added = branch.stream().filter(id -> id >= FIRST_DETACHED_ID && targetBase.contains(id) == false).sorted().toList();
        int targetHighest = targetBase.stream().max(Integer::compare).orElse(0);
        if (added.isEmpty() == false && branch.containsAll(targetBase) && Collections.max(branch) <= targetHighest) {
            errors.add(
                String.format(
                    Locale.ROOT,
                    "Index versions %s do not raise this branch's highest id [%s], so indices created with this change would still get [%s]"
                        + " and main could not tell that they have it. Also add a new patch id above [%s].",
                    added.stream().map(ValidateIndexVersionsTask::format).toList(),
                    format(targetHighest),
                    format(targetHighest),
                    format(targetHighest)
                )
            );
        }
        return errors;
    }

    /** Release branches and the serverless patch branch follow main. Other targets, such as main and stacked PR branches, may define new ids. */
    static boolean comparedWithMain(String targetBranch, String patchBranch) {
        return targetBranch != null && (targetBranch.matches("\\d+\\.\\d+") || targetBranch.equals(patchBranch));
    }

    static String label(Map.Entry<Integer, String> definition) {
        return definition.getValue() + " = " + format(definition.getKey());
    }

    static String format(int id) {
        return String.format(Locale.ROOT, "%d_%03d_%d_%02d", id / 1_000_000, id / 1000 % 1000, id / 100 % 10, id % 100);
    }
}
