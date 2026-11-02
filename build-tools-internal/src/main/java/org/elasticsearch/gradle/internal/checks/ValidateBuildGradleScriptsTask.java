/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks;

import org.elasticsearch.gradle.internal.conventions.problems.ElasticsearchBuildProblems;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.problems.Problem;
import org.gradle.api.problems.ProblemId;
import org.gradle.api.problems.ProblemReporter;
import org.gradle.api.problems.Problems;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * Validates repository {@code build.gradle} scripts against authoring guardrails.
 *
 * <p>The task is intentionally repository-wide and root-owned. Each rule produces structured
 * Gradle Problems so failures are visible in the problems report as well as the console output.
 */
@CacheableTask
public abstract class ValidateBuildGradleScriptsTask extends DefaultTask {

    private final Path repositoryRoot;
    private final File rootBuildFile;
    private final ProjectLayout projectLayout;
    private final ProblemReporter problemReporter;

    @Inject
    public ValidateBuildGradleScriptsTask(ProjectLayout projectLayout, Problems problems) {
        this.repositoryRoot = projectLayout.getSettingsDirectory().getAsFile().toPath();
        this.rootBuildFile = projectLayout.getSettingsDirectory().file("build.gradle").getAsFile();
        this.projectLayout = projectLayout;
        this.problemReporter = problems.getReporter();
        setDescription("Validates build.gradle scripts against repository guardrails");
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getScriptFiles();

    /**
     * Inline ignore entries configured by the root build, keyed by rule id.
     *
     * <p>Entries are either {@code path/to/build.gradle} to suppress all matches in that file, or
     * {@code path/to/build.gradle::N} to baseline exactly {@code N} matches and fail if the count grows or shrinks.
     */
    @Input
    public abstract MapProperty<String, List<String>> getIgnore();

    @OutputFile
    public File getOutputMarker() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "markers/" + getName());
    }

    @TaskAction
    public void validateScripts() throws IOException {
        List<ScriptRule> rules = buildRules();
        Set<IgnoreEntry> ignoredViolations = parseIgnoreEntries(getIgnore().getOrElse(java.util.Map.of()));
        Map<IgnoreKey, IgnoreEntry> ignoreEntriesByKey = indexIgnoreEntries(ignoredViolations);
        List<File> scriptFiles = getScriptFiles().getFiles().stream().sorted().toList();

        List<Problem> problems = new ArrayList<>();
        List<String> violations = new ArrayList<>();
        Map<IgnoreEntry, Integer> matchedIgnoredViolations = new LinkedHashMap<>();
        for (File scriptFile : scriptFiles) {
            if (scriptFile.isFile() == false) {
                continue;
            }
            String content = Files.readString(scriptFile.toPath(), StandardCharsets.UTF_8);
            validateFile(scriptFile, content, rules, ignoreEntriesByKey, matchedIgnoredViolations, problems, violations);
        }
        validateIgnoredViolations(ignoredViolations, matchedIgnoredViolations, problems, violations);

        if (problems.isEmpty() == false) {
            throw problemReporter.throwing(
                new GradleException(
                    "Found invalid build.gradle script validation results:"
                        + System.lineSeparator()
                        + String.join(System.lineSeparator(), violations)
                ),
                problems
            );
        }

        File marker = getOutputMarker();
        marker.getParentFile().mkdirs();
        Files.writeString(marker.toPath(), "done", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private List<ScriptRule> buildRules() {
        List<ScriptRule> rules = new ArrayList<>();
        addCrossProjectReferenceRules(rules);
        addDistributionBundledJdkRules(rules);
        addDistributionClosureAssignmentRules(rules);
        addDistributionExtClosureRules(rules);
        return rules;
    }

    /**
     * Adds the initial guardrails forbidding direct cross-project access from Groovy build scripts.
     */
    private void addCrossProjectReferenceRules(List<ScriptRule> rules) {
        rules.add(
            new ScriptRule(
                "cross-project-dereference",
                "Direct cross-project dereference in build.gradle",
                path -> true,
                Pattern.compile("project\\s*\\(\\s*['\"]:[^'\"]+['\"]\\s*\\)\\s*\\."),
                "Build scripts must not read another project's model directly via project(\":path\").* because that couples "
                    + "project configuration and breaks isolated-project-safe authoring.",
                "Replace the dereference with a dependency, consumable configuration, published artifact, or explicit task input."
            )
        );
        rules.add(
            new ScriptRule(
                "cross-project-configuration",
                "Cross-project configuration closure in build.gradle",
                path -> true,
                Pattern.compile("project\\s*\\(\\s*['\"]:[^'\"]+['\"]\\s*\\)\\s*\\{"),
                "Build scripts must not configure another project via project(\":path\") { ... } because configuration should live "
                    + "in the target project or a plugin applied there.",
                "Move the configuration into the target project or a shared plugin/convention applied by that project."
            )
        );
    }

    /**
     * Adds distribution-specific guardrails learned during the distribution build cleanup.
     */
    private void addDistributionBundledJdkRules(List<ScriptRule> rules) {
        rules.add(
            new ScriptRule(
                "distribution-bundled-jdk-source-of-truth",
                "Distribution scripts must use BundledJdkPlatforms for bundled-JDK platform availability",
                this::isDistributionBuildScript,
                Pattern.compile("\\bVersionProperties\\s*\\.\\s*(?:bundledJdkMajorVersion|getBundledJdkMajorVersion\\s*\\(\\s*\\))"),
                "Distribution build scripts must not derive bundled-JDK platform availability directly from "
                    + "VersionProperties.bundledJdkMajorVersion because that misses the pre-release runtime-JDK path "
                    + "and lets archive registration drift from bundled-JDK registration.",
                "Use BundledJdkPlatforms.bundledJdkMajor(buildParams) together with "
                    + "BundledJdkPlatforms.architecturesFor(...) or BundledJdkPlatforms.isSupported(...)."
            )
        );
    }

    /**
     * Adds a ratchet against growing the ad-hoc helper-closure surface in distribution scripts.
     */
    private void addDistributionClosureAssignmentRules(List<ScriptRule> rules) {
        rules.add(
            new ScriptRule(
                "distribution-closure-assignment",
                "Distribution scripts must not introduce helper closure assignments",
                this::isDistributionBuildScript,
                Pattern.compile("(?m)^[ \t]*(?:def\\s+)?[A-Za-z_][A-Za-z0-9_]*\\s*=\\s*\\{"),
                "Distribution build scripts should not define ad-hoc helper closures inline because that keeps "
                    + "build logic embedded in mutable Groovy scripts instead of tested plugin code and makes the "
                    + "script grow its own private API surface.",
                "Move the logic into a typed task, a plugin/helper class in build-tools-internal, or a regular method "
                    + "if the logic is purely local and temporary."
            )
        );
    }

    /**
     * Adds a ratchet against exporting script helper APIs via ext in distribution scripts.
     */
    private void addDistributionExtClosureRules(List<ScriptRule> rules) {
        rules.add(
            new ScriptRule(
                "distribution-ext-closure-api",
                "Distribution scripts must not export helper closures via ext",
                this::isDistributionBuildScript,
                Pattern.compile("(?m)^[ \t]*ext\\.[A-Za-z_][A-Za-z0-9_]*\\s*=\\s*\\{"),
                "Exporting helper closures from a distribution build script via ext creates a dynamic script API that is hard "
                    + "to discover, hard to refactor safely, and not covered by normal compiled-code checks.",
                "Move the shared behavior into a plugin, extension type, helper class, or typed task instead of exposing it via ext."
            )
        );
    }

    private void validateFile(
        File scriptFile,
        String content,
        List<ScriptRule> rules,
        Map<IgnoreKey, IgnoreEntry> ignoreEntriesByKey,
        Map<IgnoreEntry, Integer> matchedIgnoredViolations,
        List<Problem> problems,
        List<String> violations
    ) {
        String relativePath = relativize(scriptFile);
        for (ScriptRule rule : rules) {
            if (rule.appliesToPath().test(relativePath) == false) {
                continue;
            }
            Matcher matcher = rule.pattern().matcher(content);
            while (matcher.find()) {
                int lineNumber = lineNumber(content, matcher.start());
                String lineText = lineText(content, matcher.start()).trim();
                IgnoreEntry ignoreEntry = ignoreEntriesByKey.get(new IgnoreKey(rule.id(), relativePath));
                if (ignoreEntry != null && ignoreEntry.matchesConsumed(matchedIgnoredViolations.getOrDefault(ignoreEntry, 0)) == false) {
                    matchedIgnoredViolations.merge(ignoreEntry, 1, Integer::sum);
                    continue;
                }

                String label = "[" + rule.id() + "] " + relativePath + ":" + lineNumber;
                violations.add("- " + label + System.lineSeparator() + "  " + lineText);
                problems.add(
                    problemReporter.create(
                        ProblemId.create(rule.id(), rule.title(), ElasticsearchBuildProblems.BUILD_GRADLE_SCRIPTS),
                        spec -> spec.contextualLabel(label)
                            .details(rule.details())
                            .lineInFileLocation(scriptFile.getAbsolutePath(), lineNumber)
                            .solution(rule.solution())
                    )
                );
            }
        }
    }

    private void validateIgnoredViolations(
        Set<IgnoreEntry> ignoredViolations,
        Map<IgnoreEntry, Integer> matchedIgnoredViolations,
        List<Problem> problems,
        List<String> violations
    ) {
        for (IgnoreEntry ignoreEntry : ignoredViolations) {
            int matchedCount = matchedIgnoredViolations.getOrDefault(ignoreEntry, 0);
            if (ignoreEntry.isStale(matchedCount) == false) {
                continue;
            }
            String label = "[stale-ignore-entry] " + ignoreEntry.ruleId() + " -> " + ignoreEntry.displayValue();
            String details = ignoreEntry.expectedMatches() == null
                ? "The validateBuildGradleScripts ignore entry no longer matches any violation. Ignore entries must shrink as scripts are cleaned up."
                : String.format(
                    Locale.ROOT,
                    "The validateBuildGradleScripts counted ignore entry expected [%d] matches but found [%d]. Counted ignores must track the current baseline exactly.",
                    ignoreEntry.expectedMatches(),
                    matchedCount
                );
            violations.add("- " + label);
            problems.add(
                problemReporter.create(
                    ProblemId.create(
                        "stale-ignore-entry",
                        "Stale build.gradle validation ignore entry",
                        ElasticsearchBuildProblems.BUILD_GRADLE_SCRIPTS
                    ),
                    spec -> spec.contextualLabel(label)
                        .details(details)
                        .fileLocation(rootBuildFile.getAbsolutePath())
                        .solution(
                            "Remove or update the stale ignore entry from the validateBuildGradleScripts task configuration in the root build.gradle file."
                        )
                )
            );
        }
    }

    private Set<IgnoreEntry> parseIgnoreEntries(java.util.Map<String, List<String>> rawEntries) {
        Set<IgnoreEntry> entries = new LinkedHashSet<>();
        rawEntries.forEach((ruleId, paths) -> {
            String trimmedRuleId = ruleId.trim();
            if (trimmedRuleId.isEmpty()) {
                throw new GradleException("Ignore rule ids must not be blank");
            }
            for (String path : paths) {
                String trimmedPath = path.trim();
                if (trimmedPath.isEmpty()) {
                    throw new GradleException(String.format(Locale.ROOT, "Ignore path for rule [%s] must not be blank", trimmedRuleId));
                }
                entries.add(parseIgnoreEntry(trimmedRuleId, trimmedPath));
            }
        });
        return entries;
    }

    private IgnoreEntry parseIgnoreEntry(String ruleId, String rawValue) {
        int countSeparator = rawValue.lastIndexOf("::");
        if (countSeparator < 0) {
            return new IgnoreEntry(ruleId, rawValue, null);
        }

        String relativePath = rawValue.substring(0, countSeparator).trim();
        String rawCount = rawValue.substring(countSeparator + 2).trim();
        if (relativePath.isEmpty()) {
            throw new GradleException(String.format(Locale.ROOT, "Ignore path for rule [%s] must not be blank", ruleId));
        }
        if (rawCount.isEmpty()) {
            throw new GradleException(String.format(Locale.ROOT, "Ignore count for rule [%s] must not be blank", ruleId));
        }

        final int count;
        try {
            count = Integer.parseInt(rawCount);
        } catch (NumberFormatException e) {
            throw new GradleException(
                String.format(Locale.ROOT, "Ignore count for rule [%s] must be an integer: [%s]", ruleId, rawValue),
                e
            );
        }
        if (count < 0) {
            throw new GradleException(
                String.format(Locale.ROOT, "Ignore count for rule [%s] must be non-negative: [%s]", ruleId, rawValue)
            );
        }
        return new IgnoreEntry(ruleId, relativePath, count);
    }

    private static Map<IgnoreKey, IgnoreEntry> indexIgnoreEntries(Set<IgnoreEntry> ignoredViolations) {
        Map<IgnoreKey, IgnoreEntry> byKey = new LinkedHashMap<>();
        for (IgnoreEntry ignoreEntry : ignoredViolations) {
            byKey.put(new IgnoreKey(ignoreEntry.ruleId(), ignoreEntry.relativePath()), ignoreEntry);
        }
        return byKey;
    }

    private String relativize(File file) {
        return repositoryRoot.relativize(file.toPath()).toString().replace(File.separatorChar, '/');
    }

    private static int lineNumber(String content, int offset) {
        int lineNumber = 1;
        for (int i = 0; i < offset; i++) {
            if (content.charAt(i) == '\n') {
                lineNumber++;
            }
        }
        return lineNumber;
    }

    private boolean isDistributionBuildScript(String relativePath) {
        return relativePath.startsWith("distribution/");
    }

    private static String lineText(String content, int offset) {
        int lineStart = content.lastIndexOf('\n', offset);
        int lineEnd = content.indexOf('\n', offset);
        int start = lineStart == -1 ? 0 : lineStart + 1;
        int end = lineEnd == -1 ? content.length() : lineEnd;
        return content.substring(start, end);
    }

    private record ScriptRule(String id, String title, Predicate<String> appliesToPath, Pattern pattern, String details, String solution) {}

    private record IgnoreKey(String ruleId, String relativePath) {}

    private record IgnoreEntry(String ruleId, String relativePath, Integer expectedMatches) {
        boolean matchesConsumed(int matchedCount) {
            return expectedMatches != null && matchedCount >= expectedMatches;
        }

        boolean isStale(int matchedCount) {
            return expectedMatches == null ? matchedCount == 0 : matchedCount != expectedMatches;
        }

        String displayValue() {
            return expectedMatches == null ? relativePath : relativePath + "::" + expectedMatches;
        }
    }
}
