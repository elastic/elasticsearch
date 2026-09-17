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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
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
     * Inline baseline entries configured by the root build, keyed by rule id.
     */
    @Input
    public abstract MapProperty<String, List<String>> getBaseline();

    @OutputFile
    public File getOutputMarker() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "markers/" + getName());
    }

    @TaskAction
    public void validateScripts() throws IOException {
        List<ScriptRule> rules = buildRules();
        Set<BaselineEntry> baselineEntries = parseBaselineEntries(getBaseline().getOrElse(java.util.Map.of()));
        List<File> scriptFiles = getScriptFiles().getFiles().stream().sorted().toList();

        List<Problem> problems = new ArrayList<>();
        List<String> violations = new ArrayList<>();
        Set<BaselineEntry> matchedBaselineEntries = new LinkedHashSet<>();
        for (File scriptFile : scriptFiles) {
            if (scriptFile.isFile() == false) {
                continue;
            }
            String content = Files.readString(scriptFile.toPath(), StandardCharsets.UTF_8);
            validateFile(scriptFile, content, rules, baselineEntries, matchedBaselineEntries, problems, violations);
        }
        validateBaselineEntries(baselineEntries, matchedBaselineEntries, problems, violations);

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
                Pattern.compile("project\\s*\\(\\s*['\"]:[^'\"]+['\"]\\s*\\)\\s*\\{"),
                "Build scripts must not configure another project via project(\":path\") { ... } because configuration should live "
                    + "in the target project or a plugin applied there.",
                "Move the configuration into the target project or a shared plugin/convention applied by that project."
            )
        );
    }

    private void validateFile(
        File scriptFile,
        String content,
        List<ScriptRule> rules,
        Set<BaselineEntry> baselineEntries,
        Set<BaselineEntry> matchedBaselineEntries,
        List<Problem> problems,
        List<String> violations
    ) {
        String relativePath = relativize(scriptFile);
        for (ScriptRule rule : rules) {
            Matcher matcher = rule.pattern().matcher(content);
            while (matcher.find()) {
                int lineNumber = lineNumber(content, matcher.start());
                String lineText = lineText(content, matcher.start()).trim();
                BaselineEntry baselineEntry = new BaselineEntry(rule.id(), relativePath);
                if (baselineEntries.contains(baselineEntry)) {
                    matchedBaselineEntries.add(baselineEntry);
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

    private void validateBaselineEntries(
        Set<BaselineEntry> baselineEntries,
        Set<BaselineEntry> matchedBaselineEntries,
        List<Problem> problems,
        List<String> violations
    ) {
        Set<BaselineEntry> staleEntries = new LinkedHashSet<>(baselineEntries);
        staleEntries.removeAll(matchedBaselineEntries);
        for (BaselineEntry staleEntry : staleEntries) {
            String label = "[stale-baseline-entry] " + staleEntry.ruleId() + " -> " + staleEntry.relativePath();
            violations.add("- " + label);
            problems.add(
                problemReporter.create(
                    ProblemId.create(
                        "stale-baseline-entry",
                        "Stale build.gradle validation baseline entry",
                        ElasticsearchBuildProblems.BUILD_GRADLE_SCRIPTS
                    ),
                    spec -> spec.contextualLabel(label)
                        .details(
                            "The validateBuildGradleScripts baseline entry no longer matches any violation. Baselines must shrink as scripts are cleaned up."
                        )
                        .fileLocation(rootBuildFile.getAbsolutePath())
                        .solution(
                            "Remove the stale baseline entry from the validateBuildGradleScripts task configuration in the root build.gradle file."
                        )
                )
            );
        }
    }

    private Set<BaselineEntry> parseBaselineEntries(java.util.Map<String, List<String>> rawEntries) {
        Set<BaselineEntry> entries = new LinkedHashSet<>();
        rawEntries.forEach((ruleId, paths) -> {
            String trimmedRuleId = ruleId.trim();
            if (trimmedRuleId.isEmpty()) {
                throw new GradleException("Baseline rule ids must not be blank");
            }
            for (String path : paths) {
                String trimmedPath = path.trim();
                if (trimmedPath.isEmpty()) {
                    throw new GradleException(String.format(Locale.ROOT, "Baseline path for rule [%s] must not be blank", trimmedRuleId));
                }
                entries.add(new BaselineEntry(trimmedRuleId, trimmedPath));
            }
        });
        return entries;
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

    private static String lineText(String content, int offset) {
        int lineStart = content.lastIndexOf('\n', offset);
        int lineEnd = content.indexOf('\n', offset);
        int start = lineStart == -1 ? 0 : lineStart + 1;
        int end = lineEnd == -1 ? content.length() : lineEnd;
        return content.substring(start, end);
    }

    private record ScriptRule(String id, String title, Pattern pattern, String details, String solution) {}

    private record BaselineEntry(String ruleId, String relativePath) {}
}
