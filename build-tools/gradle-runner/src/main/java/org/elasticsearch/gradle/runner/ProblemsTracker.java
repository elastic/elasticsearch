/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import org.gradle.tooling.events.ProgressEvent;
import org.gradle.tooling.events.ProgressListener;
import org.gradle.tooling.events.problems.ContextualLabel;
import org.gradle.tooling.events.problems.Problem;
import org.gradle.tooling.events.problems.ProblemDefinition;
import org.gradle.tooling.events.problems.ProblemGroup;
import org.gradle.tooling.events.problems.ProblemId;
import org.gradle.tooling.events.problems.ProblemSummariesEvent;
import org.gradle.tooling.events.problems.ProblemSummary;
import org.gradle.tooling.events.problems.Severity;
import org.gradle.tooling.events.problems.SingleProblemEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Collects Gradle Problems API progress events emitted by the Tooling API.
 *
 * <p>Gradle emits individual {@link SingleProblemEvent}s up to an internal per-problem threshold,
 * then reports the remaining duplicate count via {@link ProblemSummariesEvent}. Counting both gives
 * the full number of reported problems without parsing the generated HTML report.
 *
 * <p>The tracker stores only aggregate counts per fully-qualified problem id, so memory usage stays
 * bounded by the number of distinct problem types rather than the total number of reported events.
 *
 * <p>Since Gradle 9.9, problem ids name the <em>kind</em> of problem (for example {@code Gradle:Deprecation:Task usage})
 * and the specifics (which task, which method, ...) are only carried by the contextual label of each problem. To keep
 * the report actionable, the tracker also counts contextual labels per problem id, capped at
 * {@link #MAX_LABELS_PER_PROBLEM} distinct labels per id. Problems that Gradle only reports via
 * {@link ProblemSummariesEvent}s carry no label and are only reflected in the per-id count.
 */
public class ProblemsTracker implements ProgressListener {

    private static final String UNKNOWN_SEVERITY = "UNKNOWN";
    static final int MAX_LABELS_PER_PROBLEM = 20;

    private final Map<String, MutableProblemEntry> problemsById = new LinkedHashMap<>();
    private int totalProblems;

    @Override
    public synchronized void statusChanged(ProgressEvent event) {
        if (event instanceof SingleProblemEvent singleProblemEvent) {
            recordProblem(singleProblemEvent.getProblem());
        } else if (event instanceof ProblemSummariesEvent summariesEvent) {
            for (ProblemSummary summary : summariesEvent.getProblemSummaries()) {
                recordSummary(summary);
            }
        }
    }

    /**
     * Builds an immutable summary report ordered for stable output.
     */
    public synchronized ProblemsReport buildReport() {
        Map<String, Integer> severityCounts = new TreeMap<>();
        List<ProblemsReport.ProblemEntry> problems = new ArrayList<>(problemsById.size());
        for (MutableProblemEntry entry : problemsById.values()) {
            severityCounts.merge(entry.severity, entry.count, Integer::sum);
            List<ProblemsReport.LabelEntry> labels = entry.labelCounts.entrySet()
                .stream()
                .map(label -> new ProblemsReport.LabelEntry(label.getKey(), label.getValue()))
                .sorted(
                    Comparator.comparingInt(ProblemsReport.LabelEntry::count).reversed().thenComparing(ProblemsReport.LabelEntry::label)
                )
                .toList();
            problems.add(new ProblemsReport.ProblemEntry(entry.id, entry.displayName, entry.severity, entry.count, labels));
        }

        problems.sort(
            Comparator.comparingInt(ProblemsReport.ProblemEntry::count).reversed().thenComparing(ProblemsReport.ProblemEntry::id)
        );
        List<ProblemsReport.SeverityEntry> severities = severityCounts.entrySet()
            .stream()
            .map(entry -> new ProblemsReport.SeverityEntry(entry.getKey(), entry.getValue()))
            .sorted(
                Comparator.comparingInt(ProblemsReport.SeverityEntry::count)
                    .reversed()
                    .thenComparing(ProblemsReport.SeverityEntry::severity)
            )
            .toList();
        return new ProblemsReport(totalProblems, severities, problems);
    }

    private void recordProblem(Problem problem) {
        ProblemDefinition definition = problem.getDefinition();
        ProblemId problemId = definition.getId();
        MutableProblemEntry entry = increment(problemId, severityName(definition.getSeverity()), 1);
        String label = contextualLabel(problem);
        if (label != null) {
            entry.recordLabel(label);
        }
    }

    private void recordSummary(ProblemSummary summary) {
        increment(summary.getProblemId(), null, summary.getCount());
    }

    private static String contextualLabel(Problem problem) {
        ContextualLabel contextualLabel = problem.getContextualLabel();
        if (contextualLabel == null) {
            return null;
        }
        String label = contextualLabel.getContextualLabel();
        return label == null || label.isBlank() ? null : label;
    }

    private MutableProblemEntry increment(ProblemId problemId, String severity, int delta) {
        String id = fullyQualifiedName(problemId);
        MutableProblemEntry entry = problemsById.get(id);
        if (entry == null) {
            // Since Gradle 9.9 the name of a problem id is its display name; ProblemId.getDisplayName() is deprecated.
            entry = new MutableProblemEntry(id, problemId.getName(), severity != null ? severity : UNKNOWN_SEVERITY);
            problemsById.put(id, entry);
        } else if (UNKNOWN_SEVERITY.equals(entry.severity) && severity != null) {
            entry.severity = severity;
        }
        entry.count += delta;
        totalProblems += delta;
        return entry;
    }

    private static String severityName(Severity severity) {
        if (severity.isKnown() == false) {
            return UNKNOWN_SEVERITY;
        }

        int severityValue = severity.getSeverity();
        if (severityValue == Severity.ERROR.getSeverity()) {
            return "ERROR";
        }
        if (severityValue == Severity.WARNING.getSeverity()) {
            return "WARNING";
        }
        if (severityValue == Severity.ADVICE.getSeverity()) {
            return "ADVICE";
        }
        return Integer.toString(severityValue);
    }

    private static String fullyQualifiedName(ProblemId problemId) {
        List<String> names = new ArrayList<>();
        ProblemGroup group = problemId.getGroup();
        while (group != null) {
            names.add(0, group.getName());
            group = group.getParent();
        }
        names.add(problemId.getName());
        return String.join(":", names);
    }

    private static final class MutableProblemEntry {
        private final String id;
        private final String displayName;
        private final Map<String, Integer> labelCounts = new LinkedHashMap<>();
        private String severity;
        private int count;

        private MutableProblemEntry(String id, String displayName, String severity) {
            this.id = id;
            this.displayName = displayName;
            this.severity = severity;
        }

        private void recordLabel(String label) {
            if (labelCounts.containsKey(label) || labelCounts.size() < MAX_LABELS_PER_PROBLEM) {
                labelCounts.merge(label, 1, Integer::sum);
            }
        }
    }
}
