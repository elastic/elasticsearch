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
 */
public class ProblemsTracker implements ProgressListener {

    private static final String UNKNOWN_SEVERITY = "UNKNOWN";

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
            problems.add(new ProblemsReport.ProblemEntry(entry.id, entry.displayName, entry.severity, entry.count));
        }

        problems.sort(Comparator.comparingInt(ProblemsReport.ProblemEntry::count).reversed().thenComparing(ProblemsReport.ProblemEntry::id));
        List<ProblemsReport.SeverityEntry> severities = severityCounts.entrySet()
            .stream()
            .map(entry -> new ProblemsReport.SeverityEntry(entry.getKey(), entry.getValue()))
            .sorted(Comparator.comparingInt(ProblemsReport.SeverityEntry::count).reversed().thenComparing(ProblemsReport.SeverityEntry::severity))
            .toList();
        return new ProblemsReport(totalProblems, severities, problems);
    }

    private void recordProblem(Problem problem) {
        ProblemDefinition definition = problem.getDefinition();
        ProblemId problemId = definition.getId();
        increment(problemId, severityName(definition.getSeverity()), 1);
    }

    private void recordSummary(ProblemSummary summary) {
        increment(summary.getProblemId(), null, summary.getCount());
    }

    private void increment(ProblemId problemId, String severity, int delta) {
        String id = fullyQualifiedName(problemId);
        MutableProblemEntry entry = problemsById.get(id);
        if (entry == null) {
            entry = new MutableProblemEntry(id, problemId.getDisplayName(), severity != null ? severity : UNKNOWN_SEVERITY);
            problemsById.put(id, entry);
        } else if (UNKNOWN_SEVERITY.equals(entry.severity) && severity != null) {
            entry.severity = severity;
        }
        entry.count += delta;
        totalProblems += delta;
    }

    private static String severityName(Severity severity) {
        if (severity == Severity.ERROR) {
            return "ERROR";
        }
        if (severity == Severity.WARNING) {
            return "WARNING";
        }
        if (severity == Severity.ADVICE) {
            return "ADVICE";
        }
        return severity.isKnown() ? Integer.toString(severity.getSeverity()) : UNKNOWN_SEVERITY;
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
        private String severity;
        private int count;

        private MutableProblemEntry(String id, String displayName, String severity) {
            this.id = id;
            this.displayName = displayName;
            this.severity = severity;
        }
    }
}
