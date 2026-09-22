/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import org.gradle.tooling.events.problems.Problem;
import org.gradle.tooling.events.problems.ProblemDefinition;
import org.gradle.tooling.events.problems.ProblemGroup;
import org.gradle.tooling.events.problems.ProblemId;
import org.gradle.tooling.events.problems.ProblemSummariesEvent;
import org.gradle.tooling.events.problems.ProblemSummary;
import org.gradle.tooling.events.problems.Severity;
import org.gradle.tooling.events.problems.SingleProblemEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProblemsTrackerTests {

    @Test
    void countsSingleProblemsAndSummaries() {
        ProblemsTracker tracker = new ProblemsTracker();
        ProblemGroup validation = problemGroup("validation", null);
        ProblemGroup configurationCache = problemGroup("configuration-cache", validation);

        ProblemId repeatedProblemId = problemId(
            "cannot-access-another-project",
            "Cannot access another project",
            configurationCache
        );
        tracker.statusChanged(singleProblemEvent(problem(repeatedProblemId, Severity.ERROR)));
        tracker.statusChanged(problemSummariesEvent(problemSummary(repeatedProblemId, 4)));

        ProblemId warningProblemId = problemId("deprecated-thing", "Deprecated thing", validation);
        tracker.statusChanged(singleProblemEvent(problem(warningProblemId, Severity.WARNING)));

        ProblemsReport report = tracker.buildReport();
        assertEquals(6, report.totalProblems());

        Map<String, Integer> severities = report.severities()
            .stream()
            .collect(Collectors.toMap(ProblemsReport.SeverityEntry::severity, ProblemsReport.SeverityEntry::count));
        assertEquals(Map.of("ERROR", 5, "WARNING", 1), severities);

        assertEquals(2, report.problems().size());
        assertEquals("validation:configuration-cache:cannot-access-another-project", report.problems().get(0).id());
        assertEquals(5, report.problems().get(0).count());
        assertEquals("ERROR", report.problems().get(0).severity());
        assertEquals("validation:deprecated-thing", report.problems().get(1).id());
        assertEquals(1, report.problems().get(1).count());
        assertEquals("WARNING", report.problems().get(1).severity());
    }

    @Test
    void usesUnknownSeverityWhenOnlySummaryWasObserved() {
        ProblemsTracker tracker = new ProblemsTracker();
        ProblemId problemId = problemId("summary-only", "Summary only", problemGroup("validation", null));

        tracker.statusChanged(problemSummariesEvent(problemSummary(problemId, 3)));

        ProblemsReport report = tracker.buildReport();
        assertEquals(3, report.totalProblems());
        assertEquals("UNKNOWN", report.problems().get(0).severity());
        assertEquals("UNKNOWN", report.severities().get(0).severity());
        assertEquals(3, report.severities().get(0).count());
    }

    private static SingleProblemEvent singleProblemEvent(Problem problem) {
        return proxy(SingleProblemEvent.class, name -> switch (name) {
            case "getProblem" -> problem;
            default -> unsupported(name);
        });
    }

    private static ProblemSummariesEvent problemSummariesEvent(ProblemSummary... summaries) {
        return proxy(ProblemSummariesEvent.class, name -> switch (name) {
            case "getProblemSummaries" -> List.of(summaries);
            default -> unsupported(name);
        });
    }

    private static ProblemSummary problemSummary(ProblemId problemId, int count) {
        return proxy(ProblemSummary.class, name -> switch (name) {
            case "getProblemId" -> problemId;
            case "getCount" -> count;
            default -> unsupported(name);
        });
    }

    private static Problem problem(ProblemId problemId, Severity severity) {
        ProblemDefinition definition = proxy(ProblemDefinition.class, name -> switch (name) {
            case "getId" -> problemId;
            case "getSeverity" -> severity;
            case "getDocumentationLink" -> null;
            default -> unsupported(name);
        });
        return proxy(Problem.class, name -> switch (name) {
            case "getDefinition" -> definition;
            case "getContextualLabel", "getDetails", "getFailure", "getAdditionalData" -> null;
            case "getOriginLocations", "getContextualLocations", "getSolutions" -> List.of();
            default -> unsupported(name);
        });
    }

    private static ProblemId problemId(String name, String displayName, ProblemGroup group) {
        return proxy(ProblemId.class, methodName -> switch (methodName) {
            case "getName" -> name;
            case "getDisplayName" -> displayName;
            case "getGroup" -> group;
            default -> unsupported(methodName);
        });
    }

    private static ProblemGroup problemGroup(String name, ProblemGroup parent) {
        return proxy(ProblemGroup.class, methodName -> switch (methodName) {
            case "getName", "getDisplayName" -> name;
            case "getParent" -> parent;
            default -> unsupported(methodName);
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Function<String, Object> handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, args) -> switch (method.getName()) {
            case "equals" -> proxy == args[0];
            case "hashCode" -> System.identityHashCode(proxy);
            case "toString" -> type.getSimpleName();
            default -> handler.apply(method.getName());
        });
    }

    private static Object unsupported(String methodName) {
        throw new UnsupportedOperationException(methodName);
    }
}
