/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.problems;

import org.gradle.api.Action;
import org.gradle.api.problems.Problem;
import org.gradle.api.problems.ProblemId;
import org.gradle.api.problems.ProblemReporter;
import org.gradle.api.problems.ProblemSpec;

import java.lang.reflect.Method;
import java.util.Collection;

/**
 * Thin shim around {@link ProblemReporter} that routes {@code ERROR}-severity problems through
 * {@code ProblemReporterInternal.reportError(...)} on Gradle 9.6+. As of Gradle 9.6 the public
 * {@link ProblemReporter#report(Problem) report(...)} entry point always renders diagnostics
 * as {@code WARNING} in {@code build/reports/problems/problems-report.html} regardless of
 * {@code spec.severity(Severity.ERROR)} on the producing {@code ProblemSpec}; a new
 * {@code reportError(...)} family on the internal {@code ProblemReporterInternal} interface is
 * required to surface ERROR severity in the report.
 *
 * <p>On older Gradle versions (no {@code reportError} method) this shim falls back to
 * {@link ProblemReporter#report report(...)}, which there still honored
 * {@code spec.severity(Severity.ERROR)}.
 *
 * <p>The reflection here is intentional — {@code reportError} currently lives only on the
 * internal {@code org.gradle.api.problems.internal.ProblemReporterInternal} interface, which
 * is not on the public compile classpath. Remove the reflection once {@code reportError} is
 * promoted to the public {@link ProblemReporter} interface.
 */
public final class ProblemReporting {

    private static final Method REPORT_ERROR_COLLECTION = lookupReportError(Collection.class);
    private static final Method REPORT_ERROR_SINGLE = lookupReportError(Problem.class);

    private ProblemReporting() {}

    /**
     * Report an ERROR-severity problem built from a {@link ProblemId} and a spec configurer,
     * using {@code reportError} on Gradle 9.6+ and falling back to {@code report} otherwise.
     */
    public static void reportError(ProblemReporter reporter, ProblemId id, Action<? super ProblemSpec> spec) {
        reportError(reporter, reporter.create(id, spec));
    }

    /**
     * Report a single ERROR-severity problem, using {@code reportError} on Gradle 9.6+
     * and falling back to {@code report} otherwise.
     */
    public static void reportError(ProblemReporter reporter, Problem problem) {
        if (REPORT_ERROR_SINGLE != null) {
            try {
                REPORT_ERROR_SINGLE.invoke(reporter, problem);
                return;
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to invoke ProblemReporterInternal.reportError(Problem)", e);
            }
        }
        reporter.report(problem);
    }

    /**
     * Report a collection of ERROR-severity problems, using {@code reportError} on Gradle 9.6+
     * and falling back to {@code report} otherwise. No-op when the collection is empty.
     */
    public static void reportErrors(ProblemReporter reporter, Collection<? extends Problem> problems) {
        if (problems.isEmpty()) {
            return;
        }
        if (REPORT_ERROR_COLLECTION != null) {
            try {
                REPORT_ERROR_COLLECTION.invoke(reporter, problems);
                return;
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed to invoke ProblemReporterInternal.reportError(Collection)", e);
            }
        }
        reporter.report(problems);
    }

    private static Method lookupReportError(Class<?> argType) {
        try {
            Class<?> internal = Class.forName("org.gradle.api.problems.internal.ProblemReporterInternal");
            Method m = internal.getMethod("reportError", argType);
            m.setAccessible(true);
            return m;
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            return null;
        }
    }
}
