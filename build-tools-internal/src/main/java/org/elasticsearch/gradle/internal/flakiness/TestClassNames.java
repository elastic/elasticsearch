/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.util.List;

/**
 * Whether a fully-qualified class name is something a {@code Test} task could actually run.
 *
 * <h2>Why this is needed</h2>
 * A test source set contains more than tests: abstract bases, mock plugins, fixture servers, parsers. Nothing
 * upstream of the plan distinguishes them, so without this filter two things go wrong, and both end as a
 * <em>silent zero-test run</em> that {@code deriveOutcome} scores as {@code hang} - the exact failure mode
 * this whole feature exists to remove:
 * <ul>
 *   <li>a changed non-test file (say {@code TestUtils.java} under {@code src/test/java}) resolves to a target
 *       and emits {@code --tests TestUtils}, which matches nothing;</li>
 *   <li>expanding an abstract <em>helper</em> yields its inner/anonymous subclasses - concrete in bytecode,
 *       never tests - which emit {@code --tests Foo$1}.</li>
 * </ul>
 * The TypeScript this resolver replaced had the filter baked into its path regexes
 * ({@code /^(.+)\/src\/test\/java\/(.+Tests)\.java$/} and friends), so restoring it is closing a regression
 * rather than adding a new rule.
 *
 * <h2>Where the suffixes come from</h2>
 * {@code TestingConventionsPrecommitPlugin} is the authoritative convention and enforces per source set:
 * {@code Tests} for {@code test}, {@code IT} (or {@code Tests}) for {@code internalClusterTest}, {@code IT}
 * for {@code javaRestTest}/{@code yamlRestTest}. This check deliberately accepts the <b>union</b> rather than
 * the per-source-set suffix, and additionally accepts {@code TestCase}:
 * <ul>
 *   <li>the union avoids rejecting a class that is legitimately named for a different source set's
 *       convention;</li>
 *   <li>{@code TestCase} is normally an abstract base, but a handful of <em>concrete</em> runnable tests use
 *       it - {@code RollingUpgradeLuceneIndexCompatibilityTestCase} is concrete with three test methods.
 *       Abstract classes never reach this check (they are expanded, not run), so accepting the suffix costs
 *       nothing and rejecting it would silently drop a real test.</li>
 * </ul>
 * Audited over the whole repo's compiled output: of the concrete descendants of the 450 top-level abstract
 * bases, this rejects 7 classes and every one is a helper ({@code WebProxyServer}, {@code Otlp*Parser},
 * {@code *PauseFieldPlugin}, {@code LocalStateSecurity}), with no test among them.
 *
 * <p>A rejection is always <em>reported</em>, never silently dropped - see
 * {@link PlanBuilder#REASON_NOT_A_TEST_CLASS}. That keeps a mis-named real test visible instead of turning it
 * into a missing check.
 */
public final class TestClassNames {

    /** Accepted simple-name suffixes; see the class javadoc for why this is the union plus {@code TestCase}. */
    private static final List<String> TEST_SUFFIXES = List.of("Tests", "IT", "TestCase");

    private TestClassNames() {}

    /**
     * Whether {@code fqcn} names a class a {@code Test} task could run.
     *
     * <p>Inner and anonymous classes are rejected outright: Gradle's {@code --tests} filter addresses
     * top-level classes, and a {@code $}-bearing name only ever reaches the plan by way of bytecode-level
     * subclass expansion, never from a source file or a {@code muted-tests.yml} entry.
     */
    public static boolean isRunnableTestClass(String fqcn) {
        if (fqcn == null || fqcn.isBlank() || fqcn.indexOf('$') >= 0) {
            return false;
        }
        String simple = fqcn.substring(fqcn.lastIndexOf('.') + 1);
        for (String suffix : TEST_SUFFIXES) {
            if (simple.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }
}
