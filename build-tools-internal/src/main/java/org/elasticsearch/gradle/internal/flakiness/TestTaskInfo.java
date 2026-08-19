/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import java.nio.file.Path;
import java.util.List;

/**
 * A Gradle-free snapshot of one {@code Test} task of one project, taken <em>after that project has finished
 * configuring</em> (see {@link FlakinessProjectModel#testTaskSnapshot}).
 *
 * <p>These three facts are what let the resolver answer "which task actually runs this test class?" without
 * any per-project convention knowledge: a {@code Test} task runs the classes in its {@code testClassesDirs},
 * and it runs them only if it is {@code enabled}. Several ES conventions disable the conventional bare task
 * ({@code test} / {@code javaRestTest}) and point differently-named {@code Test} tasks at the <em>same</em>
 * source-set output - {@code elasticsearch.bwc-test} ({@code v&lt;version&gt;#bwcTest}) and
 * {@code elasticsearch.distro-test} ({@code destructiveDistroTest.&lt;distro&gt;}) both do exactly that - so the
 * bare task name is not a safe assumption. See {@link TestTaskSelector}.
 *
 * <p><b>Post-configuration values are mandatory.</b> {@code enabled} and {@code testClassesDirs} are mutated
 * by convention plugins and build scripts that may run after the flakiness model hook is installed, so
 * snapshotting them from inside a {@code configureEach} callback would capture pre-mutation values. They are
 * therefore read late - at configuration-cache store time, after the whole configuration phase has run (see
 * {@link FlakinessProjectModel#testTaskSnapshot}).
 *
 * @param name             the task name, e.g. {@code test}, {@code v9.6.0#bwcTest}
 * @param taskPath         the fully qualified task path, e.g. {@code :libs:dissect:test}
 * @param enabled          the task's {@code enabled} flag, read post-configuration; a disabled task is
 *                         reported {@code SKIPPED} by Gradle and runs zero tests
 * @param testClassesDirs  the compiled-class directories this task actually runs tests from
 */
public record TestTaskInfo(String name, String taskPath, boolean enabled, List<Path> testClassesDirs) {}
