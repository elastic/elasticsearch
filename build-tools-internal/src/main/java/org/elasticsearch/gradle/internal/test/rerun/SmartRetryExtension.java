/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun;

import org.gradle.api.provider.Property;

/**
 * Per-project configuration for the smart retry mechanism implemented by {@link InternalTestRerunPlugin}.
 * <p>
 * Registered on every project as {@code smartRetry}.
 */
public abstract class SmartRetryExtension {

    /**
     * Whether individual test methods that passed in a previous build attempt may be excluded when the task is retried.
     * Defaults to {@code true}.
     * <p>
     * Projects whose suites depend on execution order must set this to {@code false}. The canonical example is an upgrade
     * suite parameterized over upgrade phases: an earlier phase both asserts behaviour and establishes the cluster state a
     * later phase relies on, so a phase that passed must still run when a later phase is retried. Disabling this leaves
     * task-level and suite-level pruning in place, since those either skip a suite in its entirety or not at all.
     */
    public abstract Property<Boolean> getPruneIndividualTests();
}
