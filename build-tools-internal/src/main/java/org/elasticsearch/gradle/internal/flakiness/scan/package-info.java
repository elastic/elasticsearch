/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Builds a repo-wide flakiness plan from the per-project resolve outputs and compiled bytecode.
 * {@link org.elasticsearch.gradle.internal.flakiness.scan.FlakinessTargets} restores ref ordering and decides
 * which refs no project claimed; {@link org.elasticsearch.gradle.internal.flakiness.scan.ClassHierarchyScanner}
 * finds concrete descendants of abstract targets across project boundaries.
 * {@link org.elasticsearch.gradle.internal.flakiness.scan.PlanBuilder} joins each descendant's bytecode origin
 * directory to the source-set disposition reported by its owning project, so it uses that source set's real
 * tasks rather than the abstract base's tasks.
 * {@link org.elasticsearch.gradle.internal.flakiness.scan.CommandBuilder} attaches runnable batch commands.
 * For the end-to-end pipeline, see {@code .buildkite/scripts/flakiness-detection/README.md}.
 */
package org.elasticsearch.gradle.internal.flakiness.scan;
