/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Shared data contracts for the per-project resolve and repo-wide scan phases.
 * {@link org.elasticsearch.gradle.internal.flakiness.model.SourceSetInfo} and
 * {@link org.elasticsearch.gradle.internal.flakiness.model.TestTaskInfo} snapshot a project's Gradle model
 * for resolution; {@link org.elasticsearch.gradle.internal.flakiness.model.BaseTarget} and
 * {@link org.elasticsearch.gradle.internal.flakiness.model.SourceSetDisposition} carry the per-project answer
 * into scanning. {@link org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan} and
 * {@link org.elasticsearch.gradle.internal.flakiness.model.PlanCommand} describe the output consumed by the
 * TypeScript runner. The project snapshots are Java-side data, not TypeScript wire types.
 * {@link org.elasticsearch.gradle.internal.flakiness.FlakinessJson} owns serialization of these records.
 * For the end-to-end workflow, see {@code .buildkite/scripts/flakiness-detection/README.md}.
 */
package org.elasticsearch.gradle.internal.flakiness.model;
