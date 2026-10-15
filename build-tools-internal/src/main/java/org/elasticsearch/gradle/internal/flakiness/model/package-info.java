/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Shared data contracts for the flakiness-detection pipeline.
 *
 * <pre>
 * TypeScript bootstrap                         per-project resolve                   repo-wide scan / emit
 * -------------------                         -------------------                   ----------------------
 * flakiness-refs.json
 *        |
 *        v
 * {@link org.elasticsearch.gradle.internal.flakiness.model.FlakinessRef}
 *
 * Gradle project model
 *        |
 *        +--> {@link org.elasticsearch.gradle.internal.flakiness.model.SourceSetInfo}
 *        |          (where sources/classes live)
 *        |
 *        +--> {@link org.elasticsearch.gradle.internal.flakiness.model.TestTaskInfo}
 *                   (which Test tasks really run them)
 *                        |
 *                        v
 *                 resolve project task
 *                        |
 *                        +--> {@link org.elasticsearch.gradle.internal.flakiness.model.BaseTarget}
 *                        |        ref resolved to a project/sourceSet/kind target
 *                        |
 *                        +--> {@link org.elasticsearch.gradle.internal.flakiness.model.SourceSetDisposition}
 *                                 source-set output mapped to runnable tasks
 *
 * all project resolve JSONs + compiled bytecode
 *                        |
 *                        v
 *                 {@link org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan}
 *                        |
 *                        +--> PlanEntry      concrete runnable/skip targets
 *                        +--> Expansion      abstract base -> concrete subclasses report
 *                        +--> TaskSelection  capped task fan-out report
 *                        +--> Unresolved     refs that could not be mapped
 *                        |
 *                        v
 *                 {@link org.elasticsearch.gradle.internal.flakiness.model.PlanCommand}
 *                 ready batch commands for the TypeScript runner
 *
 * {@link org.elasticsearch.gradle.internal.flakiness.model.Kinds} supplies the shared wire vocabulary used
 * across these records: source-set names, target kinds, dispositions, Buildkite step keys, labels and caps.
 * </pre>
 *
 * <p>Relations:
 * <ul>
 *   <li>{@link org.elasticsearch.gradle.internal.flakiness.model.SourceSetInfo} and
 *       {@link org.elasticsearch.gradle.internal.flakiness.model.TestTaskInfo} are snapshots of a single
 *       project's configured Gradle model. They are Java-side inputs to resolution, not the final wire
 *       contract consumed by TypeScript.</li>
 *   <li>{@link org.elasticsearch.gradle.internal.flakiness.model.BaseTarget} is the first successful answer
 *       to a {@link org.elasticsearch.gradle.internal.flakiness.model.FlakinessRef}: it says which
 *       project/source set/kind the ref resolved to, and which task paths can rerun it.</li>
 *   <li>{@link org.elasticsearch.gradle.internal.flakiness.model.SourceSetDisposition} complements
 *       {@link org.elasticsearch.gradle.internal.flakiness.model.BaseTarget} during bytecode enrichment: when
 *       an abstract base expands to concrete subclasses in a different output directory, the scan phase uses
 *       the matching disposition to find the correct runnable tasks for that subclass.</li>
 *   <li>{@link org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan} is the repo-wide merged view.
 *       Its nested records distinguish executable entries, reporting-only expansion/task-selection metadata,
 *       and unresolved refs that must stay visible instead of being silently dropped.</li>
 *   <li>{@link org.elasticsearch.gradle.internal.flakiness.model.PlanCommand} is derived from
 *       {@link org.elasticsearch.gradle.internal.flakiness.model.FlakinessPlan} entries and gives the thin
 *       runner layer ready-to-execute batch commands without having to reverse-engineer task paths.</li>
 * </ul>
 *
 * <p>{@link org.elasticsearch.gradle.internal.flakiness.FlakinessJson} owns serialization of these records.
 * For the end-to-end workflow, see {@code .buildkite/scripts/flakiness-detection/README.md}.
 */
package org.elasticsearch.gradle.internal.flakiness.model;
