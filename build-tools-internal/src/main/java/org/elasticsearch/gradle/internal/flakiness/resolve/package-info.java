/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Resolves flakiness refs against each project's own source sets and {@code Test} tasks.
 * {@link org.elasticsearch.gradle.internal.flakiness.resolve.FlakinessProjectResolvePlugin} registers a
 * per-project task; each project claims only refs it owns and writes its answer under
 * {@link org.elasticsearch.gradle.internal.flakiness.FlakinessLayout#TARGETS_DIR}.
 * Even a project that claims no ref reports its compiled class directories and source-set dispositions:
 * scan may find a runnable subclass there that belongs to a base from another project.
 * This keeps project-model access local while giving scan the facts needed to select the owning test tasks.
 * For the end-to-end pipeline, see {@code .buildkite/scripts/flakiness-detection/README.md}.
 */
package org.elasticsearch.gradle.internal.flakiness.resolve;
