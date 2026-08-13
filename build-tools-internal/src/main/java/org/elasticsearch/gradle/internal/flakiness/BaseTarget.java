/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A resolved base target: the project/sourceSet/kind a {@link FlakinessRef} was resolved to, before
 * bytecode enrichment. It is fully authoritative - every field is derived from the owning project's real
 * configured model (via {@link FlakinessModelService}), including the exact {@code compileTaskPath} the
 * compile step must run and the {@code outputDir} the scan step must scan.
 *
 * <p>A base target may still be abstract (in which case {@link PlanBuilder} flattens it into concrete
 * subclasses) or on a bwc project ({@code bwc == true}, marked {@code skip} downstream). yaml suite/runner
 * targets carry a {@code suitePath} rather than an {@code fqcn}; a parameterised yaml case carries both
 * {@code fqcn} and {@code yamlTest}.
 *
 * <p>Serialized to {@code flakiness-base-targets.json} (the resolve-&gt;compile/scan hand-off), so it must
 * round-trip through Jackson.
 *
 * @param gradleProject   owning Gradle project path
 * @param sourceSet       owning source-set name
 * @param kind            wire kind (see {@link Kinds})
 * @param fqcn            fully-qualified class name, or {@code null} for yaml suite/runner targets
 * @param suitePath       yaml suite path, or {@code null}
 * @param yamlTest        parameterised yaml case descriptor, or {@code null}
 * @param bwc             whether the owning project is a bwc project (skip downstream)
 * @param compileTaskPath authoritative {@code compile&lt;Ss&gt;Java} task path for this target's source set
 * @param outputDir       authoritative compiled-classes output directory for this target's source set
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BaseTarget(
    String gradleProject,
    String sourceSet,
    String kind,
    String fqcn,
    String suitePath,
    String yamlTest,
    boolean bwc,
    String compileTaskPath,
    String outputDir
) {}
