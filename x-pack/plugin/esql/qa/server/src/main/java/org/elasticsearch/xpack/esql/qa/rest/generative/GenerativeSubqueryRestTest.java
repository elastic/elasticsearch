/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;

import java.util.Set;

/**
 * Generative REST tests that exercise {@code FROM (...)} subquery generation. Lives in a separate
 * class hierarchy from {@link GenerativeRestTest} so that CI muting of subquery-related failures
 * does not affect the main generative test suite.
 * <p>
 * Subquery generation is opt-in via {@link GenerativeFeature#SUBQUERIES}; this class enables it
 * by overriding {@link #rootGenerationContext()}. The shared generator infrastructure (depth
 * tracking in {@link GenerationContext}, FORK/full-text guards) is unchanged.
 */
public abstract class GenerativeSubqueryRestTest extends GenerativeRestTest {

    /**
     * Subquery-specific allowed errors. Kept here (rather than in {@link GenerativeRestTest#ALLOWED_ERRORS})
     * so muting them only affects this class.
     */
    protected static final Set<String> SUBQUERY_ALLOWED_ERRORS = Set.of(
        // Mixing a subquery with plain index patterns in a multi-source FROM can surface fields whose types
        // differ across branches. Plain FROM a, b would resolve these via union types, but the subquery-aware
        // resolution (EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_CONFLICT_RESOLUTION) rejects
        // them outright. The generator has no cheap way to predict these cross-branch collisions.
        "has conflicting data types in subqueries"
    );

    @Override
    protected GenerationContext rootGenerationContext() {
        return GenerationContext.root(Set.of(GenerativeFeature.SUBQUERIES));
    }

    @Override
    protected Set<String> getAllowedErrors() {
        return SUBQUERY_ALLOWED_ERRORS;
    }
}
