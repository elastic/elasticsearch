/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

/**
 * Optional behaviors that a generative test run can opt into via {@link GenerationContext}.
 * Features are off by default in the main generative suite so that failures specific to one
 * feature can be isolated to a dedicated test class without muting the rest.
 */
public enum GenerativeFeature {
    /**
     * No optional feature enabled; sentinel for the default generative suite.
     * <p>
     * Not to be used in code. This is the same as no features enabled.
     * */
    BASELINE,

    /**
     * Allow {@code FROM (...)} subqueries to be produced as sources by the per-feature generative test harness.
     */
    SUBQUERIES,

    /**
     * Always prepend {@code SET unmapped_fields="load"} and force unmapped
     * fields to be loaded as keyword fields.
     */
    UNMAPPED_FIELDS_LOAD,

    /**
     * Start generated pipelines from time-series source commands.
     */
    METRICS,

    /**
     * Start generated pipelines from {@code PROMQL}.
     */
    PROMQL
}
