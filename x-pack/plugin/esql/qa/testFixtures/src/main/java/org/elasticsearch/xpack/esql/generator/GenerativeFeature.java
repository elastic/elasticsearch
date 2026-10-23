/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;

import java.util.List;

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
    BASELINE(),

    /**
     * Allow {@code FROM (...)} subqueries to be produced as sources by the per-feature generative test harness.
     */
    SUBQUERIES(Cap.SUBQUERY_IN_FROM_COMMAND),

    /**
     * Always prepend {@code SET unmapped_fields="load"} and force unmapped
     * fields to be loaded as keyword fields.
     */
    UNMAPPED_FIELDS_LOAD(Cap.OPTIONAL_FIELDS_V5),

    /**
     * Always prepend {@code SET unmapped_fields="load_all"}, loading every source field absent from the index mapping as its own
     * {@code keyword} column without requiring each to be referenced. Snapshot-only, so it is skipped on clusters that don't advertise it.
     */
    UNMAPPED_FIELDS_LOAD_ALL(Cap.OPTIONAL_FIELDS_LOAD_ALL_V2),

    /**
     * Start generated pipelines from time-series source commands.
     */
    METRICS(Cap.TS_COMMAND_V0),

    /**
     * Start generated pipelines from {@code PROMQL}.
     */
    PROMQL(Cap.PROMQL_COMMAND_V0),

    /**
     * Generate {@code FROM <dataset>} queries against external parquet datasets registered via
     * {@code PUT /_query/dataset/} using an in-process S3 fixture.
     */
    PARQUET_DATASET(Cap.DATASET_IN_FROM_COMMAND);

    private final List<Cap> requiredCapabilities;

    GenerativeFeature(Cap... requiredCapabilities) {
        this.requiredCapabilities = List.of(requiredCapabilities);
    }

    /**
     * Capabilities the cluster under test must support for this feature to run. The cluster may be older than the
     * test code, so a feature it lacks is skipped rather than reported as a failure.
     */
    public List<Cap> requiredCapabilities() {
        return requiredCapabilities;
    }
}
