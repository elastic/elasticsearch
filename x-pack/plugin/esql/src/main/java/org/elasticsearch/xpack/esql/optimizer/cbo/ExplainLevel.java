/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

/**
 * How much of the reasoning behind cost-based decisions a run records. Higher levels cost more planning time but never change the
 * decisions themselves.
 */
public enum ExplainLevel {
    /** No report is built. */
    NONE,
    /** Reports the chosen plan only; alternatives that need a counterfactual search to be costed are marked not costed. */
    BASIC,
    /** Also costs every other enumerated alternative with a counterfactual search. */
    FULL
}
