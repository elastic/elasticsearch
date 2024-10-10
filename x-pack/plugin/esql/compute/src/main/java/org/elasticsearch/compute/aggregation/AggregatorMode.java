/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

public enum AggregatorMode {

    /**
     * Maps raw inputs to intermediate outputs.
     */
    INITIAL(false, true),

    /**
     * Maps intermediate inputs to intermediate outputs.
     */
    INTERMEDIATE(true, true),

    /**
     * Maps intermediate inputs to final outputs.
     */
    FINAL(true, false),

    /**
     * Maps raw inputs to final outputs. Most useful for testing.
     */
    SINGLE(false, false);

    private final boolean inputPartial;

    private final boolean outputPartial;

    AggregatorMode(boolean inputPartial, boolean outputPartial) {
        this.inputPartial = inputPartial;
        this.outputPartial = outputPartial;
    }

    public boolean isInputPartial() {
        return inputPartial;
    }

    public boolean isOutputPartial() {
        return outputPartial;
    }
}
