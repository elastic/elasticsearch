/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

public enum AggregatorMode {

    INITIAL(true, true),

    INTERMEDIATE(false, true),

    FINAL(false, false),

    // most useful for testing
    SINGLE(true, false);

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
