/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

public enum AggregatorMode {

    PARTIAL(true, true),

    FINAL(false, false),

    INTERMEDIATE(false, true),

    SINGLE(true, false);

    //
    // createIntermediate - intermediate input
    // FINAL(false, false),
    // INTERMEDIATE(false, true),

    // create - raw input
    // SINGLE(true, false);
    // PARTIAL(true, true),

    // process path - input
    // raw / intermediate
    // evaluate - output
    // final / intermediate

    private final boolean inputRaw;

    private final boolean outputPartial;

    AggregatorMode(boolean inputRaw, boolean outputPartial) {
        this.inputRaw = inputRaw;
        this.outputPartial = outputPartial;
    }

    public boolean isInputRaw() {
        return inputRaw;
    }

    public boolean isOutputPartial() {
        return outputPartial;
    }

}
