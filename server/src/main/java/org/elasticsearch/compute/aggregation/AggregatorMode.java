/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

public enum AggregatorMode {

    INITIAL(false, true),

    INTERMEDIATE(true, true),

    FINAL(true, false),

    // most useful for testing
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
