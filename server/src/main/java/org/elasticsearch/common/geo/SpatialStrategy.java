/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public enum SpatialStrategy implements Writeable {

    TERM("term"),
    RECURSIVE("recursive");

    private final String strategyName;

    SpatialStrategy(String strategyName) {
        this.strategyName = strategyName;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public static SpatialStrategy readFromStream(StreamInput in) throws IOException {
        return in.readEnum(SpatialStrategy.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SpatialStrategy fromString(String strategyName) {
        for (SpatialStrategy strategy : values()) {
            if (strategy.strategyName.equals(strategyName)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("Unknown strategy [" + strategyName + "]");
    }
}
