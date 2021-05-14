/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats class encapsulating all of the different circuit breaker stats
 */
public class AllCircuitBreakerStats implements Writeable, ToXContentFragment {

    private final CircuitBreakerStats[] allStats;

    public AllCircuitBreakerStats(CircuitBreakerStats[] allStats) {
        this.allStats = allStats;
    }

    public AllCircuitBreakerStats(StreamInput in) throws IOException {
        allStats = in.readArray(CircuitBreakerStats::new, CircuitBreakerStats[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(allStats);
    }

    public CircuitBreakerStats[] getAllStats() {
        return this.allStats;
    }

    public CircuitBreakerStats getStats(String name) {
        for (CircuitBreakerStats stats : allStats) {
            if (stats.getName().equals(name)) {
                return stats;
            }
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.BREAKERS);
        for (CircuitBreakerStats stats : allStats) {
            if (stats != null) {
                stats.toXContent(builder, params);
            }
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String BREAKERS = "breakers";
    }
}
