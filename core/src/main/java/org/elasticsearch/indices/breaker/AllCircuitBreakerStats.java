/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Stats class encapsulating all of the different circuit breaker stats
 */
public class AllCircuitBreakerStats implements Streamable, ToXContent {

    private CircuitBreakerStats[] allStats = new CircuitBreakerStats[0];

    public AllCircuitBreakerStats() {

    }

    public AllCircuitBreakerStats(CircuitBreakerStats[] allStats) {
        this.allStats = allStats;
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

    public static AllCircuitBreakerStats readOptionalAllCircuitBreakerStats(StreamInput in) throws IOException {
        AllCircuitBreakerStats stats = in.readOptionalStreamable(AllCircuitBreakerStats::new);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int statCount = in.readVInt();
        CircuitBreakerStats[] newStats = new CircuitBreakerStats[statCount];
        for (int i = 0; i < statCount; i++) {
            CircuitBreakerStats stats = new CircuitBreakerStats();
            stats.readFrom(in);
            newStats[i] = stats;
        }
        allStats = newStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(allStats.length);
        for (CircuitBreakerStats stats : allStats) {
            if (stats != null) {
                stats.writeTo(out);
            }
        }
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
        static final XContentBuilderString BREAKERS = new XContentBuilderString("breakers");
    }
}
