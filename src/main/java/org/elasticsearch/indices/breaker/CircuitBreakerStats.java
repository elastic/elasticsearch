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

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Locale;

/**
 * Class encapsulating stats about the circuit breaker
 */
public class CircuitBreakerStats implements Streamable, ToXContent {

    private CircuitBreaker.Name name;
    private long limit;
    private long estimated;
    private long trippedCount;
    private double overhead;

    CircuitBreakerStats() {

    }

    public CircuitBreakerStats(CircuitBreaker.Name name, long limit, long estimated, double overhead, long trippedCount) {
        this.name = name;
        this.limit = limit;
        this.estimated = estimated;
        this.trippedCount = trippedCount;
        this.overhead = overhead;
    }

    public CircuitBreaker.Name getName() {
        return this.name;
    }

    public long getLimit() {
        return this.limit;
    }

    public long getEstimated() {
        return this.estimated;
    }

    public long getTrippedCount() {
        return this.trippedCount;
    }

    public double getOverhead() {
        return this.overhead;
    }

    public static CircuitBreakerStats readOptionalCircuitBreakerStats(StreamInput in) throws IOException {
        CircuitBreakerStats stats = in.readOptionalStreamable(new CircuitBreakerStats());
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        // limit is the maximum from the old circuit breaker stats for backwards compatibility
        limit = in.readLong();
        estimated = in.readLong();
        overhead = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            this.trippedCount = in.readLong();
        } else {
            this.trippedCount = -1;
        }
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            this.name = CircuitBreaker.Name.readFrom(in);
        } else {
            this.name = CircuitBreaker.Name.FIELDDATA;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(limit);
        out.writeLong(estimated);
        out.writeDouble(overhead);
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeLong(trippedCount);
        }
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            CircuitBreaker.Name.writeTo(name, out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name.toString().toLowerCase(Locale.ROOT));
        builder.field(Fields.LIMIT, limit);
        builder.field(Fields.LIMIT_HUMAN, new ByteSizeValue(limit));
        builder.field(Fields.ESTIMATED, estimated);
        builder.field(Fields.ESTIMATED_HUMAN, new ByteSizeValue(estimated));
        builder.field(Fields.OVERHEAD, overhead);
        builder.field(Fields.TRIPPED_COUNT, trippedCount);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "[" + this.name.toString() +
                ",limit=" + this.limit + "/" + new ByteSizeValue(this.limit) +
                ",estimated=" + this.estimated + "/" + new ByteSizeValue(this.estimated) +
                ",overhead=" + this.overhead + ",tripped=" + this.trippedCount + "]";
    }

    static final class Fields {
        static final XContentBuilderString LIMIT = new XContentBuilderString("limit_size_in_bytes");
        static final XContentBuilderString LIMIT_HUMAN = new XContentBuilderString("limit_size");
        static final XContentBuilderString ESTIMATED = new XContentBuilderString("estimated_size_in_bytes");
        static final XContentBuilderString ESTIMATED_HUMAN = new XContentBuilderString("estimated_size");
        static final XContentBuilderString OVERHEAD = new XContentBuilderString("overhead");
        static final XContentBuilderString TRIPPED_COUNT = new XContentBuilderString("tripped");
    }
}
