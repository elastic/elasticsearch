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

package org.elasticsearch.indices.fielddata.breaker;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Class encapsulating stats about the circuit breaker
 */
public class FieldDataBreakerStats implements Streamable, ToXContent {

    private long maximum;
    private long estimated;
    private long trippedCount;
    private double overhead;

    FieldDataBreakerStats() {

    }

    public FieldDataBreakerStats(long maximum, long estimated, double overhead, long trippedCount) {
        this.maximum = maximum;
        this.estimated = estimated;
        this.trippedCount = trippedCount;
        this.overhead = overhead;
    }

    public long getMaximum() {
        return this.maximum;
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

    public static FieldDataBreakerStats readOptionalCircuitBreakerStats(StreamInput in) throws IOException {
        FieldDataBreakerStats stats = in.readOptionalStreamable(new FieldDataBreakerStats());
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        maximum = in.readLong();
        estimated = in.readLong();
        overhead = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            this.trippedCount = in.readLong();
        } else {
            this.trippedCount = -1;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(maximum);
        out.writeLong(estimated);
        out.writeDouble(overhead);
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeLong(trippedCount);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.BREAKER);
        builder.field(Fields.MAX, maximum);
        builder.field(Fields.MAX_HUMAN, new ByteSizeValue(maximum));
        builder.field(Fields.ESTIMATED, estimated);
        builder.field(Fields.ESTIMATED_HUMAN, new ByteSizeValue(estimated));
        builder.field(Fields.OVERHEAD, overhead);
        builder.field(Fields.TRIPPED_COUNT, trippedCount);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString BREAKER = new XContentBuilderString("fielddata_breaker");
        static final XContentBuilderString MAX = new XContentBuilderString("maximum_size_in_bytes");
        static final XContentBuilderString MAX_HUMAN = new XContentBuilderString("maximum_size");
        static final XContentBuilderString ESTIMATED = new XContentBuilderString("estimated_size_in_bytes");
        static final XContentBuilderString ESTIMATED_HUMAN = new XContentBuilderString("estimated_size");
        static final XContentBuilderString OVERHEAD = new XContentBuilderString("overhead");
        static final XContentBuilderString TRIPPED_COUNT = new XContentBuilderString("tripped");
    }
}
