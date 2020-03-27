/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.stats.common;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class MemoryUsage implements ToXContentObject {

    static final ParseField TIMESTAMP = new ParseField("timestamp");
    static final ParseField PEAK_USAGE_BYTES = new ParseField("peak_usage_bytes");

    public static final ConstructingObjectParser<MemoryUsage, Void> PARSER = new ConstructingObjectParser<>("analytics_memory_usage",
        true, a -> new MemoryUsage((Instant) a[0], (long) a[1]));

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), PEAK_USAGE_BYTES);
    }

    @Nullable
    private final Instant timestamp;
    private final long peakUsageBytes;

    public MemoryUsage(@Nullable Instant timestamp, long peakUsageBytes) {
        this.timestamp = timestamp == null ? null : Instant.ofEpochMilli(Objects.requireNonNull(timestamp).toEpochMilli());
        this.peakUsageBytes = peakUsageBytes;
    }

    @Nullable
    public Instant getTimestamp() {
        return timestamp;
    }

    public long getPeakUsageBytes() {
        return peakUsageBytes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (timestamp != null) {
            builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.toEpochMilli());
        }
        builder.field(PEAK_USAGE_BYTES.getPreferredName(), peakUsageBytes);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryUsage other = (MemoryUsage) o;
        return Objects.equals(timestamp, other.timestamp)
            && peakUsageBytes == other.peakUsageBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, peakUsageBytes);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(getClass())
            .add(TIMESTAMP.getPreferredName(), timestamp == null ? null : timestamp.getEpochSecond())
            .add(PEAK_USAGE_BYTES.getPreferredName(), peakUsageBytes)
            .toString();
    }
}
