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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class MemoryUsage implements Writeable, ToXContentFragment {

    private final Map<String, PoolUsage> poolStats;

    public MemoryUsage(Map<String, PoolUsage> poolStats) {
        this.poolStats = Collections.unmodifiableMap(poolStats);
    }

    public Map<String, PoolUsage> getPoolUsage() {
        return poolStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(poolStats, StreamOutput::writeString, (o, w) -> w.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(poolStats);
        return builder;
    }

    public static class PoolUsage implements Writeable, ToXContentFragment {

        private final long poolSizeBytes;
        private final long bytesAllocated;

        public PoolUsage(long poolSizeBytes, long bytesAllocated) {
            this.poolSizeBytes = poolSizeBytes;
            this.bytesAllocated = bytesAllocated;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(poolSizeBytes);
            out.writeLong(bytesAllocated);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.POOL_SIZE_IN_BYTES, Fields.POOL_SIZE, new ByteSizeValue(poolSizeBytes));
            builder.humanReadableField(Fields.ALLOCATED_IN_BYTES, Fields.ALLOCATED, new ByteSizeValue(bytesAllocated));
            return builder;
        }
    }

    static final class Fields {
        static final String POOL_SIZE_IN_BYTES = "pool_size";
        static final String POOL_SIZE = "pool_size_in_bytes";
        static final String ALLOCATED = "amount_allocated";
        static final String ALLOCATED_IN_BYTES = "amount_allocated_in_bytes";
    }
}
