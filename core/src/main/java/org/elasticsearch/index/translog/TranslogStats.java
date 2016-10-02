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
package org.elasticsearch.index.translog;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class TranslogStats extends ToXContentToBytes implements Streamable {

    private long translogSizeInBytes;
    private int numberOfOperations;

    public TranslogStats() {
    }

    public TranslogStats(int numberOfOperations, long translogSizeInBytes) {
        if (numberOfOperations < 0) {
            throw new IllegalArgumentException("numberOfOperations must be >= 0");
        }
        if (translogSizeInBytes < 0) {
            throw new IllegalArgumentException("translogSizeInBytes must be >= 0");
        }
        assert translogSizeInBytes >= 0 : "translogSizeInBytes must be >= 0, got [" + translogSizeInBytes + "]";
        this.numberOfOperations = numberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
    }

    public void add(TranslogStats translogStats) {
        if (translogStats == null) {
            return;
        }

        this.numberOfOperations += translogStats.numberOfOperations;
        this.translogSizeInBytes += translogStats.translogSizeInBytes;
    }

    public long getTranslogSizeInBytes() {
        return translogSizeInBytes;
    }

    public long estimatedNumberOfOperations() {
        return numberOfOperations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSLOG);
        builder.field(Fields.OPERATIONS, numberOfOperations);
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, translogSizeInBytes);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRANSLOG = "translog";
        static final String OPERATIONS = "operations";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        numberOfOperations = in.readVInt();
        translogSizeInBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfOperations);
        out.writeVLong(translogSizeInBytes);
    }
}
