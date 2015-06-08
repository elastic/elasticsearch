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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 *
 */
public class TranslogStats implements ToXContent, Streamable {

    private long translogSizeInBytes = 0;
    private int estimatedNumberOfOperations = -1;

    public TranslogStats() {
    }

    public TranslogStats(int estimatedNumberOfOperations, long translogSizeInBytes) {
        assert translogSizeInBytes >= 0 : "translogSizeInBytes must be >= 0, got [" + translogSizeInBytes + "]";
        this.estimatedNumberOfOperations = estimatedNumberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
    }

    public void add(TranslogStats translogStats) {
        if (translogStats == null) {
            return;
        }

        this.estimatedNumberOfOperations += translogStats.estimatedNumberOfOperations;
        this.translogSizeInBytes = +translogStats.translogSizeInBytes;
    }

    public ByteSizeValue translogSizeInBytes() {
        return new ByteSizeValue(translogSizeInBytes);
    }

    public long estimatedNumberOfOperations() {
        return estimatedNumberOfOperations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSLOG);
        builder.field(Fields.OPERATIONS, estimatedNumberOfOperations);
        builder.byteSizeField(Fields.SIZE_IN_BYTES, Fields.SIZE, translogSizeInBytes);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString TRANSLOG = new XContentBuilderString("translog");
        static final XContentBuilderString OPERATIONS = new XContentBuilderString("operations");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        estimatedNumberOfOperations = in.readVInt();
        translogSizeInBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(estimatedNumberOfOperations);
        out.writeVLong(translogSizeInBytes);
    }
}
