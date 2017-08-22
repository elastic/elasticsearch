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

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class TranslogStats implements Streamable, ToXContentFragment {

    private long translogSizeInBytes;
    private int numberOfOperations;
    private long uncommittedSizeInBytes;
    private int  uncommittedOperations;

    public TranslogStats() {
    }

    public TranslogStats(int numberOfOperations, long translogSizeInBytes, int uncommittedOperations, long uncommittedSizeInBytes) {
        if (numberOfOperations < 0) {
            throw new IllegalArgumentException("numberOfOperations must be >= 0");
        }
        if (translogSizeInBytes < 0) {
            throw new IllegalArgumentException("translogSizeInBytes must be >= 0");
        }
        if (uncommittedOperations < 0) {
            throw new IllegalArgumentException("uncommittedOperations must be >= 0");
        }
        if (uncommittedSizeInBytes < 0) {
            throw new IllegalArgumentException("uncommittedSizeInBytes must be >= 0");
        }
        this.numberOfOperations = numberOfOperations;
        this.translogSizeInBytes = translogSizeInBytes;
        this.uncommittedSizeInBytes = uncommittedSizeInBytes;
        this.uncommittedOperations = uncommittedOperations;
    }

    public void add(TranslogStats translogStats) {
        if (translogStats == null) {
            return;
        }

        this.numberOfOperations += translogStats.numberOfOperations;
        this.translogSizeInBytes += translogStats.translogSizeInBytes;
        this.uncommittedOperations += translogStats.uncommittedOperations;
        this.uncommittedSizeInBytes += translogStats.uncommittedSizeInBytes;
    }

    public long getTranslogSizeInBytes() {
        return translogSizeInBytes;
    }

    public int estimatedNumberOfOperations() {
        return numberOfOperations;
    }

    /** the size of the generations in the translog that weren't yet to comitted to lucene */
    public long getUncommittedSizeInBytes() {
        return uncommittedSizeInBytes;
    }

    /** the number of operations in generations of the translog that weren't yet to comitted to lucene */
    public int getUncommittedOperations() {
        return uncommittedOperations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("translog");
        builder.field("operations", numberOfOperations);
        builder.byteSizeField("size_in_bytes", "size", translogSizeInBytes);
        builder.field("uncommitted_operations", uncommittedOperations);
        builder.byteSizeField("uncommitted_size_in_bytes", "uncommitted_size", uncommittedSizeInBytes);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        numberOfOperations = in.readVInt();
        translogSizeInBytes = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
            uncommittedOperations = in.readVInt();
            uncommittedSizeInBytes = in.readVLong();
        } else {
            uncommittedOperations = numberOfOperations;
            uncommittedSizeInBytes = translogSizeInBytes;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfOperations);
        out.writeVLong(translogSizeInBytes);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
            out.writeVInt(uncommittedOperations);
            out.writeVLong(uncommittedSizeInBytes);
        }
    }
}
