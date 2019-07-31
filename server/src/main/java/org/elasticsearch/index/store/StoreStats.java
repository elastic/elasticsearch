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

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class StoreStats implements Writeable, ToXContentFragment {

    private long sizeInBytes;

    public StoreStats() {

    }

    public StoreStats(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
    }

    public StoreStats(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public void add(StoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;
    }


    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STORE);
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, size());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String STORE = "store";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
    }
}
