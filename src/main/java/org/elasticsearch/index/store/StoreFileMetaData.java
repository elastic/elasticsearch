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

import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 *
 */
public class StoreFileMetaData implements Streamable {

    private String name;

    // the actual file size on "disk", if compressed, the compressed size
    private long length;

    private String checksum;

    private transient Directory directory;

    private StoreFileMetaData() {
    }

    public StoreFileMetaData(String name, long length, String checksum) {
        this(name, length, checksum, null);
    }

    public StoreFileMetaData(String name, long length, String checksum, @Nullable Directory directory) {
        this.name = name;
        this.length = length;
        this.checksum = checksum;
        this.directory = directory;
    }

    public Directory directory() {
        return this.directory;
    }

    public String name() {
        return name;
    }

    /**
     * the actual file size on "disk", if compressed, the compressed size
     */
    public long length() {
        return length;
    }

    @Nullable
    public String checksum() {
        return this.checksum;
    }

    public boolean isSame(StoreFileMetaData other) {
        if (checksum == null || other.checksum == null) {
            return false;
        }
        return length == other.length && checksum.equals(other.checksum);
    }

    public static StoreFileMetaData readStoreFileMetaData(StreamInput in) throws IOException {
        StoreFileMetaData md = new StoreFileMetaData();
        md.readFrom(in);
        return md;
    }

    @Override
    public String toString() {
        return "name [" + name + "], length [" + length + "], checksum [" + checksum + "]";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        length = in.readVLong();
        if (in.readBoolean()) {
            checksum = in.readString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(length);
        if (checksum == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(checksum);
        }
    }
}
