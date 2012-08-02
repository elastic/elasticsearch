/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

    private long lastModified;

    // the actual file size on "disk", if compressed, the compressed size
    private long length;

    private String checksum;

    private transient Directory directory;

    StoreFileMetaData() {
    }

    public StoreFileMetaData(String name, long length, long lastModified, String checksum) {
        this(name, length, lastModified, checksum, null);
    }

    public StoreFileMetaData(String name, long length, long lastModified, String checksum, @Nullable Directory directory) {
        this.name = name;
        this.lastModified = lastModified;
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

    public long lastModified() {
        return this.lastModified;
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
        name = in.readUTF();
        length = in.readVLong();
        if (in.readBoolean()) {
            checksum = in.readUTF();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVLong(length);
        if (checksum == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(checksum);
        }
    }
}
