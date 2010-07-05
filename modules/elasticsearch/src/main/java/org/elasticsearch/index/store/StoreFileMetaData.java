/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class StoreFileMetaData implements Streamable {

    private String name;

    private long lastModified;

    private long sizeInBytes;

    private String md5;

    StoreFileMetaData() {
    }

    public StoreFileMetaData(String name, long sizeInBytes, long lastModified, String md5) {
        this.name = name;
        this.lastModified = lastModified;
        this.sizeInBytes = sizeInBytes;
        this.md5 = md5;
    }

    public String name() {
        return name;
    }

    public long lastModified() {
        return this.lastModified;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public String md5() {
        return md5;
    }

    public static StoreFileMetaData readStoreFileMetaData(StreamInput in) throws IOException {
        StoreFileMetaData md = new StoreFileMetaData();
        md.readFrom(in);
        return md;
    }

    @Override public String toString() {
        return "name[" + name + "], sizeInBytes[" + sizeInBytes + "], md5[" + md5 + "]";
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        sizeInBytes = in.readVLong();
        if (in.readBoolean()) {
            md5 = in.readUTF();
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVLong(sizeInBytes);
        if (md5 == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(md5);
        }
    }
}
