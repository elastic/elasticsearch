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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;

public class StoreFileMetaData implements Writeable {

    private final String name;

    // the actual file size on "disk", if compressed, the compressed size
    private final long length;

    private final String checksum;

    private final Version writtenBy;

    private final BytesRef hash;

    public StoreFileMetaData(String name, long length, String checksum, Version writtenBy) {
        this(name, length, checksum, writtenBy, null);
    }

    public StoreFileMetaData(String name, long length, String checksum, Version writtenBy, BytesRef hash) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.length = length;
        this.checksum = Objects.requireNonNull(checksum, "checksum must not be null");
        this.writtenBy = Objects.requireNonNull(writtenBy, "writtenBy must not be null");
        this.hash = hash == null ? new BytesRef() : hash;
    }

    /**
     * Read from a stream.
     */
    public StoreFileMetaData(StreamInput in) throws IOException {
        name = in.readString();
        length = in.readVLong();
        checksum = in.readString();
        try {
            writtenBy = Version.parse(in.readString());
        } catch (ParseException e) {
            throw new AssertionError(e);
        }
        hash = in.readBytesRef();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(length);
        out.writeString(checksum);
        out.writeString(writtenBy.toString());
        out.writeBytesRef(hash);
    }

    /**
     * Returns the name of this file
     */
    public String name() {
        return name;
    }

    /**
     * the actual file size on "disk", if compressed, the compressed size
     */
    public long length() {
        return length;
    }

    /**
     * Returns a string representation of the files checksum. Since Lucene 4.8 this is a CRC32 checksum written
     * by lucene.
     */
    public String checksum() {
        return this.checksum;
    }

    /**
     * Returns <code>true</code> iff the length and the checksums are the same. otherwise <code>false</code>
     */
    public boolean isSame(StoreFileMetaData other) {
        if (checksum == null || other.checksum == null) {
            // we can't tell if either or is null so we return false in this case! this is why we don't use equals for this!
            return false;
        }
        return length == other.length && checksum.equals(other.checksum) && hash.equals(other.hash);
    }

    @Override
    public String toString() {
        return "name [" + name + "], length [" + length + "], checksum [" + checksum + "], writtenBy [" + writtenBy + "]" ;
    }

    /**
     * Returns the Lucene version this file has been written by or <code>null</code> if unknown
     */
    public Version writtenBy() {
        return writtenBy;
    }

    /**
     * Returns a variable length hash of the file represented by this metadata object. This can be the file
     * itself if the file is small enough. If the length of the hash is <tt>0</tt> no hash value is available
     */
    public BytesRef hash() {
        return hash;
    }
}
