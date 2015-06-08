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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;

/**
 *
 */
public class StoreFileMetaData implements Streamable {

    private String name;

    // the actual file size on "disk", if compressed, the compressed size
    private long length;

    private String checksum;

    private Version writtenBy;

    private BytesRef hash;

    private StoreFileMetaData() {
    }

    public StoreFileMetaData(String name, long length) {
        this(name, length, null);
    }

    public StoreFileMetaData(String name, long length, String checksum) {
        this(name, length, checksum, null, null);
    }

    public StoreFileMetaData(String name, long length, String checksum, Version writtenBy) {
        this(name, length, checksum, writtenBy, null);
    }

    public StoreFileMetaData(String name, long length, String checksum, Version writtenBy, BytesRef hash) {
        this.name = name;
        this.length = length;
        this.checksum = checksum;
        this.writtenBy = writtenBy;
        this.hash = hash == null ? new BytesRef() : hash;
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
     * by lucene. Previously we use Adler32 on top of Lucene as the checksum algorithm, if {@link #hasLegacyChecksum()} returns
     * <code>true</code> this is a Adler32 checksum.
     * @return
     */
    @Nullable
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

    public static StoreFileMetaData readStoreFileMetaData(StreamInput in) throws IOException {
        StoreFileMetaData md = new StoreFileMetaData();
        md.readFrom(in);
        return md;
    }

    @Override
    public String toString() {
        return "name [" + name + "], length [" + length + "], checksum [" + checksum + "], writtenBy [" + writtenBy + "]" ;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        length = in.readVLong();
        checksum = in.readOptionalString();
        String versionString = in.readOptionalString();
        writtenBy = Lucene.parseVersionLenient(versionString, null);
        hash = in.readBytesRef();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(length);
        out.writeOptionalString(checksum);
        out.writeOptionalString(writtenBy == null ? null : writtenBy.toString());
        out.writeBytesRef(hash);
    }

    /**
     * Returns the Lucene version this file has been written by or <code>null</code> if unknown
     */
    public Version writtenBy() {
        return writtenBy;
    }

    /**
     * Returns <code>true</code>  iff the checksum is not <code>null</code> and if the file has NOT been written by
     * a Lucene version greater or equal to Lucene 4.8
     */
    public boolean hasLegacyChecksum() {
        return checksum != null && (writtenBy == null || writtenBy.onOrAfter(Version.LUCENE_4_8) == false);
    }

    /**
     * Returns a variable length hash of the file represented by this metadata object. This can be the file
     * itself if the file is small enough. If the length of the hash is <tt>0</tt> no hash value is available
     */
    public BytesRef hash() {
        return hash;
    }
}
