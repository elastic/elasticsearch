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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;

import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;

public class StoreFileMetadata implements Writeable {

    private final String name;

    // the actual file size on "disk", if compressed, the compressed size
    private final long length;

    private final String checksum;

    private final Version writtenBy;

    private final BytesRef hash;

    public StoreFileMetadata(String name, long length, String checksum, Version writtenBy) {
        this(name, length, checksum, writtenBy, null);
    }

    public StoreFileMetadata(String name, long length, String checksum, Version writtenBy, BytesRef hash) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.length = length;
        this.checksum = Objects.requireNonNull(checksum, "checksum must not be null");
        this.writtenBy = Objects.requireNonNull(writtenBy, "writtenBy must not be null");
        this.hash = hash == null ? new BytesRef() : hash;
    }

    /**
     * Read from a stream.
     */
    public StoreFileMetadata(StreamInput in) throws IOException {
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
     * Checks if the bytes returned by {@link #hash()} are the contents of the file that this instances refers to.
     *
     * @return {@code true} iff {@link #hash()} will return the actual file contents
     */
    public boolean hashEqualsContents() {
        if (hash.length == length) {
            try {
                final boolean checksumsMatch = Store.digestToString(CodecUtil.retrieveChecksum(
                    new ByteArrayIndexInput("store_file", hash.bytes, hash.offset, hash.length))).equals(checksum);
                assert checksumsMatch : "Checksums did not match for [" + this + "] which has a hash of [" + hash + "]";
                return checksumsMatch;
            } catch (Exception e) {
                // Hash didn't contain any bytes that Lucene could extract a checksum from so we can't verify against the checksum of the
                // original file. We should never see an exception here because lucene files are assumed to always contain the checksum
                // footer.
                assert false : new AssertionError("Saw exception for hash [" + hash + "] but expected it to be Lucene file", e);
                return false;
            }
        }
        return false;
    }

    /**
     * Returns <code>true</code> iff the length and the checksums are the same. otherwise <code>false</code>
     */
    public boolean isSame(StoreFileMetadata other) {
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
     * itself if the file is small enough. If the length of the hash is {@code 0} no hash value is available
     */
    public BytesRef hash() {
        return hash;
    }
}
