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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/** 
 * Implements verification checks to the best extent possible
 * against legacy segments.
 * <p>
 * For files since ES 1.3, we have a lucene checksum, and
 * we verify both CRC32 + length from that.
 * For older segment files, we have an elasticsearch Adler32 checksum
 * and a length, except for commit points.
 * For older commit points, we only have the length in metadata,
 * but lucene always wrote a CRC32 checksum we can verify in the future, too.
 * For (Jurassic?) files, we dont have an Adler32 checksum at all,
 * since its optional in the protocol. But we always know the length.
 * @deprecated only to support old segments
 */
@Deprecated
class LegacyVerification {
    
    // TODO: add a verifier for old lucene segments_N that also checks CRC.
    // but for now, at least truncation is detected here (as length will be checked)
    
    /** 
     * verifies Adler32 + length for index files before lucene 4.8
     */
    static class Adler32VerifyingIndexOutput extends VerifyingIndexOutput {
        final String adler32;
        final long length;
        final Checksum checksum = new BufferedChecksum(new Adler32());
        long written;
        
        public Adler32VerifyingIndexOutput(IndexOutput out, String adler32, long length) {
            super(out);
            this.adler32 = adler32;
            this.length = length;
        }

        @Override
        public void verify() throws IOException {
            if (written != length) {
                throw new CorruptIndexException("expected length=" + length + " != actual length: " + written + " : file truncated?", out.toString()); 
            }
            final String actualChecksum = Store.digestToString(checksum.getValue());
            if (!adler32.equals(actualChecksum)) {
                throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + adler32 +
                                                " actual=" + actualChecksum, out.toString());
            }
        }

        @Override
        public void writeByte(byte b) throws IOException {
            out.writeByte(b);
            checksum.update(b);
            written++;
        }

        @Override
        public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
            out.writeBytes(bytes, offset, length);
            checksum.update(bytes, offset, length);
            written += length;
        }
    }
    
    /** 
     * verifies length for index files before lucene 4.8
     */
    static class LengthVerifyingIndexOutput extends VerifyingIndexOutput {
        final long length;
        long written;
        
        public LengthVerifyingIndexOutput(IndexOutput out, long length) {
            super(out);
            this.length = length;
        }

        @Override
        public void verify() throws IOException {
            if (written != length) {
                throw new CorruptIndexException("expected length=" + length + " != actual length: " + written + " : file truncated?", out.toString());
            }
        }

        @Override
        public void writeByte(byte b) throws IOException {
            out.writeByte(b);
            written++;
        }

        @Override
        public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
            out.writeBytes(bytes, offset, length);
            written += length;
        }
    }
}
