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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.InputStreamDataInput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Encapsulating class used for operating on translog streams. Static methods
 * on this class use the latest version of the stream.
 */
public class TranslogStreams {

    /** V0, no header, no checksums */
    public static TranslogStream LEGACY_TRANSLOG_STREAM = new LegacyTranslogStream();
    /** V1, header, with per-op checksums */
    public static TranslogStream CHECKSUMMED_TRANSLOG_STREAM = new ChecksummedTranslogStream();

    public static TranslogStream LATEST = CHECKSUMMED_TRANSLOG_STREAM;

    public static final String TRANSLOG_CODEC = "translog";
    private static final byte LUCENE_CODEC_HEADER_BYTE = 0x3f;
    private static final byte UNVERSIONED_TRANSLOG_HEADER_BYTE = 0x00;

    /**
     * Returns a new empty translog operation for the given {@link Translog.Operation.Type}
     */
    static Translog.Operation newOperationFromType(Translog.Operation.Type type) throws IOException {
        switch (type) {
            case CREATE:
                return new Translog.Create();
            case DELETE:
                return new Translog.Delete();
            case DELETE_BY_QUERY:
                return new Translog.DeleteByQuery();
            case SAVE:
                return new Translog.Index();
            default:
                throw new IOException("No type for [" + type + "]");
        }
    }

    /**
     * Read the next {@link Translog.Operation} from the stream using the
     * latest translog version
     */
    public static Translog.Operation readTranslogOperation(StreamInput in) throws IOException {
        return LATEST.read(in);
    }

    /**
     * Write the {@link Translog.Operation} to the output stream using the
     * latest translog version
     */
    public static void writeTranslogOperation(StreamOutput out, Translog.Operation op) throws IOException {
        LATEST.write(out, op);
    }

    /**
     * Given a file, return a VersionedTranslogStream based on an
     * optionally-existing header in the file. If the file does not exist, or
     * has zero length, returns the latest version. If the header does not
     * exist, assumes Version 0 of the translog file format.
     * <p/>
     * The caller is responsible for closing the TranslogStream.
     *
     * @throws IOException
     */
    public static TranslogStream translogStreamFor(Path translogFile) throws IOException {

        try (InputStreamStreamInput headerStream = new InputStreamStreamInput(Files.newInputStream(translogFile))) {
            if (Files.exists(translogFile) == false || Files.size(translogFile) == 0) {
                // if it doesn't exist or has no data, use the latest version,
                // there aren't any backwards compatibility issues
                return CHECKSUMMED_TRANSLOG_STREAM;
            }
            // Lucene's CodecUtil writes a magic number of 0x3FD76C17 with the
            // header, in binary this looks like:
            //
            // binary: 0011 1111 1101 0111 0110 1100 0001 0111
            // hex   :    3    f    d    7    6    c    1    7
            //
            // With version 0 of the translog, the first byte is the
            // Operation.Type, which will always be between 0-4, so we know if
            // we grab the first byte, it can be:
            // 0x3f => Lucene's magic number, so we can assume it's version 1 or later
            // 0x00 => version 0 of the translog
            //
            // otherwise the first byte of the translog is corrupted and we
            // should bail
            byte b1 = headerStream.readByte();
            if (b1 == LUCENE_CODEC_HEADER_BYTE) {
                // Read 3 more bytes, meaning a whole integer has been read
                byte b2 = headerStream.readByte();
                byte b3 = headerStream.readByte();
                byte b4 = headerStream.readByte();
                // Convert the 4 bytes that were read into an integer
                int header = ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + ((b4 & 0xFF) << 0);
                // We confirm CodecUtil's CODEC_MAGIC number (0x3FD76C17)
                // ourselves here, because it allows us to read the first
                // byte separately
                if (header != CodecUtil.CODEC_MAGIC) {
                    throw new TranslogCorruptedException("translog looks like version 1 or later, but has corrupted header");
                }
                // Confirm the rest of the header using CodecUtil, extracting
                // the translog version
                int version = CodecUtil.checkHeaderNoMagic(new InputStreamDataInput(headerStream), TRANSLOG_CODEC, 1, Integer.MAX_VALUE);
                switch (version) {
                    case ChecksummedTranslogStream.VERSION:
                        return CHECKSUMMED_TRANSLOG_STREAM;
                    default:
                        throw new TranslogCorruptedException("No known translog stream version: " + version);
                }
            } else if (b1 == UNVERSIONED_TRANSLOG_HEADER_BYTE) {
                return LEGACY_TRANSLOG_STREAM;
            } else {
                throw new TranslogCorruptedException("Invalid first byte in translog file, got: " + Long.toHexString(b1) + ", expected 0x00 or 0x3f");
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new TranslogCorruptedException("Translog header corrupted", e);
        }
    }
}
