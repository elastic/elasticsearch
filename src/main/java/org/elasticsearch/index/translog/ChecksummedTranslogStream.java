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
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.common.io.stream.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Version 1 of the translog file format. Writes a header to identify the
 * format, also writes checksums for each operation
 */
public class ChecksummedTranslogStream implements TranslogStream {

    public static final int VERSION = 1;

    ChecksummedTranslogStream() {
    }

    /** Read and verify the checksum for the BufferedChecksumStreamInput */
    private void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    /** Read the operation size twice, verifying that the values are equal */
    private int readAndVerifyOpSize(StreamInput in) throws IOException {
        int opSize1 = in.readInt();
        int opSize2 = in.readInt();
        if (opSize1 != opSize2) {
            throw new TranslogCorruptedException("translog operation sizes do not match, got: "
                    + opSize1 + ", and: " + opSize2);
        }

        return opSize1;
    }

    /** Read a translog operation from the stream, without reading the size or checksum */
    private Translog.Operation readOperation(StreamInput in) throws IOException {
        Translog.Operation operation;
        try {
            Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
            operation = TranslogStreams.newOperationFromType(type);
            operation.readFrom(in);
        } catch (AssertionError|Exception e) {
            throw new TranslogCorruptedException("translog corruption while reading from stream", e);
        }
        return operation;
    }

    @Override
    public Translog.Operation read(StreamInput inStream) throws IOException {
        int opSize = readAndVerifyOpSize(inStream);

        // This BufferedChecksumStreamInput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(inStream);
        Translog.Operation operation = readOperation(in);
        verifyChecksum(in);
        return operation;
    }

    @Override
    public Translog.Operation greedyRead(StreamInput in) throws IOException {
        int opSize = readAndVerifyOpSize(in);

        byte[] bytes = new byte[opSize];
        in.read(bytes, 0, opSize);
        // The last 4 bytes are the checksum
        byte b1 = bytes[opSize - 4];
        byte b2 = bytes[opSize - 3];
        byte b3 = bytes[opSize - 2];
        byte b4 = bytes[opSize - 1];
        // Convert the last 4 bytes into an integer
        int checksum = ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + ((b4 & 0xFF) << 0);
        long expectedChecksum = checksum & 0xFFFF_FFFFL;
        Checksum digest = new BufferedChecksum(new CRC32());
        // Update the digest based on the bytes from the stream, minus 4 to
        // exclude the checksum
        digest.update(bytes, 0, opSize - 4);
        long actualChecksum = digest.getValue();
        if (expectedChecksum != actualChecksum) {
            throw new TranslogCorruptedException("translog operation is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(actualChecksum));
        }
        // BytesStreamInput does not need to be closed (closing it is a noop)
        BytesStreamInput bsi = new BytesStreamInput(bytes, false);
        return readOperation(bsi);
    }

    @Override
    public void write(StreamOutput outStream, Translog.Operation op) throws IOException {
        // We first write to a NoopStreamOutput to get the size of the
        // operation. We could write to a byte array and then send that as an
        // alternative, but here we choose to use CPU over allocating new
        // byte arrays.
        NoopStreamOutput noopOut = new NoopStreamOutput();
        noopOut.writeByte(op.opType().id());
        op.writeTo(noopOut);
        noopOut.writeInt(0); // checksum holder
        int size = noopOut.getCount();

        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(outStream);
        outStream.writeInt(size); // opSize is not checksummed
        outStream.writeInt(size); // second opSize
        out.writeByte(op.opType().id());
        op.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int)checksum);
    }

    @Override
    public int writeHeader(FileChannel channel) throws IOException {
        // This OutputStreamDataOutput is intentionally not closed because
        // closing it will close the FileChannel
        OutputStreamDataOutput out = new OutputStreamDataOutput(Channels.newOutputStream(channel));
        CodecUtil.writeHeader(out, TranslogStreams.TRANSLOG_CODEC, VERSION);
        return CodecUtil.headerLength(TranslogStreams.TRANSLOG_CODEC);
    }

    @Override
    public StreamInput openInput(File translogFile) throws IOException {
        try {
            InputStreamStreamInput in = new InputStreamStreamInput(new FileInputStream(translogFile));
            CodecUtil.checkHeader(new InputStreamDataInput(in), TranslogStreams.TRANSLOG_CODEC, VERSION, VERSION);
            return in;
        } catch (EOFException e) {
            throw new TruncatedTranslogException("translog header truncated", e);
        } catch (IOException e) {
            throw new TranslogCorruptedException("translog header corrupted", e);
        }
    }
}
