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
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

/**
 * Version 1 of the translog file format. Writes a header to identify the
 * format, also writes checksums for each operation
 */
public class ChecksummedTranslogStream implements TranslogStream {

    public static final int VERSION = 1;

    private final InputStreamStreamInput in;
    private final boolean fileExists;

    ChecksummedTranslogStream(InputStreamStreamInput in, boolean fileExists) {
        this.in = in;
        // This could be a new file, in which case we can ignore reading and
        // verifying the header
        this.fileExists = fileExists;
        if (fileExists) {
            // The header must be read to advance the input stream
            readAndVerifyHeader();
        }
    }

    private void readAndVerifyHeader() {
        assert this.in != null : "headers are only for translog files read from disk, not streaming operations";
        try {
            CodecUtil.checkHeader(new InputStreamDataInput(this.in), TranslogStreams.TRANSLOG_CODEC, VERSION, VERSION);
        } catch (IOException e) {
            throw new TranslogCorruptedException("translog header corrupted", e);
        }
    }

    public Translog.Operation read() throws IOException {
        if (this.fileExists == false) {
            throw new IOException("translog file does not exist");
        }
        assert this.fileExists : "cannot read from a stream for a file that does not exist";
        in.readInt(); // ignored operation size
        return this.read(in);
    }

    private void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    @Override
    public Translog.Operation read(StreamInput inStream) throws IOException {
        // This BufferedChecksumStreamInput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(inStream);
        Translog.Operation operation;
        try {
            Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
            operation = TranslogStreams.newOperationFromType(type);
            operation.readFrom(in);
        } catch (AssertionError|Exception e) {
            throw new TranslogCorruptedException("translog corruption while reading from stream", e);
        }
        verifyChecksum(in);
        return operation;
    }

    @Override
    public Translog.Source readSource(byte[] data) throws IOException {
        StreamInput nonChecksummingIn = new BytesStreamInput(data, false);
        BufferedChecksumStreamInput in;
        Translog.Source source;
        try {
            // the size header, not used and not part of the checksum
            // because it is computed after the operation is written
            nonChecksummingIn.readInt();
            // This BufferedChecksumStreamInput remains unclosed on purpose,
            // because closing it closes the underlying stream, which we don't
            // want to do here.
            in = new BufferedChecksumStreamInput(nonChecksummingIn);
            Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
            Translog.Operation operation = TranslogStreams.newOperationFromType(type);
            source = operation.readSource(in);
        } catch (AssertionError|Exception e) {
            throw new TranslogCorruptedException("translog corruption while reading from byte array", e);
        }
        verifyChecksum(in);
        return source;
    }

    @Override
    public void write(StreamOutput outStream, Translog.Operation op) throws IOException {
        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(outStream);
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
    public void close() throws IOException {
        this.in.close();
    }
}
