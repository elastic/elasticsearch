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
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;

public class MessageSerializer {

    private final ThreadPool threadPool = null;
    private final String[] features = new String[0];
    private final Writeable message = null;
    private final Version version = Version.CURRENT;
    private final String action = null;
    private final long requestId = -1;
    private final boolean compressMessage = false;
    private byte status = 0;


    public void request() throws IOException {
        CompressibleBytesOutputStream stream = preamble();
        if (version.onOrAfter(Version.V_6_3_0)) {
            stream.writeStringArray(features);
        }
        stream.writeString(action);

        BytesReference postamble = postamble(stream);
    }

    public void response() throws IOException {
        final boolean compress = compressMessage && canCompress(message);
        if (compress) {
            status = TransportStatus.setCompress(status);
        }

        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, compress);

        stream.setVersion(version);
        threadPool.getThreadContext().writeTo(stream);


        Set<String> features = null;
        stream.setFeatures(features);

        BytesReference postamble = postamble(stream);
    }

    private final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

    private CompressibleBytesOutputStream preamble() throws IOException {
        // only compress if asked and the request is not bytes. Otherwise only
        // the header part is compressed, and the "body" can't be extracted as compressed
        final boolean compress = compressMessage && canCompress(message);
        if (compress) {
            status = TransportStatus.setCompress(status);
        }

        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        final CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, compress);

        stream.setVersion(version);
        threadPool.getThreadContext().writeTo(stream);

        return stream;
    }

    private BytesReference postamble(CompressibleBytesOutputStream stream) throws IOException {
        final BytesReference zeroCopyBuffer;
        if (message instanceof BytesTransportRequest) { // what a shitty optimization - we should use a direct send method instead
            BytesTransportRequest bRequest = (BytesTransportRequest) message;
            bRequest.writeThin(stream);
            zeroCopyBuffer = bRequest.bytes;
        } else if (message instanceof RemoteTransportException) {
            stream.writeException((RemoteTransportException) message);
            zeroCopyBuffer = BytesArray.EMPTY;
        } else {
            message.writeTo(stream);
            zeroCopyBuffer = BytesArray.EMPTY;
        }
        // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
        // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
        // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
        // #validateRequest method. this might be a problem in deflate after all but it's important to write
        // the marker bytes.
        final BytesReference messageBody = stream.materializeBytes();
        final BytesReference header = buildHeader(requestId, status, stream.getVersion(), messageBody.length() + zeroCopyBuffer.length());
        return new CompositeBytesReference(header, messageBody, zeroCopyBuffer);
    }

    /**
     * Writes the Tcp message header into a bytes reference.
     *
     * @param requestId       the request ID
     * @param status          the request status
     * @param protocolVersion the protocol version used to serialize the data in the message
     * @param length          the payload length in bytes
     * @see TcpHeader
     */
    private BytesReference buildHeader(long requestId, byte status, Version protocolVersion, int length) throws IOException {
        try (BytesStreamOutput headerOutput = new BytesStreamOutput(TcpHeader.HEADER_SIZE)) {
            headerOutput.setVersion(protocolVersion);
            TcpHeader.writeHeader(headerOutput, requestId, status, protocolVersion, length);
            final BytesReference bytes = headerOutput.bytes();
            assert bytes.length() == TcpHeader.HEADER_SIZE : "header size mismatch expected: " + TcpHeader.HEADER_SIZE + " but was: "
                + bytes.length();
            return bytes;
        }
    }

    private static boolean canCompress(Writeable message) {
        return message instanceof BytesTransportRequest == false;
    }
}
