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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;

public class MessageSerializer {

    private static abstract class Message implements Writeable {

        private final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        protected final ThreadPool threadPool;
        protected final Version version;
        protected final long requestId;
        protected final Writeable message;
        protected final boolean compressMessage;
        protected byte status = 0;

        protected Message(ThreadPool threadPool, Version version, long requestId, Writeable message, boolean compressMessage) {
            this.threadPool = threadPool;
            this.version = version;
            this.requestId = requestId;
            this.message = message;
            this.compressMessage = compressMessage;
        }

        CompressibleBytesOutputStream initStream() throws IOException {
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

        BytesReference finish(CompressibleBytesOutputStream stream) throws IOException {
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
    }

    static class Request extends Message {

        private final String[] features;
        private final String action;


        Request(ThreadPool threadPool, String[] features, Writeable message, Version version, String action, long requestId,
                boolean compressMessage) {
            super(threadPool, version, requestId, message, compressMessage);
            this.features = features;
            this.action = action;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            CompressibleBytesOutputStream stream = initStream();
            if (version.onOrAfter(Version.V_6_3_0)) {
                stream.writeStringArray(features);
            }
            stream.writeString(action);

            BytesReference postamble = finish(stream);
        }
    }

    static class Response extends Message {

        private final Set<String> features;

        Response(ThreadPool threadPool, Set<String> features, Writeable message, Version version, String action, long requestId,
                 boolean compressMessage) {
            super(threadPool, version, requestId, message, compressMessage);
            this.features = features;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            CompressibleBytesOutputStream stream = initStream();
            stream.setFeatures(features);
            BytesReference postamble = finish(stream);
        }
    }

    private static boolean canCompress(Writeable message) {
        return message instanceof BytesTransportRequest == false;
    }
}
