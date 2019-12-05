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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InboundDecoderTests extends ESTestCase {

    private final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final Version version = Version.CURRENT;
    private final AtomicInteger releasedCount = new AtomicInteger(0);
    private final Releasable releasable = releasedCount::incrementAndGet;

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testDecode() throws IOException {
        String action = "test-request";
        boolean isCompressed = randomBoolean();
        long requestId = randomNonNegativeLong();
        OutboundMessage message;
        if (randomBoolean()) {
            message = new OutboundMessage.Request(threadPool.getThreadContext(), new TestRequest(randomAlphaOfLength(100)), version,
                action, requestId, false, isCompressed);
        } else {
            message = new OutboundMessage.Response(threadPool.getThreadContext(), new TestResponse(randomAlphaOfLength(100)), version,
                requestId, false, isCompressed);
        }

        final BytesReference bytes = message.serialize(new BytesStreamOutput());

        InboundAggregator aggregator = mock(InboundAggregator.class);
        InboundDecoder decoder = new InboundDecoder(aggregator);
        int bytesConsumed = decoder.handle(mock(TcpChannel.class), new ReleasableBytesReference(bytes, releasable));
        verify(aggregator).headerReceived(any(Header.class));
        assertEquals(TcpHeader.BYTES_REQUIRED_FOR_VERSION, bytesConsumed);

        final BytesReference bytes2 = bytes.slice(bytesConsumed, bytes.length() - bytesConsumed);
        int bytesConsumed2 = decoder.handle(mock(TcpChannel.class), new ReleasableBytesReference(bytes2, releasable));
        assertEquals(bytes.length() - TcpHeader.BYTES_REQUIRED_FOR_VERSION, bytesConsumed2);
        verify(aggregator, times(2)).contentReceived(any(TcpChannel.class), any(ReleasableBytesReference.class));
    }

    private static class TestRequest extends TransportRequest {

        String value;

        private TestRequest(String value) {
            this.value = value;
        }

        private TestRequest(StreamInput in) throws IOException {
            super(in);
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(value);
        }
    }

    private static class TestResponse extends TransportResponse {

        String value;

        private TestResponse(String value) {
            this.value = value;
        }

        private TestResponse(StreamInput in) throws IOException {
            super(in);
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }
}
