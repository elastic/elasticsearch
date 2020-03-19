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
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;

public class InboundPipelineTests extends ESTestCase {

    private static final int BYTE_THRESHOLD = 128 * 1024;
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    public void testPipelineHandling() throws IOException {
        // TODO: Add header tests
        final InboundDecoder decoder = new InboundDecoder();
        final InboundAggregator aggregator = new InboundAggregator();
        final List<MessageData> expected = new ArrayList<>();
        final List<MessageData> actual = new ArrayList<>();
        final List<ReleasableBytesReference> toRelease = new ArrayList<>();
        final BiConsumer<TcpChannel, AggregatedMessage> biConsumer = (c, m) -> {
            try {
                final Header header = m.getHeader();
                final MessageData actualData;
                final boolean isRequest = header.isRequest();
                final long requestId = header.getRequestId();
                final boolean isCompressed = header.isCompressed();
                if (isRequest) {
                    final TestRequest request = new TestRequest(m.getContent().streamInput());
                    actualData = new MessageData(requestId, isRequest, isCompressed, header.getActionName(), request.value);
                } else {
                    final TestResponse response = new TestResponse(m.getContent().streamInput());
                    actualData = new MessageData(requestId, isRequest, isCompressed, null, response.value);
                }
                actual.add(actualData);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        final InboundPipeline pipeline = new InboundPipeline(decoder, aggregator, biConsumer);

        final int iterations = randomIntBetween(100, 500);
        long totalMessages = 0;

        for (int i = 0; i < iterations; ++i) {
            actual.clear();
            expected.clear();
            toRelease.clear();
            try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                while (streamOutput.size() < BYTE_THRESHOLD) {
                    String actionName = "actionName";
                    final String value = randomAlphaOfLength(randomIntBetween(10, 200));
                    final boolean isRequest = randomBoolean();
                    final boolean isCompressed = randomBoolean();
                    final long requestId = totalMessages++;

                    final MessageData messageData;

                    OutboundMessage message;
                    if (isRequest) {
                        messageData = new MessageData(requestId, true, isCompressed, actionName, value);
                        message = new OutboundMessage.Request(threadContext, new TestRequest(value),
                            Version.CURRENT, actionName, requestId, false, isCompressed);
                    } else {
                        messageData = new MessageData(requestId, false, isCompressed, null, value);
                        message = new OutboundMessage.Response(threadContext, new TestResponse(value),
                            Version.CURRENT, requestId, false, isCompressed);
                    }

                    expected.add(messageData);
                    final BytesReference reference = message.serialize(new BytesStreamOutput());
                    Streams.copy(reference.streamInput(), streamOutput);
                }

                final BytesReference networkBytes = streamOutput.bytes();
                int currentOffset = 0;
                while (currentOffset != networkBytes.length()) {
                    final int remainingBytes = networkBytes.length() - currentOffset;
                    final int bytesToRead = Math.min(randomIntBetween(1, 32 * 1024), remainingBytes);
                    final BytesReference slice = networkBytes.slice(currentOffset, bytesToRead);
                    try (ReleasableBytesReference reference = new ReleasableBytesReference(slice, () -> {})) {
                        toRelease.add(reference);
                        final int handled = pipeline.handleBytes(mock(TcpChannel.class), reference);
                        currentOffset += handled;
                    }
                }

                final int messages = expected.size();
                for (int j = 0; j < messages; ++j) {
                    final MessageData expectedMessage = expected.get(j);
                    final MessageData actualMessage = actual.get(j);
                    assertEquals(expectedMessage.requestId, actualMessage.requestId);
                    assertEquals(expectedMessage.isRequest, actualMessage.isRequest);
                    assertEquals(expectedMessage.isCompressed, actualMessage.isCompressed);
                    assertEquals(expectedMessage.value, actualMessage.value);
                    assertEquals(expectedMessage.actionName, actualMessage.actionName);
                }

                for (ReleasableBytesReference released : toRelease) {
                    assertEquals(0, released.refCount());
                }
            }
        }
    }

    private static class MessageData {

        private final long requestId;
        private final boolean isRequest;
        private final boolean isCompressed;
        private final String value;
        private final String actionName;

        private MessageData(long requestId, boolean isRequest, boolean isCompressed, String actionName, String value) {
            this.requestId = requestId;
            this.isRequest = isRequest;
            this.isCompressed = isCompressed;
            this.actionName = actionName;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageData that = (MessageData) o;
            return requestId == that.requestId &&
                isRequest == that.isRequest &&
                isCompressed == that.isCompressed &&
                Objects.equals(value, that.value) &&
                Objects.equals(actionName, that.actionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestId, isRequest, isCompressed, value, actionName);
        }
    }
}
