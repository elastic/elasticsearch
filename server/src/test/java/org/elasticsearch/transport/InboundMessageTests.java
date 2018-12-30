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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class InboundMessageTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());

    public void testReadRequest() throws IOException {
        String[] features = {};
        String value = randomAlphaOfLength(10);
        Message message = new Message(value);
        String action = randomAlphaOfLength(10);
        long requestId = randomLong();
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        threadContext.putHeader("header", "header_value");
        OutboundMessage.Request request = new OutboundMessage.Request(threadContext, features, message, Version.CURRENT, action, requestId,
            isHandshake, compress);
        BytesReference reference;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            reference = request.serialize(streamOutput);
        }
        // Check that the thread context is not deleted.
        assertEquals("header_value", threadContext.getHeader("header"));

        threadContext.stashContext();
        threadContext.putHeader("header", "header_value2");

        InboundMessage.Reader reader = new InboundMessage.Reader(Version.CURRENT, registry, threadContext);
        InboundMessage inboundMessage = reader.deserialize(reference.slice(6, reference.length() - 6));
        // Check that deserialize does not overwrite current thread context.
        assertEquals("header_value2", threadContext.getHeader("header"));
        inboundMessage.getStoredContext().restore();
        assertEquals("header_value", threadContext.getHeader("header"));

    }

    private static final class Message extends TransportMessage {

        public String value;

        private Message() {
        }

        private Message(String value) {
            this.value = value;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }
}
