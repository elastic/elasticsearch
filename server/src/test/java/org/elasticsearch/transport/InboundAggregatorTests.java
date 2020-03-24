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
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class InboundAggregatorTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private InboundAggregator aggregator;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        aggregator = new InboundAggregator();
    }

    public void testCannotReceiveHeaderTwice() {
        long requestId = randomLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.setHeaders(new Tuple<>(Collections.emptyMap(), Collections.emptyMap()));
        aggregator.headerReceived(header);

        expectThrows(IllegalStateException.class, () -> aggregator.headerReceived(header));
    }

    public void testCannotReceiveContentWithoutHeader() throws IOException {
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            threadContext.writeTo(streamOutput);
            streamOutput.writeString("action_name");
            streamOutput.write(randomByteArrayOfLength(10));
            expectThrows(IllegalStateException.class, () -> {
                ReleasableBytesReference content = new ReleasableBytesReference(streamOutput.bytes(), () -> {});
                aggregator.aggregate(content);
            });
        }
    }

    public void testInboundAggregation() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.setHeaders(new Tuple<>(Collections.emptyMap(), Collections.emptyMap()));
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        InboundMessage aggregated = aggregator.aggregate(new ReleasableBytesReference(bytes, () -> {}));
        assertNull(aggregated);

        // Signal EOS
        aggregated = aggregator.finishAggregation();

        assertThat(aggregated, notNullValue());
        assertFalse(aggregated.isPing());
        assertTrue(aggregated.getHeader().isRequest());
        assertThat(aggregated.getHeader().getRequestId(), equalTo(requestId));
        assertThat(aggregated.getHeader().getVersion(), equalTo(Version.CURRENT));
    }

    public void testFinishAggregationWillFinishHeader() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        // Initiate Message
        aggregator.headerReceived(header);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            threadContext.writeTo(streamOutput);
            streamOutput.writeString("action_name");
            streamOutput.write(randomByteArrayOfLength(10));

            InboundMessage aggregated = aggregator.aggregate(new ReleasableBytesReference(streamOutput.bytes(), () -> {}));
            assertNull(aggregated);

            // Signal EOS
            aggregated = aggregator.finishAggregation();

            assertThat(aggregated, notNullValue());
            assertFalse(header.needsToReadVariableHeader());
            assertEquals("action_name", header.getActionName());
        }

    }
}
