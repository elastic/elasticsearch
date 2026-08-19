/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchTransportServiceTests extends ESTestCase {

    /**
     * Verifies that the free-context executor concurrency is half the allocated processors, with a minimum of 1.
     * A node with many processors must be able to drain free-context requests concurrently; with a single drainer
     * the queue can grow unboundedly under burst traffic and exhaust the heap.
     */
    public void testFreeContextConcurrency() {
        // single processor → minimum of 1
        assertThat(SearchTransportService.freeContextConcurrency(settings(1)), equalTo(1));
        // two processors → 1
        assertThat(SearchTransportService.freeContextConcurrency(settings(2)), equalTo(1));
        // four processors → 2
        assertThat(SearchTransportService.freeContextConcurrency(settings(4)), equalTo(2));
        // eight processors → 4
        assertThat(SearchTransportService.freeContextConcurrency(settings(8)), equalTo(4));
        // default settings use Runtime.availableProcessors() → at least 1
        assertThat(SearchTransportService.freeContextConcurrency(Settings.EMPTY), greaterThanOrEqualTo(1));
    }

    private static Settings settings(int processors) {
        return Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), processors).build();
    }

    /**
     * Verifies that {@link SearchTransportService#countingRequest} and
     * {@link SearchTransportService#countingReader} report the same byte count for an identical payload,
     * ensuring that the send-side and receive-side accounting are consistent.
     */
    public void testRequestAndReaderBytesAgree() throws IOException {
        int intVal = randomInt();
        String strVal = randomAlphaOfLengthBetween(1, 64);
        long longVal = randomLong();
        boolean boolVal = randomBoolean();
        String optStr = randomBoolean() ? null : randomAlphaOfLength(10);

        AbstractTransportRequest inner = new AbstractTransportRequest() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(intVal);
                out.writeString(strVal);
                out.writeLong(longVal);
                out.writeBoolean(boolVal);
                out.writeOptionalString(optStr);
            }
        };

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());

        long[] written = new long[1];
        SearchTransportService.countingRequest(inner, b -> written[0] = b).writeTo(out);

        long[] read = new long[1];
        Writeable.Reader<String> countingReader = SearchTransportService.countingReader(in -> {
            in.readInt();
            in.readString();
            in.readLong();
            in.readBoolean();
            in.readOptionalString();
            return "ok";
        }, b -> read[0] = b);

        var streamInput = out.bytes().streamInput();
        streamInput.setTransportVersion(TransportVersion.current());
        countingReader.read(streamInput);

        assertThat(written[0], equalTo(read[0]));
        assertThat(written[0], greaterThan(0L));
    }

    /**
     * Verifies that {@link SearchTransportService#countingRequest} counts the exact number of bytes
     * written by the inner request's {@code writeTo}.
     */
    public void testCountingRequestCountsBytes() throws IOException {
        AbstractTransportRequest request = new AbstractTransportRequest() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(1);
                out.writeInt(2);
            }
        };

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());

        long[] counted = new long[1];
        SearchTransportService.countingRequest(request, b -> counted[0] = b).writeTo(out);

        // Two ints = 8 bytes
        assertThat(counted[0], equalTo(8L));
        assertThat(out.size(), equalTo(8));
    }
}
