/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class OpenPointInTimeRequestTests extends AbstractWireSerializingTestCase<OpenPointInTimeRequest> {
    @Override
    protected Writeable.Reader<OpenPointInTimeRequest> instanceReader() {
        return OpenPointInTimeRequest::new;
    }

    @Override
    protected OpenPointInTimeRequest createTestInstance() {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest("index-1", "index-2");
        request.keepAlive(TimeValue.timeValueSeconds(randomIntBetween(1, 1000)));
        if (randomBoolean()) {
            request.maxConcurrentShardRequests(randomIntBetween(1, 10));
        } else {
            assertThat(request.maxConcurrentShardRequests(), equalTo(5));
        }
        if (randomBoolean()) {
            request.preference(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            request.routing(randomAlphaOfLength(10));
        }
        return request;
    }

    @Override
    protected OpenPointInTimeRequest mutateInstance(OpenPointInTimeRequest in) throws IOException {
        return switch (between(0, 4)) {
            case 0 -> {
                OpenPointInTimeRequest request = new OpenPointInTimeRequest("new-index");
                request.maxConcurrentShardRequests(in.maxConcurrentShardRequests());
                request.keepAlive(in.keepAlive());
                request.preference(in.preference());
                request.routing(in.routing());
                yield request;
            }
            case 1 -> {
                OpenPointInTimeRequest request = new OpenPointInTimeRequest(in.indices());
                request.maxConcurrentShardRequests(in.maxConcurrentShardRequests() + between(1, 10));
                request.keepAlive(in.keepAlive());
                request.preference(in.preference());
                request.routing(in.routing());
                yield request;
            }
            case 2 -> {
                OpenPointInTimeRequest request = new OpenPointInTimeRequest(in.indices());
                request.maxConcurrentShardRequests(in.maxConcurrentShardRequests());
                request.keepAlive(TimeValue.timeValueSeconds(between(2000, 5000)));
                request.preference(in.preference());
                request.routing(in.routing());
                yield request;
            }
            case 3 -> {
                OpenPointInTimeRequest request = new OpenPointInTimeRequest(in.indices());
                request.maxConcurrentShardRequests(in.maxConcurrentShardRequests());
                request.keepAlive(in.keepAlive());
                request.preference(randomAlphaOfLength(5));
                request.routing(in.routing());
                yield request;
            }
            case 4 -> {
                OpenPointInTimeRequest request = new OpenPointInTimeRequest(in.indices());
                request.maxConcurrentShardRequests(in.maxConcurrentShardRequests());
                request.keepAlive(in.keepAlive());
                request.preference(in.preference());
                request.routing(randomAlphaOfLength(5));
                yield request;
            }
            default -> throw new AssertionError("Unknown option");
        };
    }

    public void testUseDefaultConcurrentForOldVersion() throws Exception {
        TransportVersion previousVersion = TransportVersionUtils.getPreviousVersion(TransportVersion.V_8_500_016);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            TransportVersion version = TransportVersionUtils.randomVersionBetween(random(), TransportVersion.V_8_0_0, previousVersion);
            output.setTransportVersion(version);
            OpenPointInTimeRequest original = createTestInstance();
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                in.setTransportVersion(version);
                OpenPointInTimeRequest copy = new OpenPointInTimeRequest(in);
                assertThat(copy.maxConcurrentShardRequests(), equalTo(5));
            }
        }
    }
}
