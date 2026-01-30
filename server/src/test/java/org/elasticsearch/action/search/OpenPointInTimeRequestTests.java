/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

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
}
