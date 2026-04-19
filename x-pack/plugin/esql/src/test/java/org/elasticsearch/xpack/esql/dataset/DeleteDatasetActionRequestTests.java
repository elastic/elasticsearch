/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dataset;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.dataset.DeleteDatasetAction.Request;

import java.util.Locale;

/**
 * Round-trip wire test for {@link Request} ({@code AcknowledgedRequest} subclass, transported
 * to the master).
 */
public class DeleteDatasetActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT));
        // Request implements IndicesRequest.Replaceable. indices() is set from the name on
        // the HTTP/serialization boundary; pre-seed it here so equals() is stable after round-trip.
        r.indices(r.name());
        return r;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        Request mutated = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, instance.name() + "_mutated");
        mutated.indices(mutated.name());
        return mutated;
    }
}
