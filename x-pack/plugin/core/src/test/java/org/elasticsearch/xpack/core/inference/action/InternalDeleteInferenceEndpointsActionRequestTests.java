/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class InternalDeleteInferenceEndpointsActionRequestTests extends AbstractBWCWireSerializationTestCase<
    InternalDeleteInferenceEndpointsAction.Request> {

    @Override
    protected InternalDeleteInferenceEndpointsAction.Request mutateInstanceForVersion(
        InternalDeleteInferenceEndpointsAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<InternalDeleteInferenceEndpointsAction.Request> instanceReader() {
        return InternalDeleteInferenceEndpointsAction.Request::new;
    }

    @Override
    protected InternalDeleteInferenceEndpointsAction.Request createTestInstance() {
        var ids = new HashSet<String>();
        int count = randomIntBetween(1, 5);
        for (int i = 0; i < count; i++) {
            ids.add(randomAlphaOfLength(10));
        }
        return new InternalDeleteInferenceEndpointsAction.Request(ids, randomTimeValue());
    }

    @Override
    protected InternalDeleteInferenceEndpointsAction.Request mutateInstance(InternalDeleteInferenceEndpointsAction.Request instance)
        throws IOException {
        var newIds = new HashSet<>(instance.getInferenceEntityIds());
        newIds.add(randomAlphaOfLength(10));
        return new InternalDeleteInferenceEndpointsAction.Request(newIds, instance.masterNodeTimeout());
    }

    public void testRequestHoldsExpectedIds() {
        var ids = Set.of("id-1", "id-2", "id-3");
        var request = new InternalDeleteInferenceEndpointsAction.Request(ids, TEST_REQUEST_TIMEOUT);
        assertThat(request.getInferenceEntityIds(), is(ids));
    }
}
