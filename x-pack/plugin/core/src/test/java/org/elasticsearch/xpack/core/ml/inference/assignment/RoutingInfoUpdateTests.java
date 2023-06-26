/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;

public class RoutingInfoUpdateTests extends AbstractWireSerializingTestCase<RoutingInfoUpdate> {

    @Override
    protected Writeable.Reader<RoutingInfoUpdate> instanceReader() {
        return RoutingInfoUpdate::new;
    }

    @Override
    protected RoutingInfoUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected RoutingInfoUpdate mutateInstance(RoutingInfoUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static RoutingInfoUpdate randomInstance() {
        if (randomBoolean()) {
            return RoutingInfoUpdate.updateNumberOfAllocations(randomIntBetween(1, Integer.MAX_VALUE));
        } else {
            return RoutingInfoUpdate.updateStateAndReason(RoutingStateAndReasonTests.randomInstance());
        }
    }

    public void testApply_GivenUpdatingNumberOfAllocations() {
        RoutingInfo updatedRoutingInfo = RoutingInfoUpdate.updateNumberOfAllocations(4)
            .apply(new RoutingInfo(3, 5, RoutingState.STARTED, "some text"));
        assertThat(updatedRoutingInfo, equalTo(new RoutingInfo(4, 5, RoutingState.STARTED, "some text")));
    }

    public void testApply_GivenUpdatingStateAndReason() {
        RoutingInfo updatedRoutingInfo = RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STOPPING, "test"))
            .apply(new RoutingInfo(3, 5, RoutingState.STARTED, ""));
        assertThat(updatedRoutingInfo, equalTo(new RoutingInfo(3, 5, RoutingState.STOPPING, "test")));
    }
}
