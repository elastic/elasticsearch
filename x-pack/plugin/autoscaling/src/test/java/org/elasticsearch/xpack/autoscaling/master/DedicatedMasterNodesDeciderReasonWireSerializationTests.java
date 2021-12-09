/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.master;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DedicatedMasterNodesDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason> {
    @Override
    protected Writeable.Reader<DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason> instanceReader() {
        return DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason::new;
    }

    @Override
    protected DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason createTestInstance() {
        return new DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason(
            randomAlphaOfLength(20),
            randomIntBetween(0, 200),
            randomLongBetween(1, 1 << 12)
        );
    }

    @Override
    protected DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason mutateInstance(
        DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason instance
    ) {
        return new DedicatedMasterNodesDeciderService.DedicatedMasterNodesReason(
            instance.summary(),
            randomValueOtherThan(instance.getHotAndContentNodes(), () -> randomIntBetween(0, 200)),
            instance.getTotalHotAndContentNodesMemoryBytes()
        );
    }
}
