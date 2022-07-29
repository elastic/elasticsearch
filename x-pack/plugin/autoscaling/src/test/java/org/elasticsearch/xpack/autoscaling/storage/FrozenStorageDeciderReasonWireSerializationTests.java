/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class FrozenStorageDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    FrozenStorageDeciderService.FrozenReason> {
    @Override
    protected Writeable.Reader<FrozenStorageDeciderService.FrozenReason> instanceReader() {
        return FrozenStorageDeciderService.FrozenReason::new;
    }

    @Override
    protected FrozenStorageDeciderService.FrozenReason mutateInstance(FrozenStorageDeciderService.FrozenReason instance) {
        return new FrozenStorageDeciderService.FrozenReason(
            randomValueOtherThan(instance.totalDataSetSize(), () -> randomLongBetween(0, Long.MAX_VALUE))
        );
    }

    @Override
    protected FrozenStorageDeciderService.FrozenReason createTestInstance() {
        return new FrozenStorageDeciderService.FrozenReason(randomLongBetween(0, Long.MAX_VALUE));
    }
}
