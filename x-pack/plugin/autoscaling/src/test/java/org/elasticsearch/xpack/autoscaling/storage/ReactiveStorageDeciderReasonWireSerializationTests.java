/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class ReactiveStorageDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    ReactiveStorageDeciderService.ReactiveReason> {
    @Override
    protected Writeable.Reader<ReactiveStorageDeciderService.ReactiveReason> instanceReader() {
        return ReactiveStorageDeciderService.ReactiveReason::new;
    }

    @Override
    protected ReactiveStorageDeciderService.ReactiveReason mutateInstance(ReactiveStorageDeciderService.ReactiveReason instance) {
        switch (between(0, 2)) {
            case 0:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    randomValueOtherThan(instance.summary(), () -> randomAlphaOfLength(10)),
                    instance.unassigned(),
                    instance.assigned()
                );
            case 1:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    randomValueOtherThan(instance.unassigned(), ESTestCase::randomNonNegativeLong),
                    instance.assigned()
                );
            case 2:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    randomValueOtherThan(instance.assigned(), ESTestCase::randomNonNegativeLong)
                );
            default:
                fail("unexpected");
        }
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ReactiveStorageDeciderService.ReactiveReason createTestInstance() {
        return new ReactiveStorageDeciderService.ReactiveReason(randomAlphaOfLength(10), randomNonNegativeLong(), randomNonNegativeLong());
    }
}
