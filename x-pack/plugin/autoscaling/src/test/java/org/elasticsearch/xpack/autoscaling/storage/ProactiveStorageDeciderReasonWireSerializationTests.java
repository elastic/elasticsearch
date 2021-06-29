/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class ProactiveStorageDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    ProactiveStorageDeciderService.ProactiveReason> {
    @Override
    protected Writeable.Reader<ProactiveStorageDeciderService.ProactiveReason> instanceReader() {
        return ProactiveStorageDeciderService.ProactiveReason::new;
    }

    @Override
    protected ProactiveStorageDeciderService.ProactiveReason mutateInstance(ProactiveStorageDeciderService.ProactiveReason instance) {
        switch (between(0, 4)) {
            case 0:
                return new ProactiveStorageDeciderService.ProactiveReason(
                    randomValueOtherThan(instance.summary(), () -> randomAlphaOfLength(10)),
                    instance.unassigned(),
                    instance.assigned(),
                    instance.forecasted(),
                    instance.forecastWindow()
                );
            case 1:
                return new ProactiveStorageDeciderService.ProactiveReason(
                    instance.summary(),
                    randomLongOtherThan(instance.unassigned()),
                    instance.assigned(),
                    instance.forecasted(),
                    instance.forecastWindow()
                );
            case 2:
                return new ProactiveStorageDeciderService.ProactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    randomLongOtherThan(instance.assigned()),
                    instance.forecasted(),
                    instance.forecastWindow()
                );
            case 3:
                return new ProactiveStorageDeciderService.ProactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.assigned(),
                    randomLongOtherThan(instance.forecasted()),
                    instance.forecastWindow()
                );
            case 4:
                return new ProactiveStorageDeciderService.ProactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.assigned(),
                    instance.forecasted(),
                    randomValueOtherThan(instance.forecastWindow(), () -> new TimeValue(randomNonNegativeLong()))
                );
            default:
                fail("unexpected");
        }
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private long randomLongOtherThan(long assigned) {
        return randomValueOtherThan(assigned, ESTestCase::randomNonNegativeLong);
    }

    @Override
    protected ProactiveStorageDeciderService.ProactiveReason createTestInstance() {
        return new ProactiveStorageDeciderService.ProactiveReason(
            randomAlphaOfLength(10),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            new TimeValue(randomNonNegativeLong())
        );
    }
}
