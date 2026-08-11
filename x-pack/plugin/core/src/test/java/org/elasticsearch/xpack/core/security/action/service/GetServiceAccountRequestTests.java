/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.EnumSet;

public class GetServiceAccountRequestTests extends AbstractWireSerializingTestCase<GetServiceAccountRequest> {

    @Override
    protected Writeable.Reader<GetServiceAccountRequest> instanceReader() {
        return GetServiceAccountRequest::new;
    }

    @Override
    protected GetServiceAccountRequest createTestInstance() {
        return new GetServiceAccountRequest(
            randomFrom(randomAlphaOfLengthBetween(3, 8), null),
            randomFrom(randomAlphaOfLengthBetween(3, 8), null),
            randomManagedBy()
        );
    }

    @Override
    protected GetServiceAccountRequest mutateInstance(GetServiceAccountRequest instance) {
        switch (randomInt(2)) {
            case 0 -> {
                return new GetServiceAccountRequest(
                    randomValueOtherThan(instance.getNamespace(), () -> randomFrom(randomAlphaOfLengthBetween(3, 8), null)),
                    instance.getServiceName(),
                    instance.getManagedBy()
                );
            }
            case 1 -> {
                return new GetServiceAccountRequest(
                    instance.getNamespace(),
                    randomValueOtherThan(instance.getServiceName(), () -> randomFrom(randomAlphaOfLengthBetween(3, 8), null)),
                    instance.getManagedBy()
                );
            }
            default -> {
                return new GetServiceAccountRequest(
                    instance.getNamespace(),
                    instance.getServiceName(),
                    randomValueOtherThan(instance.getManagedBy(), GetServiceAccountRequestTests::randomManagedBy)
                );
            }
        }
    }

    private static EnumSet<ServiceAccountManagedBy> randomManagedBy() {
        return randomFrom(
            EnumSet.of(ServiceAccountManagedBy.ELASTIC),
            EnumSet.of(ServiceAccountManagedBy.USER),
            EnumSet.allOf(ServiceAccountManagedBy.class)
        );
    }
}
