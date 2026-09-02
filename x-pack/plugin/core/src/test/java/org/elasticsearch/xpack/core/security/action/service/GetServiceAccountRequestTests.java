/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountRequestTests extends AbstractWireSerializingTestCase<GetServiceAccountRequest> {

    @Override
    protected Writeable.Reader<GetServiceAccountRequest> instanceReader() {
        return GetServiceAccountRequest::new;
    }

    @Override
    protected GetServiceAccountRequest createTestInstance() {
        return new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull(), randomManagedBy());
    }

    @Override
    protected GetServiceAccountRequest mutateInstance(GetServiceAccountRequest instance) {
        return switch (between(0, 2)) {
            case 0 -> new GetServiceAccountRequest(
                randomValueOtherThan(instance.getNamespace(), GetServiceAccountRequestTests::randomNameOrNull),
                instance.getServiceName(),
                instance.getManagedBy()
            );
            case 1 -> new GetServiceAccountRequest(
                instance.getNamespace(),
                randomValueOtherThan(instance.getServiceName(), GetServiceAccountRequestTests::randomNameOrNull),
                instance.getManagedBy()
            );
            case 2 -> new GetServiceAccountRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                randomValueOtherThan(instance.getManagedBy(), GetServiceAccountRequestTests::randomManagedBy)
            );
            default -> throw new AssertionError("between(0, 2) returned something outside its own bounds");
        };
    }

    public void testDefaultsToBuiltInAccountsOnly() {
        assertThat(
            new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull()).getManagedBy(),
            equalTo(EnumSet.of(ServiceAccountManagedBy.ELASTIC))
        );
    }

    public void testRequestForBuiltInAccountsStillSerializesToNodesWithoutUserManagedAccounts() throws IOException {
        final GetServiceAccountRequest request = new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull());
        assertThat(copyInstance(request, beforeUserManagedAccountInfo()), equalTo(request));
    }

    public void testRequestForUserManagedAccountsRefusesToSerializeToNodesWithoutThem() {
        for (EnumSet<ServiceAccountManagedBy> managedBy : List.of(
            EnumSet.of(ServiceAccountManagedBy.USER),
            EnumSet.allOf(ServiceAccountManagedBy.class)
        )) {
            final GetServiceAccountRequest request = new GetServiceAccountRequest(null, null, managedBy);
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> copyInstance(request, beforeUserManagedAccountInfo())
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "cannot ask a node that does not support user-managed service accounts for accounts managed by ["
                        + managedBy.stream().map(ServiceAccountManagedBy::value).collect(Collectors.joining(", "))
                        + "]"
                )
            );
        }
    }

    private static TransportVersion beforeUserManagedAccountInfo() {
        return TransportVersionUtils.getPreviousVersion(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO);
    }

    private static String randomNameOrNull() {
        return randomFrom(randomAlphaOfLengthBetween(3, 8), null);
    }

    private static EnumSet<ServiceAccountManagedBy> randomManagedBy() {
        return randomFrom(
            EnumSet.of(ServiceAccountManagedBy.ELASTIC),
            EnumSet.of(ServiceAccountManagedBy.USER),
            EnumSet.allOf(ServiceAccountManagedBy.class)
        );
    }
}
