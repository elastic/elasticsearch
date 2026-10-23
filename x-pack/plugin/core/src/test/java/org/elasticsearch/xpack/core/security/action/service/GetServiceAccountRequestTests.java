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
        return new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull(), randomType(), randomBoolean());
    }

    @Override
    protected GetServiceAccountRequest mutateInstance(GetServiceAccountRequest instance) {
        return switch (between(0, 3)) {
            case 0 -> new GetServiceAccountRequest(
                randomValueOtherThan(instance.getNamespace(), GetServiceAccountRequestTests::randomNameOrNull),
                instance.getServiceName(),
                instance.getType(),
                instance.withProfileUid()
            );
            case 1 -> new GetServiceAccountRequest(
                instance.getNamespace(),
                randomValueOtherThan(instance.getServiceName(), GetServiceAccountRequestTests::randomNameOrNull),
                instance.getType(),
                instance.withProfileUid()
            );
            case 2 -> new GetServiceAccountRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                randomValueOtherThan(instance.getType(), GetServiceAccountRequestTests::randomType),
                instance.withProfileUid()
            );
            case 3 -> new GetServiceAccountRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                instance.getType(),
                instance.withProfileUid() == false
            );
            default -> throw new AssertionError("between(0, 3) returned something outside its own bounds");
        };
    }

    public void testProfileUidsAreNotAskedForByDefault() {
        assertFalse(new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull()).withProfileUid());
        assertFalse(new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull(), randomType()).withProfileUid());
    }

    /**
     * A node that does not know attribution has no authors to look profiles up for, so the flag is dropped rather
     * than the request refused: the reply is the same accounts without the fields the flag would have enriched.
     */
    public void testTheProfileUidFlagIsDroppedForNodesThatDoNotKnowAttribution() throws IOException {
        final GetServiceAccountRequest request = new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull(), randomType(), true);
        final TransportVersion beforeAttribution = TransportVersionUtils.getPreviousVersion(
            ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_ATTRIBUTION
        );
        assertTrue(beforeAttribution.supports(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO));
        assertThat(
            copyInstance(request, beforeAttribution),
            equalTo(new GetServiceAccountRequest(request.getNamespace(), request.getServiceName(), request.getType(), false))
        );
    }

    public void testDefaultsToBuiltInAccountsOnly() {
        assertThat(
            new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull()).getType(),
            equalTo(EnumSet.of(ServiceAccountType.BUILT_IN))
        );
    }

    public void testRequestForBuiltInAccountsStillSerializesToNodesWithoutUserManagedAccounts() throws IOException {
        final GetServiceAccountRequest request = new GetServiceAccountRequest(randomNameOrNull(), randomNameOrNull());
        assertThat(copyInstance(request, beforeUserManagedAccountInfo()), equalTo(request));
    }

    public void testRequestForUserManagedAccountsRefusesToSerializeToNodesWithoutThem() {
        for (EnumSet<ServiceAccountType> type : List.of(
            EnumSet.of(ServiceAccountType.USER_MANAGED),
            EnumSet.allOf(ServiceAccountType.class)
        )) {
            final GetServiceAccountRequest request = new GetServiceAccountRequest(null, null, type);
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> copyInstance(request, beforeUserManagedAccountInfo())
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "cannot ask a node that does not support user-managed service accounts for accounts of type ["
                        + type.stream().map(ServiceAccountType::value).collect(Collectors.joining(", "))
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

    private static EnumSet<ServiceAccountType> randomType() {
        return randomFrom(
            EnumSet.of(ServiceAccountType.BUILT_IN),
            EnumSet.of(ServiceAccountType.USER_MANAGED),
            EnumSet.allOf(ServiceAccountType.class)
        );
    }
}
