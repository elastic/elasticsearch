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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ServiceAccountInfoTests extends AbstractWireSerializingTestCase<ServiceAccountInfo> {

    @Override
    protected Writeable.Reader<ServiceAccountInfo> instanceReader() {
        return ServiceAccountInfo::readFrom;
    }

    @Override
    protected ServiceAccountInfo createTestInstance() {
        return randomBoolean() ? randomBuiltIn() : randomUserManaged();
    }

    @Override
    protected ServiceAccountInfo mutateInstance(ServiceAccountInfo instance) {
        return switch (instance) {
            // Mutating within a kind as well as across kinds, so that the fields only one kind carries take part.
            case ServiceAccountInfo.BuiltIn builtIn -> randomFrom(
                new ServiceAccountInfo.BuiltIn(randomValueOtherThan(builtIn.principal(), this::randomPrincipal), builtIn.roleDescriptor()),
                new ServiceAccountInfo.BuiltIn(builtIn.principal(), roleDescriptorWithCluster(builtIn.principal(), "manage_security")),
                randomUserManaged()
            );
            case ServiceAccountInfo.UserManaged userManaged -> randomFrom(
                new ServiceAccountInfo.UserManaged(
                    randomValueOtherThan(userManaged.principal(), this::randomPrincipal),
                    userManaged.roles(),
                    userManaged.enabled()
                ),
                new ServiceAccountInfo.UserManaged(
                    userManaged.principal(),
                    randomValueOtherThan(userManaged.roles(), () -> randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8))),
                    userManaged.enabled()
                ),
                new ServiceAccountInfo.UserManaged(userManaged.principal(), userManaged.roles(), userManaged.enabled() == false),
                randomBuiltIn()
            );
        };
    }

    public void testManagedByNamesTheKind() {
        assertThat(randomBuiltIn().managedBy(), equalTo(ServiceAccountManagedBy.ELASTIC));
        assertThat(randomUserManaged().managedBy(), equalTo(ServiceAccountManagedBy.USER));
    }

    public void testRolesAreCopiedOnConstruction() {
        final List<String> roles = new ArrayList<>(List.of("role-a"));
        final ServiceAccountInfo.UserManaged info = new ServiceAccountInfo.UserManaged("my-team/worker", roles, true);
        roles.add("role-b");
        assertThat(info.roles(), equalTo(List.of("role-a")));
    }

    public void testBuiltInAccountsStillSerializeToNodesWithoutUserManagedAccounts() throws IOException {
        final ServiceAccountInfo.BuiltIn builtIn = randomBuiltIn();
        assertThat(copyInstance(builtIn, beforeUserManagedAccountInfo()), equalTo(builtIn));
    }

    public void testUserManagedAccountsRefuseToSerializeToNodesWithoutThem() {
        final ServiceAccountInfo.UserManaged userManaged = randomUserManaged();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> copyInstance(userManaged, beforeUserManagedAccountInfo())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "cannot send information about the user-managed service account ["
                    + userManaged.principal()
                    + "] to a node that does not support user-managed service accounts"
            )
        );
    }

    private static TransportVersion beforeUserManagedAccountInfo() {
        return TransportVersionUtils.getPreviousVersion(ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_INFO);
    }

    private ServiceAccountInfo.BuiltIn randomBuiltIn() {
        final String principal = randomPrincipal();
        return new ServiceAccountInfo.BuiltIn(principal, roleDescriptorWithCluster(principal, "monitor"));
    }

    private ServiceAccountInfo.UserManaged randomUserManaged() {
        return new ServiceAccountInfo.UserManaged(
            randomPrincipal(),
            randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8)),
            randomBoolean()
        );
    }

    private String randomPrincipal() {
        return randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
    }

    private static RoleDescriptor roleDescriptorWithCluster(String name, String clusterPrivilege) {
        return new RoleDescriptor(name, new String[] { clusterPrivilege }, null, null);
    }
}
