/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;

public class ServiceAccountInfoTests extends AbstractWireSerializingTestCase<ServiceAccountInfo> {

    @Override
    protected Writeable.Reader<ServiceAccountInfo> instanceReader() {
        return ServiceAccountInfo::new;
    }

    @Override
    protected ServiceAccountInfo createTestInstance() {
        if (randomBoolean()) {
            final String principal = randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8);
            return ServiceAccountInfo.builtIn(principal, randomRoleDescriptor(principal));
        } else {
            final String principal = "custom/" + randomAlphaOfLengthBetween(3, 8);
            return ServiceAccountInfo.managed(
                principal,
                randomSubsetOf(randomIntBetween(0, 3), "role-a", "role-b", "role-c"),
                randomBoolean()
            );
        }
    }

    @Override
    protected ServiceAccountInfo mutateInstance(ServiceAccountInfo instance) {
        if (instance.isManaged()) {
            return ServiceAccountInfo.builtIn(instance.getPrincipal(), randomRoleDescriptor(instance.getPrincipal()));
        } else {
            return ServiceAccountInfo.managed(instance.getPrincipal(), List.of("mutated-role"), true);
        }
    }

    private static RoleDescriptor randomRoleDescriptor(String name) {
        return new RoleDescriptor(name, new String[] { "monitor" }, null, null);
    }
}
