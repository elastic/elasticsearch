/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;

import java.io.IOException;
import java.util.Set;

public class RemoteAccessUserAccessTests extends ESTestCase {

    public void testEncodeDecodeRoundtrip() throws IOException {
        final Set<RoleDescriptor> expectedRoleDescriptors = Set.of(
            RoleDescriptorTests.randomRoleDescriptor(),
            RoleDescriptorTests.randomRoleDescriptor(),
            RoleDescriptorTests.randomRoleDescriptor()
        );
        final Authentication expectedAuthentication = AuthenticationTestHelper.builder().build();

        // TODO
        // final RemoteAccessUserAccess decoded = RemoteAccessUserAccess.decode(
        // RemoteAccessUserAccess.encode(expectedAuthentication, new RoleDescriptorsIntersection(List.of(expectedRoleDescriptors)))
        // );
        //
        // assertThat(decoded.authentication(), equalTo(expectedAuthentication));
        // assertThat(
        // RemoteAccessUserAccess.parseRoleDescriptorsBytes(decoded.authorization().iterator().next()),
        // equalTo(expectedRoleDescriptors)
        // );
    }

}
