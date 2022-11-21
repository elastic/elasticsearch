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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecuritySubjectAccessTests extends ESTestCase {

    public void testEncodeDecodeRoundTrip() throws IOException {
        final Set<RoleDescriptor> expectedRoleDescriptorsSet = Set.of(
            RoleDescriptorTests.randomRoleDescriptor(),
            RoleDescriptorTests.randomRoleDescriptor(),
            RoleDescriptorTests.randomRoleDescriptor()
        );
        final Collection<Set<RoleDescriptor>> expectedRoleDescriptors = List.of(expectedRoleDescriptorsSet);
        final RoleDescriptorsIntersection roleDescriptorsSets = new RoleDescriptorsIntersection(expectedRoleDescriptors);

        final Authentication expectedAuthentication = AuthenticationTestHelper.builder().build();
        final RemoteClusterSecuritySubjectAccess decoded = RemoteClusterSecuritySubjectAccess.decode(
            RemoteClusterSecuritySubjectAccess.encode(expectedAuthentication, roleDescriptorsSets)
        );

        final Authentication decodedAuthentication = decoded.authentication();
        final Set<RoleDescriptor> decodedRoleDescriptorsSet = decoded.authorization().roleDescriptorsSets().iterator().next();
        assertThat(decodedAuthentication, equalTo(expectedAuthentication));
        assertThat(decodedRoleDescriptorsSet, equalTo(expectedRoleDescriptorsSet));
    }

}
