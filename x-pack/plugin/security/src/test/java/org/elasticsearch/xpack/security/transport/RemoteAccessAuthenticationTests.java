/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class RemoteAccessAuthenticationTests extends ESTestCase {

    public void test() throws IOException {
        RoleDescriptor expected = RoleDescriptorTests.randomRoleDescriptor();
        var encoded = RemoteAccessAuthentication.encode(
            AuthenticationTestHelper.builder().build(),
            new RoleDescriptorsIntersection(List.of(Set.of(expected)))
        );
        System.out.println(encoded);
        var decoded = RemoteAccessAuthentication.decode(encoded);
        System.out.println(decoded);

        var bytes = decoded.roleDescriptorsBytesList().iterator().next().iterator().next();
        var actual = new RoleDescriptor(bytes.streamInput());
        System.out.println(actual);
    }

}
