/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;

public class RemoteAccessAuthenticationTests extends ESTestCase {

    public void testWriteReadContextRoundtrip() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection = randomRoleDescriptorsIntersection();
        final var expectedRemoteAccessAuthentication = new RemoteAccessAuthentication(
            AuthenticationTestHelper.builder().build(),
            expectedRoleDescriptorsIntersection
        );

        expectedRemoteAccessAuthentication.writeToContext(ctx);
        final RemoteAccessAuthentication actual = RemoteAccessAuthentication.readFromContext(ctx);

        assertThat(actual.getAuthentication(), equalTo(expectedRemoteAccessAuthentication.getAuthentication()));
        final List<Set<RoleDescriptor>> roleDescriptorsList = new ArrayList<>();
        for (BytesReference bytesReference : actual.getRoleDescriptorsBytesList()) {
            Set<RoleDescriptor> roleDescriptors = new RemoteAccessAuthentication.RoleDescriptorsBytes(bytesReference).toRoleDescriptors();
            roleDescriptorsList.add(roleDescriptors);
        }
        final var actualRoleDescriptorsIntersection = new RoleDescriptorsIntersection(roleDescriptorsList);
        assertThat(actualRoleDescriptorsIntersection, equalTo(expectedRoleDescriptorsIntersection));
    }

    public void testRoleDescriptorsBytesToRoleDescriptors() throws IOException {
        final Set<RoleDescriptor> expectedRoleDescriptors = Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 3));
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(expectedRoleDescriptors.stream().collect(Collectors.toMap(RoleDescriptor::getName, Function.identity())));
        final Set<RoleDescriptor> actualRoleDescriptors = new RemoteAccessAuthentication.RoleDescriptorsBytes(BytesReference.bytes(builder))
            .toRoleDescriptors();
        assertThat(actualRoleDescriptors, equalTo(expectedRoleDescriptors));
    }

    private RoleDescriptorsIntersection randomRoleDescriptorsIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }
}
