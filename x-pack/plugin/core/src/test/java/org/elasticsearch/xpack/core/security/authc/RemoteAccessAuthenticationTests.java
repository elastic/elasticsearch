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
        final Authentication expectedAuthentication = AuthenticationTestHelper.builder().build();
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection = randomRoleDescriptorIntersection();

        new RemoteAccessAuthentication(expectedAuthentication, expectedRoleDescriptorsIntersection).writeToContext(ctx);
        final RemoteAccessAuthentication actual = RemoteAccessAuthentication.readFromContext(ctx);

        assertThat(actual.getAuthentication(), equalTo(expectedAuthentication));
        final List<Set<RoleDescriptor>> list = new ArrayList<>();
        for (BytesReference bytesReference : actual.getRoleDescriptorsBytesList()) {
            Set<RoleDescriptor> roleDescriptors = RemoteAccessAuthentication.parseRoleDescriptorsBytes(bytesReference);
            list.add(roleDescriptors);
        }
        final var actualRoleDescriptorIntersection = new RoleDescriptorsIntersection(list);
        assertThat(actualRoleDescriptorIntersection, equalTo(expectedRoleDescriptorsIntersection));
    }

    public void testWriteToContextThrowsIfHeaderAlreadyPresent() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        new RemoteAccessAuthentication(AuthenticationTestHelper.builder().build(), randomRoleDescriptorIntersection()).writeToContext(ctx);
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteAccessAuthentication(AuthenticationTestHelper.builder().build(), randomRoleDescriptorIntersection())
                .writeToContext(ctx)
        );
        assertThat(
            ex.getMessage(),
            equalTo("value for key [" + RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] already present")
        );
    }

    public void testParseRoleDescriptorsBytes() throws IOException {
        final Set<RoleDescriptor> expectedRoleDescriptors = Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 3));
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(expectedRoleDescriptors.stream().collect(Collectors.toMap(RoleDescriptor::getName, Function.identity())));
        final Set<RoleDescriptor> actualRoleDescriptors = RemoteAccessAuthentication.parseRoleDescriptorsBytes(
            BytesReference.bytes(builder)
        );
        assertThat(actualRoleDescriptors, equalTo(expectedRoleDescriptors));
    }

    private RoleDescriptorsIntersection randomRoleDescriptorIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }
}
