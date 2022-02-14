/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.SearchProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class UpdateProfileDataPrivilegesTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final ConfigurableClusterPrivileges.UpdateProfileDataPrivileges original = buildPrivileges();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                final ConfigurableClusterPrivileges.UpdateProfileDataPrivileges copy =
                    ConfigurableClusterPrivileges.UpdateProfileDataPrivileges.createFrom(in);
                assertThat(copy, equalTo(original));
                assertThat(original, equalTo(copy));
            }
        }
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ConfigurableClusterPrivileges.UpdateProfileDataPrivileges original = buildPrivileges();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes)) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                final ConfigurableClusterPrivileges.UpdateProfileDataPrivileges clone =
                    ConfigurableClusterPrivileges.UpdateProfileDataPrivileges.parse(parser);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));

                assertThat(clone, equalTo(original));
                assertThat(original, equalTo(clone));
            }
        }
    }

    public void testActionAndRequestPredicate() {
        final String prefix = randomAlphaOfLengthBetween(0, 3);
        final String name = randomAlphaOfLengthBetween(0, 5);
        String other = randomAlphaOfLengthBetween(0, 7);
        if (prefix.isEmpty() || other.startsWith(prefix) || other.equals(name)) {
            other = null;
        }
        final ConfigurableClusterPrivileges.UpdateProfileDataPrivileges updateProfileDataPrivilege =
            new ConfigurableClusterPrivileges.UpdateProfileDataPrivileges(Sets.newHashSet(prefix + "*", name));
        final ClusterPermission updateProfileDataPermission = updateProfileDataPrivilege.buildPermission(ClusterPermission.builder())
            .build();
        assertThat(updateProfileDataPermission, notNullValue());

        final Authentication authentication = mock(Authentication.class);
        // request application name matches privilege wildcard
        UpdateProfileDataRequest updateProfileDataRequest = randomBoolean()
            ? newUpdateProfileDataRequest(Set.of(prefix + randomAlphaOfLengthBetween(0, 2)), Set.of())
            : newUpdateProfileDataRequest(Set.of(), Set.of(prefix + randomAlphaOfLengthBetween(0, 2)));
        assertTrue(
            updateProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", updateProfileDataRequest, authentication)
        );
        // request application name matches privilege name
        updateProfileDataRequest = randomBoolean()
            ? newUpdateProfileDataRequest(Set.of(name), Set.of())
            : newUpdateProfileDataRequest(Set.of(), Set.of(name));
        assertTrue(
            updateProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", updateProfileDataRequest, authentication)
        );
        // different action name
        assertFalse(
            updateProfileDataPermission.check(
                randomFrom(ActivateProfileAction.NAME, GetProfileAction.NAME, SearchProfilesAction.NAME),
                updateProfileDataRequest,
                authentication
            )
        );
        if (other != null) {
            updateProfileDataRequest = randomBoolean()
                ? newUpdateProfileDataRequest(
                    randomBoolean() ? Set.of(prefix + randomAlphaOfLengthBetween(0, 2), other) : Set.of(other),
                    Set.of()
                )
                : newUpdateProfileDataRequest(
                    Set.of(),
                    randomBoolean() ? Set.of(prefix + randomAlphaOfLengthBetween(0, 2), other) : Set.of(other)
                );
            assertFalse(
                updateProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", updateProfileDataRequest, authentication)
            );
            updateProfileDataRequest = randomBoolean()
                ? newUpdateProfileDataRequest(randomBoolean() ? Set.of(name, other) : Set.of(other), Set.of())
                : newUpdateProfileDataRequest(Set.of(), randomBoolean() ? Set.of(name, other) : Set.of(other));
            assertFalse(
                updateProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", updateProfileDataRequest, authentication)
            );
        }
        assertFalse(
            updateProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", mock(TransportRequest.class), authentication)
        );
    }

    public void testEqualsAndHashCode() {
        final int applicationNameLength = randomIntBetween(4, 7);
        final ConfigurableClusterPrivileges.UpdateProfileDataPrivileges privileges = buildPrivileges(applicationNameLength);
        final EqualsHashCodeTestUtils.MutateFunction<ConfigurableClusterPrivileges.UpdateProfileDataPrivileges> mutate =
            orig -> buildPrivileges(applicationNameLength + randomIntBetween(1, 3));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(privileges, this::clone, mutate);
    }

    private UpdateProfileDataRequest newUpdateProfileDataRequest(Set<String> accessNames, Set<String> dataNames) {
        Map<String, Object> access = new HashMap<>();
        for (String accessName : accessNames) {
            access.put(accessName, mock(Object.class));
        }
        Map<String, Object> data = new HashMap<>();
        for (String dataName : dataNames) {
            data.put(dataName, mock(Object.class));
        }
        return new UpdateProfileDataRequest(
            randomAlphaOfLengthBetween(4, 8),
            access,
            data,
            randomLong(),
            randomLong(),
            randomFrom(WriteRequest.RefreshPolicy.values())
        );
    }

    private ConfigurableClusterPrivileges.UpdateProfileDataPrivileges clone(
        ConfigurableClusterPrivileges.UpdateProfileDataPrivileges original
    ) {
        return new ConfigurableClusterPrivileges.UpdateProfileDataPrivileges(new LinkedHashSet<>(original.getApplicationNames()));
    }

    private ConfigurableClusterPrivileges.UpdateProfileDataPrivileges buildPrivileges() {
        return buildPrivileges(randomIntBetween(4, 7));
    }

    static ConfigurableClusterPrivileges.UpdateProfileDataPrivileges buildPrivileges(int applicationNameLength) {
        Set<String> applicationNames = Sets.newHashSet(Arrays.asList(generateRandomStringArray(5, applicationNameLength, false, false)));
        return new ConfigurableClusterPrivileges.UpdateProfileDataPrivileges(applicationNames);
    }
}
