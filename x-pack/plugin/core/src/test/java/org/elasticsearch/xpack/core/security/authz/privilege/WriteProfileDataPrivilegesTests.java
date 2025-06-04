/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class WriteProfileDataPrivilegesTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final ConfigurableClusterPrivileges.WriteProfileDataPrivileges original = buildPrivileges();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                final ConfigurableClusterPrivileges.WriteProfileDataPrivileges copy =
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges.createFrom(in);
                assertThat(copy, equalTo(original));
                assertThat(original, equalTo(copy));
            }
        }
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ConfigurableClusterPrivileges.WriteProfileDataPrivileges original = buildPrivileges();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, bytes)) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                final ConfigurableClusterPrivileges.WriteProfileDataPrivileges clone =
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges.parse(parser);
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
        if (other.startsWith(prefix) || other.equals(name)) {
            other = null;
        }
        final ConfigurableClusterPrivileges.WriteProfileDataPrivileges writeProfileDataPrivileges =
            new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(Sets.newHashSet(prefix + "*", name));
        final ClusterPermission writeProfileDataPermission = writeProfileDataPrivileges.buildPermission(ClusterPermission.builder())
            .build();
        assertThat(writeProfileDataPermission, notNullValue());

        final Authentication authentication = AuthenticationTestHelper.builder().build();
        // request application name matches privilege wildcard
        UpdateProfileDataRequest updateProfileDataRequest = randomBoolean()
            ? newUpdateProfileDataRequest(Set.of(prefix + randomAlphaOfLengthBetween(0, 2)), Set.of())
            : newUpdateProfileDataRequest(Set.of(), Set.of(prefix + randomAlphaOfLengthBetween(0, 2)));
        assertTrue(
            writeProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", updateProfileDataRequest, authentication)
        );
        // request application name matches privilege name
        updateProfileDataRequest = randomBoolean()
            ? newUpdateProfileDataRequest(Set.of(name), Set.of())
            : newUpdateProfileDataRequest(Set.of(), Set.of(name));
        assertTrue(
            writeProfileDataPermission.check("cluster:admin/xpack/security/profile/put/data", updateProfileDataRequest, authentication)
        );
        // different action name
        assertFalse(
            writeProfileDataPermission.check(
                randomFrom(ActivateProfileAction.NAME, GetProfilesAction.NAME, SuggestProfilesAction.NAME),
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
            assertFalse(writeProfileDataPermission.check(UpdateProfileDataAction.NAME, updateProfileDataRequest, authentication));
            updateProfileDataRequest = randomBoolean()
                ? newUpdateProfileDataRequest(randomBoolean() ? Set.of(name, other) : Set.of(other), Set.of())
                : newUpdateProfileDataRequest(Set.of(), randomBoolean() ? Set.of(name, other) : Set.of(other));
            assertFalse(writeProfileDataPermission.check(UpdateProfileDataAction.NAME, updateProfileDataRequest, authentication));
        }
        assertFalse(writeProfileDataPermission.check(UpdateProfileDataAction.NAME, mock(TransportRequest.class), authentication));
    }

    public void testParseAbnormals() throws Exception {
        final String nullApplications = "{\"write\":{\"applications\":null}}";
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    new ByteArrayInputStream(nullApplications.getBytes(StandardCharsets.UTF_8))
                )
        ) {
            parser.nextToken(); // {
            parser.nextToken(); // "write" field
            expectThrows(XContentParseException.class, () -> ConfigurableClusterPrivileges.WriteProfileDataPrivileges.parse(parser));
            parser.nextToken();
        }
        final String emptyApplications = "{\"write\":{\"applications\":[]}}";
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    new ByteArrayInputStream(emptyApplications.getBytes(StandardCharsets.UTF_8))
                )
        ) {
            parser.nextToken(); // {
            parser.nextToken(); // "write" field
            ConfigurableClusterPrivileges.WriteProfileDataPrivileges priv = ConfigurableClusterPrivileges.WriteProfileDataPrivileges.parse(
                parser
            );
            parser.nextToken();
            assertThat(priv.getApplicationNames().size(), is(0));
            UpdateProfileDataRequest updateProfileDataRequest = randomBoolean()
                ? newUpdateProfileDataRequest(Set.of(randomAlphaOfLengthBetween(0, 2)), Set.of())
                : newUpdateProfileDataRequest(Set.of(), Set.of(randomAlphaOfLengthBetween(0, 2)));
            ClusterPermission perm = priv.buildPermission(ClusterPermission.builder()).build();
            assertFalse(perm.check(UpdateProfileDataAction.NAME, updateProfileDataRequest, AuthenticationTestHelper.builder().build()));
        }
        final String aNullApplication = "{\"write\":{\"applications\":[null]}}";
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    new ByteArrayInputStream(aNullApplication.getBytes(StandardCharsets.UTF_8))
                )
        ) {
            parser.nextToken(); // {
            parser.nextToken(); // "write" field
            expectThrows(ElasticsearchParseException.class, () -> ConfigurableClusterPrivileges.WriteProfileDataPrivileges.parse(parser));
            parser.nextToken();
        }
        final String anEmptyApplication = "{\"write\":{\"applications\":[\"\"]}}";
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    new ByteArrayInputStream(anEmptyApplication.getBytes(StandardCharsets.UTF_8))
                )
        ) {
            parser.nextToken(); // {
            parser.nextToken(); // "write" field
            ConfigurableClusterPrivileges.WriteProfileDataPrivileges priv = ConfigurableClusterPrivileges.WriteProfileDataPrivileges.parse(
                parser
            );
            parser.nextToken();
            assertThat(priv.getApplicationNames().size(), is(1));
            assertThat(priv.getApplicationNames().stream().findFirst().get(), is(""));
            UpdateProfileDataRequest updateProfileDataRequest = randomBoolean()
                ? newUpdateProfileDataRequest(Set.of(randomAlphaOfLengthBetween(1, 2)), Set.of())
                : newUpdateProfileDataRequest(Set.of(), Set.of(randomAlphaOfLengthBetween(1, 2)));
            ClusterPermission perm = priv.buildPermission(ClusterPermission.builder()).build();
            assertFalse(perm.check(UpdateProfileDataAction.NAME, updateProfileDataRequest, AuthenticationTestHelper.builder().build()));
            updateProfileDataRequest = randomBoolean()
                ? newUpdateProfileDataRequest(Set.of(""), Set.of())
                : newUpdateProfileDataRequest(Set.of(), Set.of(""));
            perm = priv.buildPermission(ClusterPermission.builder()).build();
            assertTrue(
                perm.check(
                    "cluster:admin/xpack/security/profile/put/data",
                    updateProfileDataRequest,
                    AuthenticationTestHelper.builder().build()
                )
            );
        }
    }

    public void testEqualsAndHashCode() {
        final int applicationNameLength = randomIntBetween(4, 7);
        final ConfigurableClusterPrivileges.WriteProfileDataPrivileges privileges = buildPrivileges(applicationNameLength);
        final EqualsHashCodeTestUtils.MutateFunction<ConfigurableClusterPrivileges.WriteProfileDataPrivileges> mutate =
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

    private ConfigurableClusterPrivileges.WriteProfileDataPrivileges clone(
        ConfigurableClusterPrivileges.WriteProfileDataPrivileges original
    ) {
        return new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(new LinkedHashSet<>(original.getApplicationNames()));
    }

    static ConfigurableClusterPrivileges.WriteProfileDataPrivileges buildPrivileges() {
        return buildPrivileges(randomIntBetween(4, 7));
    }

    static ConfigurableClusterPrivileges.WriteProfileDataPrivileges buildPrivileges(int applicationNameLength) {
        Set<String> applicationNames = Sets.newHashSet(Arrays.asList(generateRandomStringArray(5, applicationNameLength, false, false)));
        return new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(applicationNames);
    }
}
