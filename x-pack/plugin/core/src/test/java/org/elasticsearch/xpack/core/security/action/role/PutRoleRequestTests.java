/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivileges;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutRoleRequestTests extends ESTestCase {

    public void testValidationOfApplicationPrivileges() {
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("app", new String[]{"read"}, new String[]{"*"}));
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("app", new String[]{"action:login"}, new String[]{"/"}));
        assertSuccessfulValidation(buildRequestWithApplicationPrivilege("*", new String[]{"data/read:user"}, new String[]{"user/123"}));

        // Fail
        assertValidationError("privilege names and actions must match the pattern",
            buildRequestWithApplicationPrivilege("app", new String[]{"in valid"}, new String[]{"*"}));
        assertValidationError("An application name prefix must match the pattern",
            buildRequestWithApplicationPrivilege("000", new String[]{"all"}, new String[]{"*"}));
        assertValidationError("An application name prefix must match the pattern",
            buildRequestWithApplicationPrivilege("%*", new String[]{"all"}, new String[]{"*"}));
    }

    public void testSerialization() throws IOException {
        final PutRoleRequest original = buildRandomRequest();

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final PutRoleRequest copy = new PutRoleRequest();
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
        copy.readFrom(in);

        assertThat(copy.roleDescriptor(), equalTo(original.roleDescriptor()));
    }

    public void testSerializationBetweenV64AndV66() throws IOException {
        final PutRoleRequest original = buildRandomRequest();

        final BytesStreamOutput out = new BytesStreamOutput();
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_4_0, Version.V_6_6_0);
        out.setVersion(version);
        original.writeTo(out);

        final PutRoleRequest copy = new PutRoleRequest();
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);
        in.setVersion(version);
        copy.readFrom(in);

        assertThat(copy.name(), equalTo(original.name()));
        assertThat(copy.cluster(), equalTo(original.cluster()));
        assertIndicesSerializedRestricted(copy.indices(), original.indices());
        assertThat(copy.runAs(), equalTo(original.runAs()));
        assertThat(copy.metadata(), equalTo(original.metadata()));
        assertThat(copy.getRefreshPolicy(), equalTo(original.getRefreshPolicy()));

        assertThat(copy.applicationPrivileges(), equalTo(original.applicationPrivileges()));
        assertThat(copy.conditionalClusterPrivileges(), equalTo(original.conditionalClusterPrivileges()));
    }

    public void testSerializationV60AndV32() throws IOException {
        final PutRoleRequest original = buildRandomRequest();

        final BytesStreamOutput out = new BytesStreamOutput();
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_6_0, Version.V_6_3_2);
        out.setVersion(version);
        original.writeTo(out);

        final PutRoleRequest copy = new PutRoleRequest();
        final StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        copy.readFrom(in);

        assertThat(copy.name(), equalTo(original.name()));
        assertThat(copy.cluster(), equalTo(original.cluster()));
        assertIndicesSerializedRestricted(copy.indices(), original.indices());
        assertThat(copy.runAs(), equalTo(original.runAs()));
        assertThat(copy.metadata(), equalTo(original.metadata()));
        assertThat(copy.getRefreshPolicy(), equalTo(original.getRefreshPolicy()));

        assertThat(copy.applicationPrivileges(), iterableWithSize(0));
        assertThat(copy.conditionalClusterPrivileges(), arrayWithSize(0));
    }

    private void assertIndicesSerializedRestricted(RoleDescriptor.IndicesPrivileges[] copy, RoleDescriptor.IndicesPrivileges[] original) {
        assertThat(copy.length, equalTo(original.length));
        for (int i = 0; i < copy.length; i++) {
            assertThat(copy[i].allowRestrictedIndices(), equalTo(false));
            assertThat(copy[i].getIndices(), equalTo(original[i].getIndices()));
            assertThat(copy[i].getPrivileges(), equalTo(original[i].getPrivileges()));
            assertThat(copy[i].getDeniedFields(), equalTo(original[i].getDeniedFields()));
            assertThat(copy[i].getGrantedFields(), equalTo(original[i].getGrantedFields()));
            assertThat(copy[i].getQuery(), equalTo(original[i].getQuery()));
        }
    }

    private void assertSuccessfulValidation(PutRoleRequest request) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, nullValue());
    }

    private void assertValidationError(String message, PutRoleRequest request) {
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem(containsString(message)));
    }

    private PutRoleRequest buildRequestWithApplicationPrivilege(String appName, String[] privileges, String[] resources) {
        final PutRoleRequest request = new PutRoleRequest();
        request.name("test");
        final ApplicationResourcePrivileges privilege = ApplicationResourcePrivileges.builder()
            .application(appName)
            .privileges(privileges)
            .resources(resources)
            .build();
        request.addApplicationPrivileges(new ApplicationResourcePrivileges[]{privilege});
        return request;
    }

    private PutRoleRequest buildRandomRequest() {

        final PutRoleRequest request = new PutRoleRequest();
        request.name(randomAlphaOfLengthBetween(4, 9));

        request.cluster(randomSubsetOf(Arrays.asList("monitor", "manage", "all", "manage_security", "manage_ml", "monitor_watcher"))
            .toArray(Strings.EMPTY_ARRAY));

        for (int i = randomIntBetween(0, 4); i > 0; i--) {
            request.addIndex(
                generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(3, 8), false, false),
                randomSubsetOf(randomIntBetween(1, 2), "read", "write", "index", "all").toArray(Strings.EMPTY_ARRAY),
                generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(3, 8), true),
                generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(3, 8), true),
                null,
                randomBoolean()
            );
        }

        final Supplier<String> stringWithInitialLowercase = ()
            -> randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 12);
        final ApplicationResourcePrivileges[] applicationPrivileges = new ApplicationResourcePrivileges[randomIntBetween(0, 5)];
        for (int i = 0; i < applicationPrivileges.length; i++) {
            applicationPrivileges[i] = ApplicationResourcePrivileges.builder()
                .application(stringWithInitialLowercase.get())
                .privileges(randomArray(1, 3, String[]::new, stringWithInitialLowercase))
                .resources(generateRandomStringArray(5, randomIntBetween(3, 8), false, false))
                .build();
        }
        request.addApplicationPrivileges(applicationPrivileges);

        if (randomBoolean()) {
            final String[] appNames = randomArray(1, 4, String[]::new, stringWithInitialLowercase);
            request.conditionalCluster(new ConditionalClusterPrivileges.ManageApplicationPrivileges(Sets.newHashSet(appNames)));
        }

        request.runAs(generateRandomStringArray(4, 3, false, true));

        final Map<String, Object> metadata = new HashMap<>();
        for (String key : generateRandomStringArray(3, 5, false, true)) {
            metadata.put(key, randomFrom(Boolean.TRUE, Boolean.FALSE, 1, 2, randomAlphaOfLengthBetween(2, 9)));
        }
        request.metadata(metadata);

        request.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        return request;
    }
}
