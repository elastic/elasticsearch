/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.elasticsearch.test.TestMatchers.predicateMatches;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class ManageApplicationPrivilegesTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final ManageApplicationPrivileges original = buildPrivileges();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                final ManageApplicationPrivileges copy = ManageApplicationPrivileges.createFrom(in);
                assertThat(copy, equalTo(original));
                assertThat(original, equalTo(copy));
            }
        }
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ManageApplicationPrivileges original = buildPrivileges();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes)) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                // ManageApplicationPrivileges.parse requires that the parser be positioned on the "manage" field.
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                final ManageApplicationPrivileges clone = ManageApplicationPrivileges.parse(parser);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));

                assertThat(clone, equalTo(original));
                assertThat(original, equalTo(clone));
            }
        }
    }

    public void testEqualsAndHashCode() {
        final int applicationNameLength = randomIntBetween(4, 7);
        final ManageApplicationPrivileges privileges = buildPrivileges(applicationNameLength);
        final EqualsHashCodeTestUtils.MutateFunction<ManageApplicationPrivileges> mutate
            = orig -> buildPrivileges(applicationNameLength + randomIntBetween(1, 3));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(privileges, this::clone, mutate);
    }

    public void testPrivilege() {
        final ManageApplicationPrivileges privileges = buildPrivileges();
        assertThat(privileges, instanceOf(ClusterPrivilege.class));
        for (String actionName : Arrays.asList(GetPrivilegesAction.NAME, PutPrivilegesAction.NAME, DeletePrivilegesAction.NAME)) {
            assertThat(privileges.predicate(), predicateMatches(actionName));
        }
        for (String actionName : Arrays.asList(GetUsersAction.NAME, PutRoleAction.NAME, DeleteRoleMappingAction.NAME,
            HasPrivilegesAction.NAME)) {
            assertThat(privileges.predicate(), not(predicateMatches(actionName)));
        }
    }

    public void testRequestPredicate() {
        final Authentication authentication = mock(Authentication.class);
        final ManageApplicationPrivileges kibanaAndLogstash = new ManageApplicationPrivileges(Sets.newHashSet("kibana-*", "logstash"));
        final ManageApplicationPrivileges cloudAndSwiftype = new ManageApplicationPrivileges(Sets.newHashSet("cloud-*", "swiftype"));
        final BiPredicate<TransportRequest, Authentication> kibanaAndLogstashPredicate = kibanaAndLogstash.getRequestPredicate();
        final BiPredicate<TransportRequest, Authentication> cloudAndSwiftypePredicate = cloudAndSwiftype.getRequestPredicate();
        assertThat(kibanaAndLogstashPredicate, notNullValue());
        assertThat(cloudAndSwiftypePredicate, notNullValue());

        final GetPrivilegesRequest getKibana1 = new GetPrivilegesRequest();
        getKibana1.application("kibana-1");
        assertThat(kibanaAndLogstashPredicate, predicateMatches(getKibana1, authentication));
        assertThat(cloudAndSwiftypePredicate, not(predicateMatches(getKibana1, authentication)));

        final DeletePrivilegesRequest deleteLogstash = new DeletePrivilegesRequest("logstash", new String[]{"all"});
        assertThat(kibanaAndLogstashPredicate, predicateMatches(deleteLogstash, authentication));
        assertThat(cloudAndSwiftypePredicate, not(predicateMatches(deleteLogstash, authentication)));

        final PutPrivilegesRequest putKibana = new PutPrivilegesRequest();

        final List<ApplicationPrivilegeDescriptor> kibanaPrivileges = new ArrayList<>();
        for (int i = randomIntBetween(2, 6); i > 0; i--) {
            kibanaPrivileges.add(new ApplicationPrivilegeDescriptor("kibana-" + i,
                randomAlphaOfLengthBetween(3, 6).toLowerCase(Locale.ROOT), Collections.emptySet(), Collections.emptyMap()));
        }
        putKibana.setPrivileges(kibanaPrivileges);
        assertThat(kibanaAndLogstashPredicate, predicateMatches(putKibana, authentication));
        assertThat(cloudAndSwiftypePredicate, not(predicateMatches(putKibana, authentication)));
    }

    public void testSecurityForGetAllApplicationPrivileges() {
        final Authentication authentication = mock(Authentication.class);
        final GetPrivilegesRequest getAll = new GetPrivilegesRequest();
        getAll.application(null);
        getAll.privileges(new String[0]);

        assertThat(getAll.validate(), nullValue());

        final ManageApplicationPrivileges kibanaOnly = new ManageApplicationPrivileges(Sets.newHashSet("kibana-*"));
        final ManageApplicationPrivileges allApps = new ManageApplicationPrivileges(Sets.newHashSet("*"));

        assertThat(kibanaOnly.getRequestPredicate(), not(predicateMatches(getAll, authentication)));
        assertThat(allApps.getRequestPredicate(), predicateMatches(getAll, authentication));
    }

    private ManageApplicationPrivileges clone(ManageApplicationPrivileges original) {
        return new ManageApplicationPrivileges(new LinkedHashSet<>(original.getApplicationNames()));
    }

    private ManageApplicationPrivileges buildPrivileges() {
        return buildPrivileges(randomIntBetween(4, 7));
    }

    static ManageApplicationPrivileges buildPrivileges(int applicationNameLength) {
        Set<String> applicationNames = Sets.newHashSet(Arrays.asList(generateRandomStringArray(5, applicationNameLength, false, false)));
        return new ManageApplicationPrivileges(applicationNames);
    }
}
