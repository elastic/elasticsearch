/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.action.service.AuthorizedServiceAccounts;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.QueryServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageServiceAccountsPrivilege;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ManageServiceAccountsPrivilegeTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final ManageServiceAccountsPrivilege original = buildPrivileges();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                final ManageServiceAccountsPrivilege copy = ManageServiceAccountsPrivilege.createFrom(in);
                assertThat(copy, equalTo(original));
                assertThat(original, equalTo(copy));
            }
        }
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ManageServiceAccountsPrivilege original = buildPrivileges();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.flush();

            try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, out.toByteArray())) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                // parse requires that the parser be positioned on the "manage" field.
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                final ManageServiceAccountsPrivilege clone = ManageServiceAccountsPrivilege.parse(parser);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                assertThat(clone, equalTo(original));
            }
        }
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            buildPrivileges(),
            original -> new ManageServiceAccountsPrivilege(
                new LinkedHashSet<>(original.getNames()),
                new LinkedHashSet<>(original.getRoles())
            ),
            original -> randomBoolean()
                ? new ManageServiceAccountsPrivilege(Set.of(randomAlphaOfLength(12) + "/*"), original.getRoles())
                : new ManageServiceAccountsPrivilege(original.getNames(), Set.of(randomAlphaOfLength(12)))
        );
    }

    public void testParsesTheGlobalShape() throws IOException {
        final String global = """
            {
              "service_account": {
                "manage": {
                  "names": [ "apps/*" ],
                  "roles": [ "ingest-reader", "alert-writer" ]
                }
              }
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, global)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            final List<ConfigurableClusterPrivilege> parsed = ConfigurableClusterPrivileges.parse(parser);
            assertThat(
                parsed,
                equalTo(List.of(new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("ingest-reader", "alert-writer"))))
            );
        }
    }

    public void testParseRequiresBothNamesAndRoles() throws IOException {
        for (String manage : List.of("{\"names\":[\"apps/*\"]}", "{\"roles\":[\"r1\"]}")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"service_account\":{\"manage\":" + manage + "}}")) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                final XContentParseException e = expectThrows(
                    XContentParseException.class,
                    () -> ConfigurableClusterPrivileges.parse(parser)
                );
                assertThat(e.getMessage(), containsString("[manage] requires both [names] and [roles]"));
            }
        }
    }

    public void testParseRejectsUnknownField() throws IOException {
        final String global = "{\"service_account\":{\"manage\":{\"names\":[\"apps/*\"],\"roles\":[],\"privileges\":[\"all\"]}}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, global)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            final XContentParseException e = expectThrows(XContentParseException.class, () -> ConfigurableClusterPrivileges.parse(parser));
            assertThat(e.getMessage(), containsString("but found [privileges] instead"));
        }
    }

    public void testRequiresAtLeastOneName() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ManageServiceAccountsPrivilege(Set.of(), Set.of("r1"))
        );
        assertThat(e.getMessage(), containsString("at least one service account name or pattern"));
    }

    /**
     * Roles are matched as names are: wildcards and {@code /regex/} patterns cover every role they match, and a write
     * is allowed only when each of its roles matches some pattern.
     */
    public void testRolePatternsMatchRoleNames() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("ingest-*", "reader?", "/alert-(read|write)/"))
        );
        assertThat(checkPut(permission, "apps", "payroll", List.of("ingest-logs", "reader1", "alert-write")), is(true));
        assertThat(checkPut(permission, "apps", "payroll", List.of("ingest-")), is(true));
        assertThat(checkPut(permission, "apps", "payroll", List.of("ingest-logs", "reader12")), is(false));
        assertThat(checkPut(permission, "apps", "payroll", List.of("alert-admin")), is(false));
        assertThat(checkPut(permission, "apps", "payroll", List.of("logs-ingest")), is(false));
    }

    public void testRoleWildcardCoversEveryRole() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("*")));
        assertThat(checkPut(permission, "apps", "payroll", List.of(randomAlphaOfLength(8), "superuser")), is(true));
    }

    /**
     * The roles a write lists must all match the grant. A subset of them, including none at all, is allowed, so
     * narrowing an account is always possible.
     */
    public void testPutIsAllowedWhenEveryRoleIsListed() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1", "r2")));
        assertThat(checkPut(permission, "apps", "payroll", List.of("r1", "r2")), is(true));
        assertThat(checkPut(permission, "apps", "payroll", List.of("r2")), is(true));
        assertThat(checkPut(permission, "apps", "payroll", List.of()), is(true));
    }

    /**
     * Authorization sees only the request, which replaces the account wholesale, so a role the grant does not match is refused
     * even when the account already holds it.
     */
    public void testPutIsDeniedWhenAnyRoleIsUnlisted() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1", "r2")));
        assertThat(checkPut(permission, "apps", "payroll", List.of("r1", "r3")), is(false));
        assertThat(checkPut(permission, "apps", "payroll", List.of("r3")), is(false));
    }

    public void testRoleNamesWithoutPatternsAreExact() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("ingest")));
        assertThat(checkPut(permission, "apps", "payroll", List.of("ingest-admin")), is(false));
        assertThat(checkPut(permission, "apps", "payroll", List.of("INGEST")), is(false));
    }

    public void testPutIsDeniedOutsideTheNamedAccounts() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of("apps/*", "batch/nightly"), Set.of("r1"))
        );
        assertThat(checkPut(permission, "apps", "anything", List.of("r1")), is(true));
        assertThat(checkPut(permission, "batch", "nightly", List.of("r1")), is(true));
        assertThat(checkPut(permission, "batch", "hourly", List.of("r1")), is(false));
        assertThat(checkPut(permission, "apps-other", "payroll", List.of("r1")), is(false));
    }

    /**
     * Two grants held together do not combine: each is checked on its own, so the roles of one cannot be given to the
     * accounts of the other, even though the holder may write both sets of accounts and assign both sets of roles.
     */
    public void testGrantsAreEvaluatedIndependently() {
        ClusterPermission.Builder builder = ClusterPermission.builder();
        builder = new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1")).buildPermission(builder);
        builder = new ManageServiceAccountsPrivilege(Set.of("batch/*"), Set.of("r2")).buildPermission(builder);
        final ClusterPermission permission = builder.build();
        assertThat(checkPut(permission, "apps", "payroll", List.of("r1")), is(true));
        assertThat(checkPut(permission, "batch", "nightly", List.of("r2")), is(true));
        assertThat(checkPut(permission, "apps", "payroll", List.of("r2")), is(false));
        assertThat(checkPut(permission, "apps", "payroll", List.of("r1", "r2")), is(false));
    }

    /**
     * Tokens can be created for the named accounts. The roles are not consulted, as the request does not carry them:
     * they are checked when the account is written.
     */
    public void testTokensAreCreatedForTheNamedAccounts() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of("apps/*", "batch/nightly"), Set.of(randomFrom("r1", "*")))
        );
        assertThat(checkCreateToken(permission, "apps", "payroll"), is(true));
        assertThat(checkCreateToken(permission, "batch", "nightly"), is(true));
        assertThat(checkCreateToken(permission, "batch", "hourly"), is(false));
        assertThat(checkCreateToken(permission, "apps-other", "payroll"), is(false));
    }

    public void testTokensAreDeletedForTheNamedAccounts() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of("apps/*", "batch/nightly"), Set.of(randomFrom("r1", "*")))
        );
        assertThat(checkDeleteToken(permission, "apps", "payroll"), is(true));
        assertThat(checkDeleteToken(permission, "batch", "nightly"), is(true));
        assertThat(checkDeleteToken(permission, "batch", "hourly"), is(false));
        assertThat(checkDeleteToken(permission, "apps-other", "payroll"), is(false));
    }

    /**
     * The user-managed token actions are the only ones granted, even though built-in accounts' tokens are created and
     * deleted with the same requests.
     */
    public void testBuiltInAccountTokensAreNotCovered() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("*"), Set.of("r1")));
        final TransportRequest createToken = new CreateServiceAccountTokenRequest("elastic", "fleet-server", "token");
        assertThat(permission.check(CreateServiceAccountTokenAction.NAME, createToken, authentication()), is(false));
        final TransportRequest deleteToken = new DeleteServiceAccountTokenRequest("elastic", "fleet-server", "token");
        assertThat(permission.check(DeleteServiceAccountTokenAction.NAME, deleteToken, authentication()), is(false));
    }

    /**
     * Accounts are deleted by name alone, forced or not: the request carries no roles, and a write could already
     * replace the account wholesale.
     */
    public void testAccountsAreDeletedForTheNamedAccounts() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of("apps/*", "batch/nightly"), Set.of(randomFrom("r1", "*")))
        );
        assertThat(checkDeleteAccount(permission, "apps", "payroll"), is(true));
        assertThat(checkDeleteAccount(permission, "batch", "nightly"), is(true));
        assertThat(checkDeleteAccount(permission, "batch", "hourly"), is(false));
        assertThat(checkDeleteAccount(permission, "apps-other", "payroll"), is(false));
    }

    public void testCredentialsAreReadForTheNamedAccounts() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of("apps/*", "batch/nightly"), Set.of(randomFrom("r1", "*")))
        );
        assertThat(checkCredentials(permission, "apps", "payroll"), is(true));
        assertThat(checkCredentials(permission, "batch", "nightly"), is(true));
        assertThat(checkCredentials(permission, "batch", "hourly"), is(false));
        assertThat(checkCredentials(permission, "apps-other", "payroll"), is(false));
    }

    /**
     * Built-in and user-managed accounts share the credentials action, so the request itself must keep built-in
     * accounts out, however broad the names.
     */
    public void testBuiltInAccountCredentialsAreNotCovered() {
        final ClusterPermission permission = permissionFor(
            new ManageServiceAccountsPrivilege(Set.of(randomFrom("*", "elastic/*", "elastic/fleet-server")), Set.of("r1"))
        );
        assertThat(checkCredentials(permission, "elastic", "fleet-server"), is(false));
    }

    public void testGrantsNoOtherServiceAccountAction() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("*"), Set.of("r1")));
        final TransportRequest put = new PutUserManagedServiceAccountRequest("apps", "payroll", List.of("r1"), true, null);
        assertThat(permission.check("cluster:admin/xpack/security/user_managed_service_account/other", put, authentication()), is(false));
        assertThat(permission.check("cluster:admin/xpack/security/service_account/other", put, authentication()), is(false));
    }

    /**
     * Reads select accounts rather than naming them, so they are granted only once authorization has restricted them
     * to the authorized accounts, never as the caller sent them.
     */
    public void testReadsAreGrantedOnlyOnceRestricted() {
        final ClusterPermission permission = permissionFor(new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1")));
        final AuthorizedServiceAccounts authorizedAccounts = new AuthorizedServiceAccounts(List.of(Set.of("apps/*")));

        final GetServiceAccountRequest get = new GetServiceAccountRequest("apps", "payroll", EnumSet.of(ServiceAccountType.USER_MANAGED));
        assertThat(permission.check(GetServiceAccountAction.NAME, get, authentication()), is(false));
        get.restrictToAuthorizedAccounts(authorizedAccounts);
        assertThat(permission.check(GetServiceAccountAction.NAME, get, authentication()), is(true));

        final QueryServiceAccountRequest query = new QueryServiceAccountRequest(null, null, null, null, null);
        assertThat(permission.check(QueryServiceAccountAction.NAME, query, authentication()), is(false));
        query.restrictToAuthorizedAccounts(authorizedAccounts);
        assertThat(permission.check(QueryServiceAccountAction.NAME, query, authentication()), is(true));
    }

    public void testAuthorizedAccountsRequireEveryGroupToMatch() {
        final AuthorizedServiceAccounts single = new AuthorizedServiceAccounts(List.of(Set.of("apps/*", "batch/nightly")));
        assertThat(single.test("apps/payroll"), is(true));
        assertThat(single.test("batch/nightly"), is(true));
        assertThat(single.test("batch/hourly"), is(false));

        final AuthorizedServiceAccounts intersected = new AuthorizedServiceAccounts(List.of(Set.of("apps/*"), Set.of("*/payroll")));
        assertThat(intersected.test("apps/payroll"), is(true));
        assertThat(intersected.test("apps/billing"), is(false));
        assertThat(intersected.test("batch/payroll"), is(false));

        expectThrows(IllegalArgumentException.class, () -> new AuthorizedServiceAccounts(List.of()));
        expectThrows(IllegalArgumentException.class, () -> new AuthorizedServiceAccounts(List.of(Set.of())));
    }

    public void testGlobalXContentOmitsServiceAccountWhenAbsent() throws IOException {
        final XContentBuilder without = JsonXContent.contentBuilder();
        ConfigurableClusterPrivileges.toXContent(
            without,
            ToXContent.EMPTY_PARAMS,
            List.of(ManageApplicationPrivilegesTests.buildPrivileges())
        );
        assertThat(BytesReference.bytes(without).utf8ToString(), not(containsString("service_account")));

        final ManageServiceAccountsPrivilege privilege = new ManageServiceAccountsPrivilege(Set.of("apps/*"), Set.of("r1"));
        final XContentBuilder with = JsonXContent.contentBuilder();
        ConfigurableClusterPrivileges.toXContent(with, ToXContent.EMPTY_PARAMS, List.of(privilege));
        assertThat(
            BytesReference.bytes(with).utf8ToString(),
            containsString("\"service_account\":{\"manage\":{\"names\":[\"apps/*\"],\"roles\":[\"r1\"]}}")
        );
    }

    private static Authentication authentication() {
        return AuthenticationTestHelper.builder().build();
    }

    private static ClusterPermission permissionFor(ManageServiceAccountsPrivilege privilege) {
        return privilege.buildPermission(ClusterPermission.builder()).build();
    }

    private static boolean checkPut(ClusterPermission permission, String namespace, String serviceName, List<String> roles) {
        final PutUserManagedServiceAccountRequest request = new PutUserManagedServiceAccountRequest(
            namespace,
            serviceName,
            roles,
            randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(8)
        );
        return permission.check(PutUserManagedServiceAccountAction.NAME, request, authentication());
    }

    private static boolean checkCreateToken(ClusterPermission permission, String namespace, String serviceName) {
        final CreateServiceAccountTokenRequest request = new CreateServiceAccountTokenRequest(namespace, serviceName, "token");
        return permission.check(CreateUserManagedServiceAccountTokenAction.NAME, request, authentication());
    }

    private static boolean checkDeleteToken(ClusterPermission permission, String namespace, String serviceName) {
        final DeleteServiceAccountTokenRequest request = new DeleteServiceAccountTokenRequest(namespace, serviceName, "token");
        return permission.check(DeleteUserManagedServiceAccountTokenAction.NAME, request, authentication());
    }

    private static boolean checkDeleteAccount(ClusterPermission permission, String namespace, String serviceName) {
        final DeleteUserManagedServiceAccountRequest request = new DeleteUserManagedServiceAccountRequest(
            namespace,
            serviceName,
            randomFrom(WriteRequest.RefreshPolicy.values()),
            randomBoolean()
        );
        return permission.check(DeleteUserManagedServiceAccountAction.NAME, request, authentication());
    }

    private static boolean checkCredentials(ClusterPermission permission, String namespace, String serviceName) {
        final GetServiceAccountCredentialsRequest request = new GetServiceAccountCredentialsRequest(namespace, serviceName);
        return permission.check(GetServiceAccountCredentialsAction.NAME, request, authentication());
    }

    static ManageServiceAccountsPrivilege buildPrivileges() {
        final Set<String> names = new LinkedHashSet<>();
        for (int i = randomIntBetween(1, 4); i > 0; i--) {
            names.add(randomAlphaOfLengthBetween(3, 8) + "/" + randomFrom("*", randomAlphaOfLengthBetween(3, 8)));
        }
        final Set<String> roles = new LinkedHashSet<>(Arrays.asList(generateRandomStringArray(5, 8, false, true)));
        return new ManageServiceAccountsPrivilege(names, roles);
    }
}
