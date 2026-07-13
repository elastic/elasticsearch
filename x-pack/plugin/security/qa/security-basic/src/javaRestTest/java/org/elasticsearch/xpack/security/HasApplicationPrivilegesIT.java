/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;

public class HasApplicationPrivilegesIT extends SecurityInBasicRestTestCase {

    private TestSecurityClient securityClient;

    @Before
    public void setupClient() {
        securityClient = new TestSecurityClient(adminClient());
    }

    public void testUserWithWildcardPrivileges() throws Exception {
        var mainApplication = randomApplicationName();

        // Privilege names that are defined application privileges, possible on the "main" app, possibly for another app.
        final Set<String> definedPrivilegeNames = new HashSet<>();

        // All applications for which application privileges have been defined
        final Set<String> allApplications = new HashSet<>();
        allApplications.add(mainApplication);
        CheckedConsumer<String, IOException> createApplicationPrivilege = (app) -> {
            final String privilegeName;
            // If this is the first privilege for this app, then maybe (randomly) reuse a privilege name that was defined for another app
            if (allApplications.contains(app) == false && definedPrivilegeNames.size() > 0 && randomBoolean()) {
                privilegeName = randomFrom(definedPrivilegeNames);
            } else {
                privilegeName = randomValueOtherThanMany(definedPrivilegeNames::contains, this::randomPrivilegeName);
            }

            createApplicationPrivilege(app, privilegeName, randomArray(1, 4, String[]::new, () -> randomActionName()));
            allApplications.add(app);
            definedPrivilegeNames.add(privilegeName);
        };

        // Create 0 or more application privileges for this application
        for (int i = randomIntBetween(0, 2); i > 0; i--) {
            createApplicationPrivilege.accept(mainApplication);
        }

        // Create 0 or more application privileges for other applications
        for (int i = randomIntBetween(0, 3); i > 0; i--) {
            createApplicationPrivilege.accept(randomValueOtherThan(mainApplication, this::randomApplicationName));
        }

        // Define a role with all privileges (by wildcard) for this application
        var roleName = randomAlphaOfLengthBetween(6, 10);
        var singleAppOnly = randomBoolean();
        createRole(roleName, singleAppOnly ? mainApplication : "*", new String[] { "*" }, new String[] { "*" });

        final Set<String> allRoles = new HashSet<>();
        allRoles.add(roleName);

        // Create 0 or more additional roles with privileges for one of the applications
        for (int i = randomIntBetween(0, 3); i > 0; i--) {
            var extraRoleName = randomValueOtherThanMany(allRoles::contains, () -> randomAlphaOfLengthBetween(8, 16));
            final String privilegeName = definedPrivilegeNames.size() > 0 && randomBoolean()
                ? randomFrom(definedPrivilegeNames) // This may or may not correspond to the application we pick. Both are valid tests
                : randomPrivilegeName();
            createRole(
                extraRoleName,
                randomFrom(allApplications),
                new String[] { privilegeName },
                new String[] { "data/" + randomAlphaOfLength(6) }
            );
            allRoles.add(extraRoleName);
        }

        // Create a user with all (might be 1 or more) of the roles
        var username = randomAlphaOfLengthBetween(8, 12);
        var password = new SecureString(randomAlphaOfLength(12).toCharArray());
        createUser(username, password, allRoles);

        // Assert that has_privileges returns true for any arbitrary privilege or action in that application
        var reqOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(username, password))
            .build();

        final String testPrivilege;
        if (randomBoolean() && definedPrivilegeNames.size() > 0) {
            testPrivilege = randomFrom(definedPrivilegeNames);
        } else if (randomBoolean()) {
            testPrivilege = randomPrivilegeName();
        } else {
            testPrivilege = randomActionName();
        }
        var testResource = randomAlphaOfLengthBetween(4, 12);

        {
            final List<ResourcePrivileges> shouldHavePrivileges = hasPrivilege(
                reqOptions,
                mainApplication,
                new String[] { testPrivilege },
                new String[] { testResource }
            );

            assertSinglePrivilege(shouldHavePrivileges, testResource, testPrivilege, true);
        }

        if (singleAppOnly) {
            List<ResourcePrivileges> shouldNotHavePrivileges = hasPrivilege(
                reqOptions,
                randomValueOtherThanMany(allApplications::contains, this::randomApplicationName),
                new String[] { testPrivilege },
                new String[] { testResource }
            );
            assertSinglePrivilege(shouldNotHavePrivileges, testResource, testPrivilege, false);

            if (allApplications.size() > 1) { // there is an app other than the main app
                shouldNotHavePrivileges = hasPrivilege(
                    reqOptions,
                    randomValueOtherThan(mainApplication, () -> randomFrom(allApplications)),
                    new String[] { testPrivilege },
                    new String[] { testResource }
                );
                assertSinglePrivilege(shouldNotHavePrivileges, testResource, testPrivilege, false);
            }
        }
    }

    private void assertSinglePrivilege(
        List<ResourcePrivileges> hasPrivilegesResult,
        String expectedResource,
        String expectedPrivilegeName,
        boolean shoudHavePrivilege
    ) {
        assertThat(hasPrivilegesResult, Matchers.hasSize(1));
        assertThat(hasPrivilegesResult.get(0).getResource(), equalTo(expectedResource));
        assertThat(hasPrivilegesResult.get(0).getPrivileges(), Matchers.hasEntry(expectedPrivilegeName, shoudHavePrivilege));
        assertThat(hasPrivilegesResult.get(0).getPrivileges(), aMapWithSize(1));
    }

    private List<ResourcePrivileges> hasPrivilege(RequestOptions requestOptions, String appName, String[] privileges, String[] resources)
        throws IOException {
        logger.info("Checking privileges: App=[{}] Privileges=[{}] Resources=[{}]", appName, privileges, resources);
        Request req = new Request("POST", "/_security/user/_has_privileges");
        req.setOptions(requestOptions);
        Map<String, Object> body = Map.ofEntries(
            Map.entry(
                "application",
                List.of(
                    Map.ofEntries(
                        Map.entry("application", appName),
                        Map.entry("privileges", List.of(privileges)),
                        Map.entry("resources", List.of(resources))
                    )
                )
            )
        );
        req.setJsonEntity(XContentTestUtils.convertToXContent(body, XContentType.JSON).utf8ToString());
        final Map<String, Object> response = responseAsMap(client().performRequest(req));
        logger.info("Has privileges: [{}]", response);
        final Map<String, Object> privilegesByResource = ObjectPath.eval("application." + appName, response);
        return Stream.of(resources).map(res -> {
            Map<String, Boolean> priv = ObjectPath.eval(res, privilegesByResource);
            return ResourcePrivileges.builder(res).addPrivileges(priv).build();
        }).collect(Collectors.toList());
    }

    private void createUser(String username, SecureString password, Set<String> roles) throws IOException {
        logger.info("Create User [{}] with roles [{}]", username, roles);
        securityClient.putUser(new User(username, roles.toArray(String[]::new)), password);
    }

    private void createRole(String roleName, String applicationName, String[] privileges, String[] resources) throws IOException {
        logger.info(
            "Create role [{}] with privileges App=[{}] Privileges=[{}] Resources=[{}]",
            roleName,
            applicationName,
            privileges,
            resources
        );
        securityClient.putRole(
            new RoleDescriptor(
                roleName,
                new String[0], // cluster
                new RoleDescriptor.IndicesPrivileges[0],
                new RoleDescriptor.ApplicationResourcePrivileges[] {
                    RoleDescriptor.ApplicationResourcePrivileges.builder()
                        .application(applicationName)
                        .privileges(privileges)
                        .resources(resources)
                        .build() },
                new ConfigurableClusterPrivilege[0],
                new String[0],// run-as
                Map.of(), // metadata
                Map.of() // transient metadata
            )
        );
    }

    private void createApplicationPrivilege(String applicationName, String privilegeName, String[] actions) {
        logger.info("Create app privilege App=[{}] Privilege=[{}] Actions=[{}]", applicationName, privilegeName, actions);
        try {
            securityClient.putApplicationPrivilege(applicationName, privilegeName, actions);
        } catch (IOException e) {
            throw new AssertionError(
                "Failed to create application privilege app=["
                    + applicationName
                    + "], privilege=["
                    + privilegeName
                    + "], actions=["
                    + String.join(",", actions)
                    + "]",
                e
            );
        }
    }

    private String randomApplicationName() {
        return randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 7);
    }

    private String randomPrivilegeName() {
        if (randomBoolean()) {
            return randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(3, 7);
        } else {
            return randomAlphaOfLengthBetween(2, 4).toLowerCase(Locale.ROOT) + randomFrom(".", "_", "-") + randomAlphaOfLengthBetween(2, 6);
        }
    }

    private String randomActionName() {
        return randomAlphaOfLengthBetween(3, 5) + ":" + randomAlphaOfLengthBetween(3, 5);
    }

}
