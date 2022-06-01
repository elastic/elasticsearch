/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.SecureString;
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
        var appName = randomApplicationName();

        final Set<String> allApplications = new HashSet<>();
        CheckedBiConsumer<String, Integer, IOException> createApplicationPrivilege = (app, index) -> {
            createApplicationPrivilege(app, "priv_" + index, randomArray(1, 4, String[]::new, () -> randomActionName()));
            allApplications.add(appName);
        };

        for (int i = randomIntBetween(0, 2); i > 0; i--) {
            // Create 0 or more application privileges for this application
            createApplicationPrivilege.accept(appName, i);
        }

        for (int i = randomIntBetween(0, 3); i > 0; i--) {
            // Create 0 or more application privileges for other applications
            createApplicationPrivilege.accept(randomValueOtherThan(appName, this::randomApplicationName), i);
        }

        // Define a role with all privileges (by wildcard) for this application
        var roleName = randomAlphaOfLengthBetween(6, 10);
        createRole(roleName, "*", new String[] { "*" }, new String[] { "*" });

        final Set<String> allRoles = new HashSet<>();
        allRoles.add(roleName);

        for (int i = randomIntBetween(0, 3); i > 0; i--) {
            // Create 0 or more additional roles with privileges for one of the applications
            var extraRoleName = randomValueOtherThanMany(allRoles::contains, () -> randomAlphaOfLengthBetween(8, 16));
            createRole(
                extraRoleName,
                randomFrom(allApplications),
                new String[] { "priv_" + i },
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

        var testPrivilege = randomBoolean()
            ? randomAlphaOfLengthBetween(4, 12)  // privilege name
            : randomActionName() // action name
        ;
        var testResource = randomAlphaOfLengthBetween(4, 12);

        {
            final List<ResourcePrivileges> shouldHavePrivileges = hasPrivilege(
                reqOptions,
                appName,
                new String[] { testPrivilege },
                new String[] { testResource }
            );

            assertThat(shouldHavePrivileges, Matchers.hasSize(1));
            assertThat(shouldHavePrivileges.get(0).getResource(), equalTo(testResource));
            assertThat(shouldHavePrivileges.get(0).getPrivileges(), Matchers.hasEntry(testPrivilege, true));
            assertThat(shouldHavePrivileges.get(0).getPrivileges(), aMapWithSize(1));
        }

        if (false)        {
            final List<ResourcePrivileges> shouldNotHavePrivileges = hasPrivilege(
                reqOptions,
                randomValueOtherThanMany(allApplications::contains, this::randomApplicationName),
                new String[] { testPrivilege },
                new String[] { testResource }
            );

            assertThat(shouldNotHavePrivileges, Matchers.hasSize(1));
            assertThat(shouldNotHavePrivileges.get(0).getResource(), equalTo(testResource));
            assertThat(shouldNotHavePrivileges.get(0).getPrivileges(), Matchers.hasEntry(testPrivilege, false));
            assertThat(shouldNotHavePrivileges.get(0).getPrivileges(), aMapWithSize(1));
        }
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

    private String randomActionName() {
        return randomAlphaOfLengthBetween(3, 5) + ":" + randomAlphaOfLengthBetween(3, 5);
    }

}
