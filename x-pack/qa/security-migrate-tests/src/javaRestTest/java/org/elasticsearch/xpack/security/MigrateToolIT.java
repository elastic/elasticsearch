/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.esnative.ESNativeRealmMigrateTool;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration tests for the {@code elasticsearch-migrate} shell command
 */
public class MigrateToolIT extends MigrateToolTestCase {

    @Before
    public void setupUpTest() throws Exception {
        Client client = getClient();
        SecurityClient c = new SecurityClient(client);

        // Add an existing user so the tool will skip it
        PutUserResponse pur = c.preparePutUser("existing", "s3kirt".toCharArray(), Hasher.BCRYPT, "role1", "user").get();
        assertTrue(pur.created());
    }

    public void testRunMigrateTool() throws Exception {
        final String testConfigDir = System.getProperty("tests.config.dir");
        logger.info("--> CONF: {}", testConfigDir);
        final Path configPath = PathUtils.get(testConfigDir);
        Settings settings = Settings.builder().put("path.home", configPath.getParent()).build();
        // Cluster should already be up
        String url = "http://" + getHttpURL().split(",")[0];
        logger.info("--> using URL: {}", url);
        MockTerminal t = new MockTerminal();
        ESNativeRealmMigrateTool.MigrateUserOrRoles muor = new ESNativeRealmMigrateTool.MigrateUserOrRoles();
        OptionParser parser = muor.getParser();

        OptionSet options = parser.parse("-u", "test_admin", "-p", "x-pack-test-password", "-U", url);
        muor.execute(t, options, new Environment(settings, configPath));

        logger.info("--> output:\n{}", t.getOutput());

        Client client = getClient();
        SecurityClient c = new SecurityClient(client);

        // Check that the migrated user can be retrieved
        GetUsersResponse resp = c.prepareGetUsers("bob").get();
        assertTrue("user 'bob' should exist", resp.hasUsers());
        User bob = resp.users()[0];
        assertEquals(bob.principal(), "bob");
        assertArrayEquals(bob.roles(), new String[] { "actual_role" });

        // Make sure the existing user did not change
        resp = c.prepareGetUsers("existing").get();
        assertTrue("user should exist", resp.hasUsers());
        User existing = resp.users()[0];
        assertEquals(existing.principal(), "existing");
        assertArrayEquals(existing.roles(), new String[] { "role1", "user" });

        // Make sure the "actual_role" made it in and is correct
        GetRolesResponse roleResp = c.prepareGetRoles().names("actual_role").get();
        assertTrue("role should exist", roleResp.hasRoles());
        RoleDescriptor rd = roleResp.roles()[0];
        assertNotNull(rd);
        assertEquals(rd.getName(), "actual_role");
        assertArrayEquals(rd.getClusterPrivileges(), new String[] { "monitor" });
        assertArrayEquals(rd.getRunAs(), new String[] { "joe" });
        RoleDescriptor.IndicesPrivileges[] ips = rd.getIndicesPrivileges();
        assertEquals(ips.length, 2);
        for (RoleDescriptor.IndicesPrivileges ip : ips) {
            final FieldPermissions fieldPermissions = new FieldPermissions(
                new FieldPermissionsDefinition(ip.getGrantedFields(), ip.getDeniedFields())
            );
            if (Arrays.equals(ip.getIndices(), new String[] { "index1", "index2" })) {
                assertArrayEquals(ip.getPrivileges(), new String[] { "read", "write", "create_index", "indices:admin/refresh" });
                assertTrue(fieldPermissions.hasFieldLevelSecurity());
                assertTrue(fieldPermissions.grantsAccessTo("bar"));
                assertTrue(fieldPermissions.grantsAccessTo("foo"));
                assertNotNull(ip.getQuery());
                assertThat(
                    ip.getQuery().iterator().next().utf8ToString(),
                    containsString("{\"bool\":{\"must_not\":{\"match\":{\"hidden\":true}}}}")
                );
            } else {
                assertArrayEquals(ip.getIndices(), new String[] { "*" });
                assertArrayEquals(ip.getPrivileges(), new String[] { "read" });
                assertFalse(fieldPermissions.hasFieldLevelSecurity());
                assertNull(ip.getQuery());
            }
        }

        // Check that bob can access the things the "actual_role" says he can
        String token = basicAuthHeaderValue("bob", new SecureString("x-pack-test-password".toCharArray()));
        // Create "index1" index and try to search from it as "bob"
        client.filterWithHeader(Collections.singletonMap("Authorization", token)).admin().indices().prepareCreate("index1").get();
        // Wait for the index to be ready so it doesn't fail if no shards are initialized
        client.admin()
            .cluster()
            .health(
                Requests.clusterHealthRequest("index1")
                    .timeout(TimeValue.timeValueSeconds(30))
                    .waitForYellowStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true)
            )
            .actionGet();
        client.filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("index1").get();
    }
}
