/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.audit.logfile.CapturingLogger;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authz.permission.ClusterPermission;
import org.elasticsearch.shield.authz.permission.IndicesPermission;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.authz.permission.RunAsPermission;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.IndexPrivilege;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 *
 */
public class FileRolesStoreTests extends ESTestCase {

    public void testParseFile() throws Exception {
        Path path = getDataPath("roles.yml");
        Map<String, Role> roles = FileRolesStore.parseFile(path, logger, Settings.builder()
                .put(XPackPlugin.featureEnabledSetting(Security.DLS_FLS_FEATURE), true)
                .build());
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(9));

        Role role = roles.get("role1");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role1"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster().privilege(), is(ClusterPrivilege.ALL));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(2));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));

        IndicesPermission.Group group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(2));
        assertThat(group.indices()[0], equalTo("idx1"));
        assertThat(group.indices()[1], equalTo("idx2"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege(), is(IndexPrivilege.READ));

        group = role.indices().groups()[1];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("idx3"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().implies(IndexPrivilege.READ), is(true));
        assertThat(group.privilege().implies(IndexPrivilege.WRITE),is(true));

        role = roles.get("role1.ab");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role1.ab"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster().privilege(), is(ClusterPrivilege.ALL));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(0));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));

        role = roles.get("role2");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role2"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster().privilege(), is(ClusterPrivilege.ALL)); // MONITOR is collapsed into ALL
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices(), is(IndicesPermission.Core.NONE));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));

        role = roles.get("role3");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role3"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("/.*_.*/"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().isAlias(IndexPrivilege.union(IndexPrivilege.READ, IndexPrivilege.WRITE)), is(true));

        role = roles.get("role4");
        assertThat(role, nullValue());

        role = roles.get("role_run_as");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role_run_as"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.indices(), is(IndicesPermission.Core.NONE));
        assertThat(role.runAs(), notNullValue());
        assertThat(role.runAs().check("user1"), is(true));
        assertThat(role.runAs().check("user2"), is(true));
        assertThat(role.runAs().check("user" + randomIntBetween(3, 9)), is(false));

        role = roles.get("role_run_as1");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role_run_as1"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.indices(), is(IndicesPermission.Core.NONE));
        assertThat(role.runAs(), notNullValue());
        assertThat(role.runAs().check("user1"), is(true));
        assertThat(role.runAs().check("user2"), is(true));
        assertThat(role.runAs().check("user" + randomIntBetween(3, 9)), is(false));

        role = roles.get("role_fields");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role_fields"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("field_idx"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().isAlias(IndexPrivilege.READ), is(true));
        assertThat(group.getFields(), contains("foo", "boo"));

        role = roles.get("role_query");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role_query"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("query_idx"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().isAlias(IndexPrivilege.READ), is(true));
        assertThat(group.getFields(), nullValue());
        assertThat(group.getQuery(), notNullValue());

        role = roles.get("role_query_fields");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role_query_fields"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("query_fields_idx"));
        assertThat(group.privilege(), notNullValue());
        assertThat(group.privilege().isAlias(IndexPrivilege.READ), is(true));
        assertThat(group.getFields(), contains("foo", "boo"));
        assertThat(group.getQuery(), notNullValue());
    }

    public void testParseFileWithFLSAndDLSDisabled() throws Exception {
        Path path = getDataPath("roles.yml");
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.ERROR);
        Map<String, Role> roles = FileRolesStore.parseFile(path, logger, Settings.builder()
                .put(XPackPlugin.featureEnabledSetting(Security.DLS_FLS_FEATURE), false)
                .build());
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(6));
        assertThat(roles.get("role_fields"), nullValue());
        assertThat(roles.get("role_query"), nullValue());
        assertThat(roles.get("role_query_fields"), nullValue());

        List<CapturingLogger.Msg> entries = logger.output(CapturingLogger.Level.ERROR);
        assertThat(entries, hasSize(3));
        assertThat(entries.get(0).text, startsWith("invalid role definition [role_fields] in roles file [" + path.toAbsolutePath() +
                "]. document and field level security is not enabled."));
        assertThat(entries.get(1).text, startsWith("invalid role definition [role_query] in roles file [" + path.toAbsolutePath() +
                "]. document and field level security is not enabled."));
        assertThat(entries.get(2).text, startsWith("invalid role definition [role_query_fields] in roles file [" + path.toAbsolutePath() +
                "]. document and field level security is not enabled."));
    }

    /**
     * This test is mainly to make sure we can read the default roles.yml config
     */
    public void testDefaultRolesFile() throws Exception {
        // TODO we should add the config dir to the resources so we don't copy this stuff around...
        Path path = getDataPath("default_roles.yml");
        Map<String, Role> roles = FileRolesStore.parseFile(path, logger, Settings.EMPTY);
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(3));

        assertThat(roles, hasKey("logstash"));
        assertThat(roles, hasKey("monitoring_user"));
        assertThat(roles, hasKey("remote_monitoring_agent"));
    }

    public void testAutoReload() throws Exception {
        ThreadPool threadPool = null;
        ResourceWatcherService watcherService = null;
        try {
            Path roles = getDataPath("roles.yml");
            Path tmp = createTempFile();
            try (OutputStream stream = Files.newOutputStream(tmp)) {
                Files.copy(roles, stream);
            }

            Settings settings = Settings.builder()
                    .put("resource.reload.interval.high", "500ms")
                    .put(FileRolesStore.ROLES_FILE_SETTING.getKey(), tmp.toAbsolutePath())
                    .put("path.home", createTempDir())
                    .build();

            Environment env = new Environment(settings);
            threadPool = new ThreadPool("test");
            watcherService = new ResourceWatcherService(settings, threadPool);
            final CountDownLatch latch = new CountDownLatch(1);
            FileRolesStore store = new FileRolesStore(settings, env, watcherService, new RefreshListener() {
                @Override
                public void onRefresh() {
                    latch.countDown();
                }
            });
            store.start();

            Role role = store.role("role1");
            assertThat(role, notNullValue());
            role = store.role("role5");
            assertThat(role, nullValue());

            watcherService.start();

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.newLine();
                writer.newLine();
                writer.append("role5:").append(System.lineSeparator());
                writer.append("  cluster:").append(System.lineSeparator());
                writer.append("    - 'MONITOR'");
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Waited too long for the updated file to be picked up");
            }

            role = store.role("role5");
            assertThat(role, notNullValue());
            assertThat(role.name(), equalTo("role5"));
            assertThat(role.cluster().check("cluster:monitor/foo/bar", null, null), is(true));
            assertThat(role.cluster().check("cluster:admin/foo/bar", null, null), is(false));

        } finally {
            if (watcherService != null) {
                watcherService.stop();
            }
            terminate(threadPool);
        }
    }

    public void testThatEmptyFileDoesNotResultInLoop() throws Exception {
        Path file = createTempFile();
        Files.write(file, Collections.singletonList("#"), StandardCharsets.UTF_8);
        Map<String, Role> roles = FileRolesStore.parseFile(file, logger, Settings.EMPTY);
        assertThat(roles.keySet(), is(empty()));
    }

    public void testThatInvalidRoleDefinitions() throws Exception {
        Path path = getDataPath("invalid_roles.yml");
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.ERROR);
        Map<String, Role> roles = FileRolesStore.parseFile(path, logger, Settings.EMPTY);
        assertThat(roles.size(), is(1));
        assertThat(roles, hasKey("valid_role"));
        Role role = roles.get("valid_role");
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("valid_role"));

        List<CapturingLogger.Msg> entries = logger.output(CapturingLogger.Level.ERROR);
        assertThat(entries, hasSize(6));
        assertThat(entries.get(0).text, startsWith("invalid role definition [$dlk39] in roles file [" + path.toAbsolutePath() +
                "]. invalid role name"));
        assertThat(entries.get(1).text, startsWith("invalid role definition [role1] in roles file [" + path.toAbsolutePath() + "]"));
        assertThat(entries.get(2).text, startsWith("failed to parse role [role2]"));
        assertThat(entries.get(3).text, startsWith("failed to parse role [role3]"));
        assertThat(entries.get(4).text, startsWith("failed to parse role [role4]"));
        assertThat(entries.get(5).text, startsWith("failed to parse indices privileges for role [role5]"));
    }

    public void testThatRoleNamesDoesNotResolvePermissions() throws Exception {
        Path path = getDataPath("invalid_roles.yml");
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.ERROR);
        Set<String> roleNames = FileRolesStore.parseFileForRoleNames(path, logger);
        assertThat(roleNames.size(), is(6));
        assertThat(roleNames, containsInAnyOrder("valid_role", "role1", "role2", "role3", "role4", "role5"));

        List<CapturingLogger.Msg> entries = logger.output(CapturingLogger.Level.ERROR);
        assertThat(entries, hasSize(1));
        assertThat(entries.get(0).text, startsWith("invalid role definition [$dlk39] in roles file [" + path.toAbsolutePath() +
                "]. invalid role name"));
    }

    public void testReservedRoles() throws Exception {

        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.INFO);

        Path path = getDataPath("reserved_roles.yml");
        Map<String, Role> roles = FileRolesStore.parseFile(path, logger, Settings.EMPTY);
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(1));

        assertThat(roles, hasKey("admin"));

        List<CapturingLogger.Msg> messages = logger.output(CapturingLogger.Level.WARN);
        assertThat(messages, notNullValue());
        assertThat(messages, hasSize(4));
        // the system role will always be checked first
        assertThat(messages.get(0).text, containsString("role [__es_system_role] is reserved"));
        assertThat(messages.get(1).text, containsString("role [superuser] is reserved"));
        assertThat(messages.get(2).text, containsString("role [kibana] is reserved"));
        assertThat(messages.get(3).text, containsString("role [transport_client] is reserved"));
    }
}
