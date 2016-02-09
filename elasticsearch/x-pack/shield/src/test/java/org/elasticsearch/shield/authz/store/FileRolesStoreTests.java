/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.store;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.Shield;
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
                .put(XPackPlugin.featureEnabledSetting(Shield.DLS_FLS_FEATURE), true)
                .build());
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(10));

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
        assertThat(group.privilege(), is(IndexPrivilege.CRUD));

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
        assertThat(role, notNullValue());
        assertThat(role.name(), equalTo("role4"));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.Core.NONE));
        assertThat(role.indices(), is(IndicesPermission.Core.NONE));
        assertThat(role.runAs(), is(RunAsPermission.Core.NONE));

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
                .put(XPackPlugin.featureEnabledSetting(Shield.DLS_FLS_FEATURE), false)
                .build());
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(7));
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
        Path path = getDataPath("default_roles.yml");
        Map<String, Role> roles = FileRolesStore.parseFile(path, logger, Settings.EMPTY);
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(8));

        assertThat(roles, hasKey("admin"));
        assertThat(roles, hasKey("power_user"));
        assertThat(roles, hasKey("user"));
        assertThat(roles, hasKey("kibana3"));
        assertThat(roles, hasKey("kibana4"));
        assertThat(roles, hasKey("logstash"));
        assertThat(roles, hasKey("marvel_user"));
        assertThat(roles, hasKey("marvel_agent"));
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
                    .put("shield.authz.store.files.roles", tmp.toAbsolutePath())
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
                writer.append("  cluster: 'MONITOR'");
            }

            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("Waited too long for the updated file to be picked up");
            }

            role = store.role("role5");
            assertThat(role, notNullValue());
            assertThat(role.name(), equalTo("role5"));
            assertThat(role.cluster().check("cluster:monitor/foo/bar"), is(true));
            assertThat(role.cluster().check("cluster:admin/foo/bar"), is(false));

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
        assertThat(entries, hasSize(5));
        assertThat(entries.get(0).text, startsWith("invalid role definition [$dlk39] in roles file [" + path.toAbsolutePath() +
                "]. invalid role name"));
        assertThat(entries.get(1).text, startsWith("invalid role definition [role1] in roles file [" + path.toAbsolutePath() + "]"));
        assertThat(entries.get(2).text, startsWith("invalid role definition [role2] in roles file [" + path.toAbsolutePath() +
                "]. could not resolve cluster privileges [blkjdlkd]"));
        assertThat(entries.get(3).text, startsWith("invalid role definition [role3] in roles file [" + path.toAbsolutePath() +
                "]. [indices] field value must be an array"));
        assertThat(entries.get(4).text, startsWith("invalid role definition [role4] in roles file [" + path.toAbsolutePath() +
                "]. could not resolve indices privileges [al;kjdlkj;lkj]"));
    }

    public void testThatRoleNamesDoesNotResolvePermissions() throws Exception {
        Path path = getDataPath("invalid_roles.yml");
        CapturingLogger logger = new CapturingLogger(CapturingLogger.Level.ERROR);
        Set<String> roleNames = FileRolesStore.parseFileForRoleNames(path, logger);
        assertThat(roleNames.size(), is(5));
        assertThat(roleNames, containsInAnyOrder("valid_role", "role1", "role2", "role3", "role4"));

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
        assertThat(messages, hasSize(2));
        // the system role will always be checked first
        assertThat(messages.get(0).text, containsString("role [__es_system_role] is reserved"));
        assertThat(messages.get(1).text, containsString("role [__es_internal_role] is reserved"));
    }
}
