/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.arrayContaining;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileRolesStoreTests extends ESTestCase {

    public RestrictedIndices restrictedIndices = new RestrictedIndices(Automatons.EMPTY);

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            singletonList(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    (p, c) -> MatchAllQueryBuilder.fromXContent(p)
                )
            )
        );
    }

    public void testParseFile() throws Exception {
        Path path = getDataPath("roles.yml");
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.builder().put(XPackSettings.DLS_FLS_ENABLED.getKey(), true).build(),
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(9));

        RoleDescriptor descriptor = roles.get("role1");
        assertNotNull(descriptor);
        Role role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role1" }));
        assertThat(role.cluster(), notNullValue());
        assertTrue(role.cluster().implies(ClusterPrivilegeResolver.ALL.buildPermission(ClusterPermission.builder()).build()));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(2));
        assertThat(role.runAs(), is(RunAsPermission.NONE));

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
        assertTrue(Operations.subsetOf(IndexPrivilege.READ.getAutomaton(), group.privilege().getAutomaton()));
        assertTrue(Operations.subsetOf(IndexPrivilege.WRITE.getAutomaton(), group.privilege().getAutomaton()));

        descriptor = roles.get("role1.ab");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role1.ab" }));
        assertThat(role.cluster(), notNullValue());
        assertTrue(role.cluster().implies(ClusterPrivilegeResolver.ALL.buildPermission(ClusterPermission.builder()).build()));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(0));
        assertThat(role.runAs(), is(RunAsPermission.NONE));

        descriptor = roles.get("role2");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role2" }));
        assertThat(role.cluster(), notNullValue());
        assertTrue(role.cluster().implies(ClusterPrivilegeResolver.ALL.buildPermission(ClusterPermission.builder()).build()));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices(), is(IndicesPermission.NONE));
        assertThat(role.runAs(), is(RunAsPermission.NONE));

        descriptor = roles.get("role3");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role3" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));
        assertThat(role.runAs(), is(RunAsPermission.NONE));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("/.*_.*/"));
        assertThat(group.privilege(), notNullValue());
        assertTrue(
            Operations.sameLanguage(
                group.privilege().getAutomaton(),
                MinimizationOperations.minimize(
                    Operations.union(IndexPrivilege.READ.getAutomaton(), IndexPrivilege.WRITE.getAutomaton()),
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
                )
            )
        );

        descriptor = roles.get("role4");
        assertNull(descriptor);

        descriptor = roles.get("role_run_as");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role_run_as" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.indices(), is(IndicesPermission.NONE));
        assertThat(role.runAs(), notNullValue());
        assertThat(role.runAs().check("user1"), is(true));
        assertThat(role.runAs().check("user2"), is(true));
        assertThat(role.runAs().check("user" + randomIntBetween(3, 9)), is(false));

        descriptor = roles.get("role_run_as1");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role_run_as1" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.indices(), is(IndicesPermission.NONE));
        assertThat(role.runAs(), notNullValue());
        assertThat(role.runAs().check("user1"), is(true));
        assertThat(role.runAs().check("user2"), is(true));
        assertThat(role.runAs().check("user" + randomIntBetween(3, 9)), is(false));

        descriptor = roles.get("role_fields");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role_fields" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("field_idx"));
        assertThat(group.privilege(), notNullValue());
        assertTrue(Operations.sameLanguage(group.privilege().getAutomaton(), IndexPrivilege.READ.getAutomaton()));
        assertTrue(group.getFieldPermissions().grantsAccessTo("foo"));
        assertTrue(group.getFieldPermissions().grantsAccessTo("boo"));
        assertTrue(group.getFieldPermissions().hasFieldLevelSecurity());

        descriptor = roles.get("role_query");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role_query" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("query_idx"));
        assertThat(group.privilege(), notNullValue());
        assertTrue(Operations.sameLanguage(group.privilege().getAutomaton(), IndexPrivilege.READ.getAutomaton()));
        assertFalse(group.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(group.getQuery(), notNullValue());

        descriptor = roles.get("role_query_fields");
        assertNotNull(descriptor);
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role_query_fields" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.indices(), notNullValue());
        assertThat(role.indices().groups(), notNullValue());
        assertThat(role.indices().groups().length, is(1));

        group = role.indices().groups()[0];
        assertThat(group.indices(), notNullValue());
        assertThat(group.indices().length, is(1));
        assertThat(group.indices()[0], equalTo("query_fields_idx"));
        assertThat(group.privilege(), notNullValue());
        assertTrue(Operations.sameLanguage(group.privilege().getAutomaton(), IndexPrivilege.READ.getAutomaton()));
        assertTrue(group.getFieldPermissions().grantsAccessTo("foo"));
        assertTrue(group.getFieldPermissions().grantsAccessTo("boo"));
        assertTrue(group.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(group.getQuery(), notNullValue());

        assertThat(roles.get("role_query_invalid"), nullValue());
    }

    public void testParseFileWithFLSAndDLSDisabled() throws Exception {
        Path path = getDataPath("roles.yml");
        Logger logger = CapturingLogger.newCapturingLogger(Level.ERROR, null);
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        events.clear();
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.builder().put(XPackSettings.DLS_FLS_ENABLED.getKey(), false).build(),
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(6));
        assertThat(roles.get("role_fields"), nullValue());
        assertThat(roles.get("role_query"), nullValue());
        assertThat(roles.get("role_query_fields"), nullValue());
        assertThat(roles.get("role_query_invalid"), nullValue());

        assertThat(events, hasSize(4));
        assertThat(
            events.get(0),
            startsWith(
                "invalid role definition [role_fields] in roles file ["
                    + path.toAbsolutePath()
                    + "]. document and field level security is not enabled."
            )
        );
        assertThat(
            events.get(1),
            startsWith(
                "invalid role definition [role_query] in roles file ["
                    + path.toAbsolutePath()
                    + "]. document and field level security is not enabled."
            )
        );
        assertThat(
            events.get(2),
            startsWith(
                "invalid role definition [role_query_fields] in roles file ["
                    + path.toAbsolutePath()
                    + "]. document and field level security is not enabled."
            )
        );
        assertThat(
            events.get(3),
            startsWith(
                "invalid role definition [role_query_invalid] in roles file ["
                    + path.toAbsolutePath()
                    + "]. document and field level security is not enabled."
            )
        );
    }

    public void testParseFileWithFLSAndDLSUnlicensed() throws Exception {
        Path path = getDataPath("roles.yml");
        Logger logger = CapturingLogger.newCapturingLogger(Level.WARN, null);
        List<String> events = CapturingLogger.output(logger.getName(), Level.WARN);
        events.clear();
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(false);
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(path, logger, Settings.EMPTY, licenseState, xContentRegistry());
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(9));
        assertNotNull(roles.get("role_fields"));
        assertNotNull(roles.get("role_query"));
        assertNotNull(roles.get("role_query_fields"));

        assertThat(events, hasSize(3));
        assertThat(
            events.get(0),
            startsWith("role [role_fields] uses document and/or field level security, which is not enabled by the current license")
        );
        assertThat(
            events.get(1),
            startsWith("role [role_query] uses document and/or field level security, which is not enabled by the current license")
        );
        assertThat(
            events.get(2),
            startsWith(
                "role [role_query_fields] uses document and/or field level security, which is not enabled by the current " + "license"
            )
        );
    }

    /**
     * This test is mainly to make sure we can read the default roles.yml config
     */
    public void testDefaultRolesFile() throws Exception {
        // TODO we should add the config dir to the resources so we don't copy this stuff around...
        Path path = getDataPath("default_roles.yml");
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.EMPTY,
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(0));
    }

    public void testAutoReload() throws Exception {
        ThreadPool threadPool = null;
        ResourceWatcherService watcherService = null;
        try {
            Path roles = getDataPath("roles.yml");
            Path home = createTempDir();
            Path xpackConf = home.resolve("config");
            Files.createDirectories(xpackConf);
            Path tmp = xpackConf.resolve("roles.yml");
            try (OutputStream stream = Files.newOutputStream(tmp)) {
                Files.copy(roles, stream);
            }

            Settings.Builder builder = Settings.builder().put("resource.reload.interval.high", "100ms").put("path.home", home);
            Settings settings = builder.build();
            Environment env = TestEnvironment.newEnvironment(settings);
            threadPool = new TestThreadPool("test");
            watcherService = new ResourceWatcherService(settings, threadPool);
            final CountDownLatch latch = new CountDownLatch(1);
            final Set<String> modifiedRoles = new HashSet<>();
            FileRolesStore store = new FileRolesStore(settings, env, watcherService, roleSet -> {
                modifiedRoles.addAll(roleSet);
                latch.countDown();
            }, TestUtils.newTestLicenseState(), xContentRegistry());

            Set<RoleDescriptor> descriptors = store.roleDescriptors(Collections.singleton("role1"));
            assertThat(descriptors, notNullValue());
            assertEquals(1, descriptors.size());
            descriptors = store.roleDescriptors(Collections.singleton("role5"));
            assertThat(descriptors, notNullValue());
            assertTrue(descriptors.isEmpty());

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.append("\n");
            }

            watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);
            if (latch.getCount() != 1) {
                fail("Listener should not be called as roles are not changed.");
            }

            descriptors = store.roleDescriptors(Collections.singleton("role1"));
            assertThat(descriptors, notNullValue());
            assertEquals(1, descriptors.size());

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
                writer.newLine();
                writer.newLine();
                writer.newLine();
                writer.append("role5:").append(System.lineSeparator());
                writer.append("  cluster:").append(System.lineSeparator());
                writer.append("    - 'MONITOR'");
            }

            if (latch.await(5, TimeUnit.SECONDS) == false) {
                fail("Waited too long for the updated file to be picked up");
            }

            assertEquals(1, modifiedRoles.size());
            assertTrue(modifiedRoles.contains("role5"));
            final TransportRequest request = mock(TransportRequest.class);
            final Authentication authentication = AuthenticationTestHelper.builder().build();
            descriptors = store.roleDescriptors(Collections.singleton("role5"));
            assertThat(descriptors, notNullValue());
            assertEquals(1, descriptors.size());
            Role role = Role.buildFromRoleDescriptor(
                descriptors.iterator().next(),
                new FieldPermissionsCache(Settings.EMPTY),
                restrictedIndices
            );
            assertThat(role, notNullValue());
            assertThat(role.names(), equalTo(new String[] { "role5" }));
            assertThat(role.cluster().check("cluster:monitor/foo/bar", request, authentication), is(true));
            assertThat(role.cluster().check("cluster:admin/foo/bar", request, authentication), is(false));

            // truncate to remove some
            // Not asserting exact content of the role change set since file truncation and subsequent are not
            // atomic and hence can result in different change set to be reported.
            final CountDownLatch truncateLatch = new CountDownLatch(1);
            store = new FileRolesStore(settings, env, watcherService, roleSet -> {
                if (roleSet.contains("dummy1")) {
                    truncateLatch.countDown();
                }
            }, TestUtils.newTestLicenseState(), xContentRegistry());

            final Set<String> allRolesPreTruncate = store.getAllRoleNames();
            assertTrue(allRolesPreTruncate.contains("role5"));
            // Use a marker role so that when the countdown latch is triggered,
            // we are sure it is triggered by the new file content instead of the initial truncation
            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
                writer.append("role5:").append(System.lineSeparator());
                writer.append("  cluster:").append(System.lineSeparator());
                writer.append("    - 'MONITOR'").append(System.lineSeparator());
                writer.append("dummy1:").append(System.lineSeparator());
                writer.append("  cluster:").append(System.lineSeparator());
                writer.append("    - 'ALL'");
            }

            assertTrue(truncateLatch.await(5, TimeUnit.SECONDS));
            descriptors = store.roleDescriptors(Collections.singleton("role5"));
            assertThat(descriptors, notNullValue());
            assertEquals(1, descriptors.size());
            assertArrayEquals(new String[] { "MONITOR" }, descriptors.iterator().next().getClusterPrivileges());

            // modify
            final Set<String> modifiedFileRolesModified = new HashSet<>();
            final CountDownLatch modifyLatch = new CountDownLatch(1);
            store = new FileRolesStore(settings, env, watcherService, roleSet -> {
                modifiedFileRolesModified.addAll(roleSet);
                if (roleSet.contains("dummy2")) {
                    modifyLatch.countDown();
                }
            }, TestUtils.newTestLicenseState(), xContentRegistry());

            try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
                writer.append("role5:").append(System.lineSeparator());
                writer.append("  cluster:").append(System.lineSeparator());
                writer.append("    - 'ALL'").append(System.lineSeparator());
                writer.append("dummy2:").append(System.lineSeparator());
                writer.append("  cluster:").append(System.lineSeparator());
                writer.append("    - 'ALL'");
            }

            assertTrue(modifyLatch.await(5, TimeUnit.SECONDS));
            assertTrue(modifiedFileRolesModified.contains("role5"));
            descriptors = store.roleDescriptors(Collections.singleton("role5"));
            assertThat(descriptors, notNullValue());
            assertEquals(1, descriptors.size());
            assertArrayEquals(new String[] { "ALL" }, descriptors.iterator().next().getClusterPrivileges());
        } finally {
            if (watcherService != null) {
                watcherService.close();
            }
            terminate(threadPool);
        }
    }

    public void testThatEmptyFileDoesNotResultInLoop() throws Exception {
        Path file = createTempFile();
        Files.write(file, Collections.singletonList("#"), StandardCharsets.UTF_8);
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            file,
            logger,
            Settings.EMPTY,
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );
        assertThat(roles.keySet(), is(empty()));
    }

    public void testThatInvalidRoleDefinitions() throws Exception {
        Path path = getDataPath("invalid_roles.yml");
        Logger logger = CapturingLogger.newCapturingLogger(Level.ERROR, null);
        List<String> entries = CapturingLogger.output(logger.getName(), Level.ERROR);
        entries.clear();
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.EMPTY,
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );
        assertThat(roles.size(), is(1));
        assertThat(roles, hasKey("valid_role"));
        RoleDescriptor descriptor = roles.get("valid_role");
        assertNotNull(descriptor);
        Role role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "valid_role" }));

        assertThat(entries, hasSize(6));
        assertThat(
            entries.get(0),
            startsWith("invalid role definition [fóóbár] in roles file [" + path.toAbsolutePath() + "]. invalid role name")
        );
        assertThat(entries.get(1), startsWith("invalid role definition [role1] in roles file [" + path.toAbsolutePath() + "]"));
        assertThat(entries.get(2), startsWith("failed to parse role [role2]"));
        assertThat(entries.get(3), startsWith("failed to parse role [role3]"));
        assertThat(entries.get(4), startsWith("failed to parse role [role4]"));
        assertThat(entries.get(5), startsWith("failed to parse indices privileges for role [role5]"));
    }

    public void testThatRoleNamesDoesNotResolvePermissions() throws Exception {
        Path path = getDataPath("invalid_roles.yml");
        Logger logger = CapturingLogger.newCapturingLogger(Level.ERROR, null);
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        events.clear();
        Set<String> roleNames = FileRolesStore.parseFileForRoleNames(path, logger);
        assertThat(roleNames.size(), is(6));
        assertThat(roleNames, containsInAnyOrder("valid_role", "role1", "role2", "role3", "role4", "role5"));

        assertThat(events, hasSize(1));
        assertThat(
            events.get(0),
            startsWith("invalid role definition [fóóbár] in roles file [" + path.toAbsolutePath() + "]. invalid role name")
        );
    }

    public void testReservedRoles() throws Exception {
        Logger logger = CapturingLogger.newCapturingLogger(Level.INFO, null);
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        events.clear();
        Path path = getDataPath("reserved_roles.yml");
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.EMPTY,
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(2));

        assertThat(roles, hasKey("admin"));
        assertThat(roles, hasKey("_system"));

        assertThat(events, notNullValue());
        assertThat(events, hasSize(3));
        assertThat(events.get(0), containsString("Role [superuser] is reserved"));
        assertThat(events.get(1), containsString("Role [kibana_system] is reserved"));
        assertThat(events.get(2), containsString("Role [transport_client] is reserved"));
    }

    public void testUsageStats() throws Exception {
        Path roles = getDataPath("roles.yml");
        Path home = createTempDir();
        Path tmp = home.resolve("config/roles.yml");
        Files.createDirectories(tmp.getParent());
        try (OutputStream stream = Files.newOutputStream(tmp)) {
            Files.copy(roles, stream);
        }

        final boolean flsDlsEnabled = randomBoolean();
        Settings settings = Settings.builder()
            .put("resource.reload.interval.high", "500ms")
            .put("path.home", home)
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), flsDlsEnabled)
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        FileRolesStore store = new FileRolesStore(
            settings,
            env,
            mock(ResourceWatcherService.class),
            TestUtils.newTestLicenseState(),
            xContentRegistry()
        );

        Map<String, Object> usageStats = store.usageStats();

        assertThat(usageStats.get("size"), is(flsDlsEnabled ? 9 : 6));
        assertThat(usageStats.get("fls"), is(flsDlsEnabled));
        assertThat(usageStats.get("dls"), is(flsDlsEnabled));
    }

    // test that we can read a role where field permissions are stored in 2.x format (fields:...)
    public void testBWCFieldPermissions() throws IOException {
        Path path = getDataPath("roles2xformat.yml");
        byte[] bytes = Files.readAllBytes(path);
        String roleString = new String(bytes, Charset.defaultCharset());
        RoleDescriptor role = FileRolesStore.parseRoleDescriptor(roleString, path, logger, true, Settings.EMPTY, xContentRegistry());
        RoleDescriptor.IndicesPrivileges indicesPrivileges = role.getIndicesPrivileges()[0];
        assertThat(indicesPrivileges.getGrantedFields(), arrayContaining("foo", "boo"));
        assertNull(indicesPrivileges.getDeniedFields());
    }

}
