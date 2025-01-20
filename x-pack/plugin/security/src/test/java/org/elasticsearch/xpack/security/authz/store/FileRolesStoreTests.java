/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
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
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.security.authz.FileRoleValidator;
import org.junit.BeforeClass;

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

    @BeforeClass
    public static void setUpClass() {
        // Initialize the reserved roles store so that static fields are populated.
        // In production code, this is guaranteed by how components are initialized by the Security plugin
        new ReservedRolesStore();
    }

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
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(11));

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
        assertTrue(AutomatonTestUtil.subsetOf(IndexPrivilege.READ.getAutomaton(), group.privilege().getAutomaton()));
        assertTrue(AutomatonTestUtil.subsetOf(IndexPrivilege.WRITE.getAutomaton(), group.privilege().getAutomaton()));

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
            AutomatonTestUtil.sameLanguage(
                group.privilege().getAutomaton(),
                Operations.determinize(
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
        assertTrue(AutomatonTestUtil.sameLanguage(group.privilege().getAutomaton(), IndexPrivilege.READ.getAutomaton()));
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
        assertTrue(AutomatonTestUtil.sameLanguage(group.privilege().getAutomaton(), IndexPrivilege.READ.getAutomaton()));
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
        assertTrue(AutomatonTestUtil.sameLanguage(group.privilege().getAutomaton(), IndexPrivilege.READ.getAutomaton()));
        assertTrue(group.getFieldPermissions().grantsAccessTo("foo"));
        assertTrue(group.getFieldPermissions().grantsAccessTo("boo"));
        assertTrue(group.getFieldPermissions().hasFieldLevelSecurity());
        assertThat(group.getQuery(), notNullValue());

        assertThat(roles.get("role_query_invalid"), nullValue());

        descriptor = roles.get("role_with_description");
        assertNotNull(descriptor);
        assertThat(descriptor.getDescription(), is(equalTo("Allows all security-related operations!")));
        role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "role_with_description" }));
        assertThat(role.cluster(), notNullValue());
        assertThat(role.cluster().privileges(), equalTo(Set.of(ClusterPrivilegeResolver.MANAGE_SECURITY)));
        assertThat(role.indices(), is(IndicesPermission.NONE));
        assertThat(role.runAs(), is(RunAsPermission.NONE));

    }

    public void testParseFileWithRemoteIndicesAndCluster() throws IllegalAccessException, IOException {
        final Logger logger = CapturingLogger.newCapturingLogger(Level.ERROR, null);
        final List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        events.clear();
        final Path path = getDataPath("roles_with_remote_indices_and_cluster.yml");
        final Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.builder().put(XPackSettings.DLS_FLS_ENABLED.getKey(), true).build(),
            TestUtils.newTestLicenseState(),
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(2));

        final RoleDescriptor roleDescriptor = roles.get("role");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getRemoteIndicesPrivileges().length, equalTo(1));
        final RoleDescriptor.RemoteIndicesPrivileges remoteIndicesPrivileges = roleDescriptor.getRemoteIndicesPrivileges()[0];
        assertThat(remoteIndicesPrivileges.remoteClusters(), arrayContaining("remote1", "*-remote"));
        assertThat(remoteIndicesPrivileges.indicesPrivileges().getIndices(), arrayContaining("idx1", "idx2"));
        assertThat(remoteIndicesPrivileges.indicesPrivileges().getPrivileges(), arrayContaining("READ"));
        assertThat(remoteIndicesPrivileges.indicesPrivileges().allowRestrictedIndices(), is(false));
        assertThat(remoteIndicesPrivileges.indicesPrivileges().getQuery(), nullValue());

        RemoteClusterPermissions remoteClusterPermissions = roleDescriptor.getRemoteClusterPermissions();
        remoteClusterPermissions.validate(); // no exception should be thrown
        assertThat(remoteClusterPermissions.groups().size(), equalTo(2));
        assertThat(remoteClusterPermissions.groups().get(0).remoteClusterAliases(), arrayContaining("remote0"));
        assertThat(remoteClusterPermissions.groups().get(1).remoteClusterAliases(), arrayContaining("remote1"));
        assertThat(remoteClusterPermissions.groups().get(0).clusterPrivileges(), arrayContaining("monitor_enrich"));
        assertThat(remoteClusterPermissions.groups().get(1).clusterPrivileges(), arrayContaining("monitor_enrich"));

        final RoleDescriptor roleDescriptor2 = roles.get("role_with_fls_dls");
        assertNotNull(roleDescriptor2);
        assertThat(roleDescriptor2.getRemoteIndicesPrivileges().length, equalTo(1));
        final RoleDescriptor.RemoteIndicesPrivileges remoteIndicesPrivileges4 = roleDescriptor2.getRemoteIndicesPrivileges()[0];
        assertThat(remoteIndicesPrivileges4.remoteClusters(), arrayContaining("*"));
        assertThat(remoteIndicesPrivileges4.indicesPrivileges().getIndices(), arrayContaining("idx1"));
        assertThat(remoteIndicesPrivileges4.indicesPrivileges().getPrivileges(), arrayContaining("READ"));
        assertThat(remoteIndicesPrivileges4.indicesPrivileges().allowRestrictedIndices(), is(false));
        assertThat(remoteIndicesPrivileges4.indicesPrivileges().getGrantedFields(), arrayContaining("foo", "boo"));
        assertThat(remoteIndicesPrivileges4.indicesPrivileges().getDeniedFields(), arrayContaining("boo"));
        assertThat(remoteIndicesPrivileges4.indicesPrivileges().getQuery().utf8ToString(), equalTo("{ \"match_all\": {} }"));

        remoteClusterPermissions = roleDescriptor2.getRemoteClusterPermissions();
        assertThat(remoteClusterPermissions.groups().size(), equalTo(0));
        assertThat(remoteClusterPermissions, equalTo(RemoteClusterPermissions.NONE));

        assertThat(roles.get("invalid_role_missing_clusters"), nullValue());
        assertThat(roles.get("invalid_role_empty_names"), nullValue());
        assertThat(roles.get("invalid_role_empty_privileges"), nullValue());
        assertThat(roles.get("invalid_role_missing_remote_clusters"), nullValue());
        assertThat(roles.get("invalid_role_bad_priv_remote_clusters"), nullValue());
        assertThat(events, hasSize(5));
        assertThat(
            events.get(0),
            startsWith(
                "failed to parse remote indices privileges for role [invalid_role_missing_clusters]. "
                    + "missing required [clusters] field. skipping role..."
            )
        );
        assertThat(
            events.get(1),
            startsWith(
                "failed to parse indices privileges for role [invalid_role_empty_names]. "
                    + "expected field [names] value to be a string or an array of strings, but found [VALUE_NULL] instead. skipping role..."
            )
        );
        assertThat(
            events.get(2),
            startsWith(
                "failed to parse indices privileges for role [invalid_role_empty_privileges]. "
                    + "missing required [privileges] field. skipping role..."
            )
        );
        assertThat(
            events.get(3),
            startsWith(
                "failed to parse remote_cluster for role [invalid_role_missing_remote_clusters]. "
                    + "expected field [remote_cluster] value to be an array"
            )
        );
        assertThat(
            events.get(4),
            startsWith(
                "failed to parse remote_cluster for role [invalid_role_bad_priv_remote_clusters]. "
                    + "[monitor_enrich, monitor_stats] are the only values allowed for [privileges] within [remote_cluster]. "
                    + "Found [junk]. skipping role..."
            )
        );
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
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(8));
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
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.EMPTY,
            licenseState,
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(11));
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
            xContentRegistry(),
            new FileRoleValidator.Default()
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
            }, new FileRoleValidator.Default(), TestUtils.newTestLicenseState(), xContentRegistry());

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
            }, new FileRoleValidator.Default(), TestUtils.newTestLicenseState(), xContentRegistry());

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
            }, new FileRoleValidator.Default(), TestUtils.newTestLicenseState(), xContentRegistry());

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
            xContentRegistry(),
            new FileRoleValidator.Default()
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
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        assertThat(roles.size(), is(1));
        assertThat(roles, hasKey("valid_role"));
        RoleDescriptor descriptor = roles.get("valid_role");
        assertNotNull(descriptor);
        Role role = Role.buildFromRoleDescriptor(descriptor, new FieldPermissionsCache(Settings.EMPTY), restrictedIndices);
        assertThat(role, notNullValue());
        assertThat(role.names(), equalTo(new String[] { "valid_role" }));

        assertThat(entries, hasSize(8));
        assertThat(
            entries.get(0),
            startsWith("invalid role definition [fóóbár] in roles file [" + path.toAbsolutePath() + "]. invalid role name")
        );
        assertThat(entries.get(1), startsWith("invalid role definition [role1] in roles file [" + path.toAbsolutePath() + "]"));
        assertThat(entries.get(2), startsWith("failed to parse role [role2]"));
        assertThat(entries.get(3), startsWith("failed to parse role [role3]"));
        assertThat(entries.get(4), startsWith("failed to parse role [role4]"));
        assertThat(entries.get(5), startsWith("failed to parse indices privileges for role [role5]"));
        assertThat(entries.get(6), startsWith("failed to parse role [role6]. unexpected field [restriction]"));
        assertThat(
            entries.get(7),
            startsWith("invalid role definition [role7] in roles file [" + path.toAbsolutePath() + "]. invalid description")
        );
    }

    public void testThatRoleNamesDoesNotResolvePermissions() throws Exception {
        Path path = getDataPath("invalid_roles.yml");
        Logger logger = CapturingLogger.newCapturingLogger(Level.ERROR, null);
        List<String> events = CapturingLogger.output(logger.getName(), Level.ERROR);
        events.clear();
        Set<String> roleNames = FileRolesStore.parseFileForRoleNames(path, logger);
        assertThat(roleNames.size(), is(8));
        assertThat(roleNames, containsInAnyOrder("valid_role", "role1", "role2", "role3", "role4", "role5", "role6", "role7"));

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
            xContentRegistry(),
            new FileRoleValidator.Default()
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
            xContentRegistry(),
            new FileRoleValidator.Default()
        );

        Map<String, Object> usageStats = store.usageStats();

        assertThat(usageStats.get("size"), is(flsDlsEnabled ? 11 : 8));
        assertThat(usageStats.get("remote_indices"), is(1L));
        assertThat(usageStats.get("remote_cluster"), is(1L));
        assertThat(usageStats.get("fls"), is(flsDlsEnabled));
        assertThat(usageStats.get("dls"), is(flsDlsEnabled));
    }

    public void testExists() throws Exception {
        Path path = getDataPath("roles.yml");
        Path home = createTempDir();
        Path tmp = home.resolve("config/roles.yml");
        Files.createDirectories(tmp.getParent());
        try (OutputStream stream = Files.newOutputStream(tmp)) {
            Files.copy(path, stream);
        }

        Settings settings = Settings.builder().put("resource.reload.interval.high", "500ms").put("path.home", home).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        FileRolesStore store = new FileRolesStore(
            settings,
            env,
            mock(ResourceWatcherService.class),
            TestUtils.newTestLicenseState(),
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        Map<String, RoleDescriptor> roles = FileRolesStore.parseFile(
            path,
            logger,
            Settings.builder().put(XPackSettings.DLS_FLS_ENABLED.getKey(), true).build(),
            TestUtils.newTestLicenseState(),
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        assertThat(roles, notNullValue());
        assertThat(roles.size(), is(11));
        for (var role : roles.keySet()) {
            assertThat(store.exists(role), is(true));
        }
        assertThat(store.exists(randomValueOtherThanMany(roles::containsKey, () -> randomAlphaOfLength(20))), is(false));
    }

    // test that we can read a role where field permissions are stored in 2.x format (fields:...)
    public void testBWCFieldPermissions() throws IOException {
        Path path = getDataPath("roles2xformat.yml");
        byte[] bytes = Files.readAllBytes(path);
        String roleString = new String(bytes, Charset.defaultCharset());
        RoleDescriptor role = FileRolesStore.parseRoleDescriptor(
            roleString,
            path,
            logger,
            true,
            Settings.EMPTY,
            xContentRegistry(),
            new FileRoleValidator.Default()
        );
        RoleDescriptor.IndicesPrivileges indicesPrivileges = role.getIndicesPrivileges()[0];
        assertThat(indicesPrivileges.getGrantedFields(), arrayContaining("foo", "boo"));
        assertNull(indicesPrivileges.getDeniedFields());
    }

}
