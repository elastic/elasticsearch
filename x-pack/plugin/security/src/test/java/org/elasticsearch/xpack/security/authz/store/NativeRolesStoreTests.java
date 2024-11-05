/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleRestrictionTests;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissionGroup;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.authz.ReservedRoleNameChecker;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomApplicationPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomClusterPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRemoteIndicesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRoleDescriptorMetadata;
import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.ROLE_REMOTE_CLUSTER_PRIVS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NativeRolesStoreTests extends ESTestCase {

    private ThreadPool threadPool;

    private final Client client = mock(Client.class);

    @Before
    public void beforeNativeRoleStoreTests() {
        threadPool = new TestThreadPool("index audit trail update mapping tests");
        when(client.threadPool()).thenReturn(threadPool);
        when(client.prepareIndex(SECURITY_MAIN_ALIAS)).thenReturn(new IndexRequestBuilder(client));
        when(client.prepareUpdate(any(), any())).thenReturn(new UpdateRequestBuilder(client));
        when(client.prepareDelete(any(), any())).thenReturn(new DeleteRequestBuilder(client, SECURITY_MAIN_ALIAS));
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private NativeRolesStore createRoleStoreForTest() {
        return createRoleStoreForTest(Settings.builder().build());
    }

    private NativeRolesStore createRoleStoreForTest(Settings settings) {
        new ReservedRolesStore(Set.of("superuser"));
        final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(TransportVersion.current());
        final SecuritySystemIndices systemIndices = new SecuritySystemIndices(settings);
        final FeatureService featureService = mock(FeatureService.class);
        systemIndices.init(client, featureService, clusterService);
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();
        // Create the index
        securityIndex.clusterChanged(new ClusterChangedEvent("source", getClusterStateWithSecurityIndex(), getEmptyClusterState()));

        return new NativeRolesStore(
            settings,
            client,
            TestUtils.newTestLicenseState(),
            securityIndex,
            clusterService,
            mock(FeatureService.class),
            new ReservedRoleNameChecker.Default(),
            mock(NamedXContentRegistry.class)
        );
    }

    private void putRole(NativeRolesStore rolesStore, RoleDescriptor roleDescriptor, ActionListener<Boolean> actionListener)
        throws IOException {
        if (randomBoolean()) {
            rolesStore.putRole(WriteRequest.RefreshPolicy.IMMEDIATE, roleDescriptor, actionListener);
        } else {
            rolesStore.putRoles(WriteRequest.RefreshPolicy.IMMEDIATE, List.of(roleDescriptor), ActionListener.wrap(resp -> {
                BulkRolesResponse.Item item = resp.getItems().get(0);
                if (item.getResultType().equals("created")) {
                    actionListener.onResponse(true);
                } else {
                    throw item.getCause();
                }
            }, actionListener::onFailure));
        }
    }

    // test that we can read a role where field permissions are stored in 2.x format (fields:...)
    public void testBWCFieldPermissions() throws IOException {
        Path path = getDataPath("roles2xformat.json");
        byte[] bytes = Files.readAllBytes(path);
        String roleString = new String(bytes, Charset.defaultCharset());
        RoleDescriptor role = NativeRolesStore.transformRole(
            RoleDescriptor.ROLE_TYPE + "role1",
            new BytesArray(roleString),
            logger,
            TestUtils.newTestLicenseState()
        );
        assertNotNull(role);
        assertNotNull(role.getIndicesPrivileges());
        RoleDescriptor.IndicesPrivileges indicesPrivileges = role.getIndicesPrivileges()[0];
        assertThat(indicesPrivileges.getGrantedFields(), arrayContaining("foo", "boo"));
        assertNull(indicesPrivileges.getDeniedFields());
    }

    @SuppressWarnings("unchecked")
    public void testRoleDescriptorWithFlsDlsLicensing() throws IOException {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(false);
        RoleDescriptor flsRole = new RoleDescriptor(
            "fls",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().privileges("READ").indices("*").grantedFields("*").deniedFields("foo").build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            randomRemoteIndicesPrivileges(1, 2),
            null,
            null,
            randomAlphaOfLengthBetween(0, 20)
        );
        assertFalse(flsRole.getTransientMetadata().containsKey("unlicensed_features"));

        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);

        RoleDescriptor dlsRole = new RoleDescriptor(
            "dls",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").query(matchAllBytes).build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            randomRemoteIndicesPrivileges(1, 2),
            null,
            null,
            randomAlphaOfLengthBetween(0, 20)
        );
        assertFalse(dlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        RoleDescriptor flsDlsRole = new RoleDescriptor(
            "fls_dls",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("READ")
                    .grantedFields("*")
                    .deniedFields("foo")
                    .query(matchAllBytes)
                    .build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            randomRemoteIndicesPrivileges(1, 2),
            null,
            null,
            randomAlphaOfLengthBetween(0, 20)
        );
        assertFalse(flsDlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        RoleDescriptor noFlsDlsRole = new RoleDescriptor(
            "no_fls_dls",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            randomRemoteIndicesPrivileges(1, 2),
            null,
            null,
            randomAlphaOfLengthBetween(0, 20)
        );
        assertFalse(noFlsDlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        XContentBuilder builder = flsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        RoleDescriptor role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-fls", bytes, logger, licenseState);
        assertNotNull(role);
        assertTrue(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role.getTransientMetadata().get("unlicensed_features"), instanceOf(List.class));
        assertThat((List<String>) role.getTransientMetadata().get("unlicensed_features"), contains("fls"));
        assertThat(role, equalTo(flsRole));

        builder = dlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertTrue(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role.getTransientMetadata().get("unlicensed_features"), instanceOf(List.class));
        assertThat((List<String>) role.getTransientMetadata().get("unlicensed_features"), contains("dls"));
        assertThat(role, equalTo(dlsRole));

        builder = flsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertTrue(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role.getTransientMetadata().get("unlicensed_features"), instanceOf(List.class));
        assertThat((List<String>) role.getTransientMetadata().get("unlicensed_features"), contains("fls", "dls"));
        assertThat(role, equalTo(flsDlsRole));

        builder = noFlsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-no_fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role, equalTo(noFlsDlsRole));

        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        builder = flsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-fls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role, equalTo(flsRole));

        builder = dlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role, equalTo(dlsRole));

        builder = flsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role, equalTo(flsDlsRole));

        builder = noFlsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-no_fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role, equalTo(noFlsDlsRole));
    }

    public void testTransformingRoleWithRestrictionFails() throws IOException {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(false);
        RoleDescriptor roleWithRestriction = new RoleDescriptor(
            "role_with_restriction",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .privileges("READ")
                    .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
                    .grantedFields("*")
                    .deniedFields(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
                    .query(
                        randomBoolean()
                            ? "{ \"term\": { \""
                                + randomAlphaOfLengthBetween(3, 24)
                                + "\" : \""
                                + randomAlphaOfLengthBetween(3, 24)
                                + "\" }"
                            : "{ \"match_all\": {} }"
                    )
                    .build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            randomRemoteIndicesPrivileges(1, 2),
            null,
            RoleRestrictionTests.randomWorkflowsRestriction(1, 2),
            randomAlphaOfLengthBetween(0, 20)
        );

        XContentBuilder builder = roleWithRestriction.toXContent(
            XContentBuilder.builder(XContentType.JSON.xContent()),
            ToXContent.EMPTY_PARAMS
        );

        Logger mockedLogger = Mockito.mock(Logger.class);
        BytesReference bytes = BytesReference.bytes(builder);
        RoleDescriptor transformedRole = NativeRolesStore.transformRole(
            RoleDescriptor.ROLE_TYPE + "-role_with_restriction",
            bytes,
            mockedLogger,
            licenseState
        );
        assertThat(transformedRole, nullValue());
        ArgumentCaptor<ElasticsearchParseException> exceptionCaptor = ArgumentCaptor.forClass(ElasticsearchParseException.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockedLogger).error(messageCaptor.capture(), exceptionCaptor.capture());
        assertThat(messageCaptor.getValue(), containsString("error in the format of data for role [role_with_restriction]"));
        assertThat(
            exceptionCaptor.getValue().getMessage(),
            containsString("failed to parse role [role_with_restriction]. unexpected field [restriction]")
        );
    }

    public void testPutOfRoleWithFlsDlsUnlicensed() throws IOException {
        final Client client = mock(Client.class);
        final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(TransportVersion.current());
        final FeatureService featureService = mock(FeatureService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);

        final SecuritySystemIndices systemIndices = new SecuritySystemIndices(clusterService.getSettings());
        systemIndices.init(client, featureService, clusterService);
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();
        // Init for validation
        new ReservedRolesStore(Set.of("superuser"));
        final NativeRolesStore rolesStore = new NativeRolesStore(
            Settings.EMPTY,
            client,
            licenseState,
            securityIndex,
            clusterService,
            mock(FeatureService.class),
            mock(ReservedRoleNameChecker.class),
            mock(NamedXContentRegistry.class)
        );

        // setup the roles store so the security index exists
        securityIndex.clusterChanged(
            new ClusterChangedEvent("fls_dls_license", getClusterStateWithSecurityIndex(), getEmptyClusterState())
        );

        RoleDescriptor flsRole = new RoleDescriptor(
            "fls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().privileges("READ").indices("*").grantedFields("*").deniedFields("foo").build() },
            null
        );
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        putRole(rolesStore, flsRole, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);

        assertThat(e.getMessage(), containsString("field and document level security"));
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);

        RoleDescriptor dlsRole = new RoleDescriptor(
            "dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").query(matchAllBytes).build() },
            null
        );
        future = new PlainActionFuture<>();
        putRole(rolesStore, dlsRole, future);
        e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("field and document level security"));

        RoleDescriptor flsDlsRole = new RoleDescriptor(
            "fls_ dls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("READ")
                    .grantedFields("*")
                    .deniedFields("foo")
                    .query(matchAllBytes)
                    .build() },
            null
        );
        future = new PlainActionFuture<>();
        putRole(rolesStore, flsDlsRole, future);
        e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("field and document level security"));
    }

    public void testPutRoleWithRemotePrivsUnsupportedMinNodeVersion() throws IOException {
        // Init for validation
        new ReservedRolesStore(Set.of("superuser"));
        enum TEST_MODE {
            REMOTE_INDICES_PRIVS,
            REMOTE_CLUSTER_PRIVS,
            REMOTE_INDICES_AND_CLUSTER_PRIVS
        }
        for (TEST_MODE testMode : TEST_MODE.values()) {
            // default to both remote indices and cluster privileges and use the switch below to remove one or the other
            TransportVersion transportVersionBeforeAdvancedRemoteClusterSecurity = TransportVersionUtils.getPreviousVersion(
                TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY
            );
            RoleDescriptor.RemoteIndicesPrivileges[] remoteIndicesPrivileges = new RoleDescriptor.RemoteIndicesPrivileges[] {
                RoleDescriptor.RemoteIndicesPrivileges.builder("remote").privileges("read").indices("index").build() };
            RemoteClusterPermissions remoteClusterPermissions = new RemoteClusterPermissions().addGroup(
                new RemoteClusterPermissionGroup(
                    RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]),
                    new String[] { "remote" }
                )
            );
            switch (testMode) {
                case REMOTE_CLUSTER_PRIVS -> {
                    transportVersionBeforeAdvancedRemoteClusterSecurity = TransportVersionUtils.getPreviousVersion(
                        ROLE_REMOTE_CLUSTER_PRIVS
                    );
                    remoteIndicesPrivileges = null;
                }
                case REMOTE_INDICES_PRIVS -> remoteClusterPermissions = null;
            }
            final Client client = mock(Client.class);

            final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersions.MINIMUM_COMPATIBLE,
                transportVersionBeforeAdvancedRemoteClusterSecurity
            );
            final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(minTransportVersion);

            final XPackLicenseState licenseState = mock(XPackLicenseState.class);

            final SecuritySystemIndices systemIndices = new SecuritySystemIndices(clusterService.getSettings());
            final FeatureService featureService = mock(FeatureService.class);
            systemIndices.init(client, featureService, clusterService);
            final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();

            final NativeRolesStore rolesStore = new NativeRolesStore(
                Settings.EMPTY,
                client,
                licenseState,
                securityIndex,
                clusterService,
                mock(FeatureService.class),
                mock(ReservedRoleNameChecker.class),
                mock(NamedXContentRegistry.class)
            );
            // setup the roles store so the security index exists
            securityIndex.clusterChanged(new ClusterChangedEvent("source", getClusterStateWithSecurityIndex(), getEmptyClusterState()));

            RoleDescriptor remoteIndicesRole = new RoleDescriptor(
                "remote",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                remoteIndicesPrivileges,
                remoteClusterPermissions,
                null,
                null
            );
            PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            putRole(rolesStore, remoteIndicesRole, future);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                String.format(Locale.ROOT, "expected IllegalStateException, but not thrown for mode [%s]", testMode),
                future::actionGet
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "all nodes must have version ["
                        + (TEST_MODE.REMOTE_CLUSTER_PRIVS.equals(testMode)
                            ? ROLE_REMOTE_CLUSTER_PRIVS.toReleaseVersion()
                            : TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY.toReleaseVersion())
                        + "] or higher to support remote "
                        + (remoteIndicesPrivileges != null ? "indices" : "cluster")
                        + " privileges"
                )
            );
        }
    }

    public void testGetRoleWhenDisabled() throws Exception {
        final Settings settings = Settings.builder().put(NativeRolesStore.NATIVE_ROLES_ENABLED, "false").build();
        NativeRolesStore store = createRoleStoreForTest(settings);

        final PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
        store.getRoleDescriptors(Set.of(randomAlphaOfLengthBetween(4, 12)), future);

        assertThat(future.get().isSuccess(), is(true));
        assertThat(future.get().getDescriptors(), empty());

        Mockito.verifyNoInteractions(client);
    }

    public void testReservedRole() {
        final NativeRolesStore store = createRoleStoreForTest();
        final String roleName = randomFrom(new ArrayList<>(ReservedRolesStore.names()));

        RoleDescriptor roleDescriptor = new RoleDescriptor(
            roleName,
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().privileges("READ").indices("*").grantedFields("*").deniedFields("foo").build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            randomRemoteIndicesPrivileges(1, 2),
            null,
            null,
            randomAlphaOfLengthBetween(0, 20)
        );
        ActionRequestValidationException exception = assertThrows(ActionRequestValidationException.class, () -> {
            PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            putRole(store, roleDescriptor, future);
            future.actionGet();
        });

        assertThat(exception.getMessage(), containsString("is reserved and may not be used"));
    }

    public void testValidRole() throws IOException {
        testValidRole(randomFrom("admin", "dept_a", "restricted"));
    }

    public void testValidRoleWithInternalRoleName() throws IOException {
        testValidRole(AuthenticationTestHelper.randomInternalRoleName());
    }

    private void testValidRole(String roleName) throws IOException {
        final NativeRolesStore rolesStore = createRoleStoreForTest();

        RoleDescriptor roleDescriptor = new RoleDescriptor(
            roleName,
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().privileges("READ").indices("*").grantedFields("*").deniedFields("foo").build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            null,
            null,
            null,
            null,
            null,
            null
        );

        putRole(rolesStore, roleDescriptor, ActionListener.wrap(response -> fail(), exception -> fail()));
        boolean indexCalled = false;
        try {
            verify(client, times(1)).index(any(IndexRequest.class), any());
            indexCalled = true;
        } catch (AssertionError assertionError) {
            // Index wasn't called
        }

        boolean bulkCalled = false;
        try {
            verify(client, times(1)).bulk(any(BulkRequest.class), any());
            bulkCalled = true;
        } catch (AssertionError assertionError) {
            // bulk wasn't called
        }

        assertTrue(bulkCalled || indexCalled);
    }

    public void testCreationOfRoleWithMalformedQueryJsonFails() throws IOException {
        final NativeRolesStore rolesStore = createRoleStoreForTest();

        String[] malformedQueryJson = new String[] {
            "{ \"match_all\": { \"unknown_field\": \"\" } }",
            "{ malformed JSON }",
            "{ \"unknown\": {\"\"} }",
            "{}" };

        BytesReference query = new BytesArray(randomFrom(malformedQueryJson));

        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("idx1")
                    .privileges(new String[] { "read" })
                    .query(query)
                    .allowRestrictedIndices(randomBoolean())
                    .build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<Boolean> responseRef = new AtomicReference<>();

        putRole(rolesStore, roleDescriptor, ActionListener.wrap(responseRef::set, throwableRef::set));

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        Throwable t = throwableRef.get();
        assertThat(t, instanceOf(ElasticsearchParseException.class));
        assertThat(
            t.getMessage(),
            containsString(
                "failed to parse field 'query' for indices ["
                    + Strings.arrayToCommaDelimitedString(new String[] { "idx1" })
                    + "] at index privilege [0] of role descriptor"
            )
        );
    }

    public void testCreationOfRoleWithUnsupportedQueryFails() throws IOException {
        final NativeRolesStore rolesStore = createRoleStoreForTest();

        String hasChildQuery = "{ \"has_child\": { \"type\": \"child\", \"query\": { \"match_all\": {} } } }";
        String hasParentQuery = "{ \"has_parent\": { \"parent_type\": \"parent\", \"query\": { \"match_all\": {} } } }";

        BytesReference query = new BytesArray(randomFrom(hasChildQuery, hasParentQuery));

        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("idx1")
                    .privileges(new String[] { "read" })
                    .query(query)
                    .allowRestrictedIndices(randomBoolean())
                    .build() },
            randomApplicationPrivileges(),
            randomClusterPrivileges(),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<Boolean> responseRef = new AtomicReference<>();
        putRole(rolesStore, roleDescriptor, ActionListener.wrap(responseRef::set, throwableRef::set));

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        Throwable t = throwableRef.get();
        assertThat(t, instanceOf(ElasticsearchParseException.class));
        assertThat(
            t.getMessage(),
            containsString(
                "failed to parse field 'query' for indices ["
                    + Strings.arrayToCommaDelimitedString(new String[] { "idx1" })
                    + "] at index privilege [0] of role descriptor"
            )
        );
    }

    public void testManyValidRoles() throws IOException {
        final NativeRolesStore rolesStore = createRoleStoreForTest();
        List<String> roleNames = List.of("test", "admin", "123");

        List<RoleDescriptor> roleDescriptors = roleNames.stream()
            .map(
                roleName -> new RoleDescriptor(
                    roleName,
                    randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("*").grantedFields("*").deniedFields("foo").build() },
                    randomApplicationPrivileges(),
                    randomClusterPrivileges(),
                    generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .toList();

        AtomicReference<BulkRolesResponse> response = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();
        rolesStore.putRoles(WriteRequest.RefreshPolicy.IMMEDIATE, roleDescriptors, ActionListener.wrap(response::set, exception::set));
        assertNull(exception.get());
        verify(client, times(1)).bulk(any(BulkRequest.class), any());
    }

    public void testBulkDeleteRoles() {
        final NativeRolesStore rolesStore = createRoleStoreForTest();

        AtomicReference<BulkRolesResponse> response = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();
        rolesStore.deleteRoles(
            List.of("test-role-1", "test-role-2", "test-role-3"),
            WriteRequest.RefreshPolicy.IMMEDIATE,
            ActionListener.wrap(response::set, exception::set)
        );
        assertNull(exception.get());
        verify(client, times(1)).bulk(any(BulkRequest.class), any());
    }

    public void testBulkDeleteReservedRole() {
        final NativeRolesStore rolesStore = createRoleStoreForTest();

        AtomicReference<BulkRolesResponse> response = new AtomicReference<>();
        AtomicReference<Exception> exception = new AtomicReference<>();
        rolesStore.deleteRoles(
            List.of("superuser"),
            WriteRequest.RefreshPolicy.IMMEDIATE,
            ActionListener.wrap(response::set, exception::set)
        );
        assertNull(exception.get());
        assertThat(response.get().getItems().size(), equalTo(1));
        BulkRolesResponse.Item item = response.get().getItems().get(0);
        assertThat(item.getCause().getMessage(), equalTo("role [superuser] is reserved and cannot be deleted"));
        assertThat(item.getRoleName(), equalTo("superuser"));

        verify(client, times(0)).bulk(any(BulkRequest.class), any());
    }

    /**
     * Make sure all top level fields for a RoleDescriptor have default values to make sure they can be set to empty in an upsert
     * call to the roles API
     */
    public void testAllTopFieldsHaveEmptyDefaultsForUpsert() throws IOException, IllegalAccessException {
        final NativeRolesStore rolesStore = createRoleStoreForTest();
        RoleDescriptor allNullDescriptor = new RoleDescriptor(
            "all-null-descriptor",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        Set<ParseField> fieldsWithoutDefaultValue = Set.of(
            RoleDescriptor.Fields.INDEX,
            RoleDescriptor.Fields.NAMES,
            RoleDescriptor.Fields.ALLOW_RESTRICTED_INDICES,
            RoleDescriptor.Fields.RESOURCES,
            RoleDescriptor.Fields.QUERY,
            RoleDescriptor.Fields.PRIVILEGES,
            RoleDescriptor.Fields.CLUSTERS,
            RoleDescriptor.Fields.APPLICATION,
            RoleDescriptor.Fields.FIELD_PERMISSIONS,
            RoleDescriptor.Fields.FIELD_PERMISSIONS_2X,
            RoleDescriptor.Fields.GRANT_FIELDS,
            RoleDescriptor.Fields.EXCEPT_FIELDS,
            RoleDescriptor.Fields.METADATA_FLATTENED,
            RoleDescriptor.Fields.TRANSIENT_METADATA,
            RoleDescriptor.Fields.RESTRICTION,
            RoleDescriptor.Fields.WORKFLOWS
        );

        String serializedOutput = Strings.toString(rolesStore.createRoleXContentBuilder(allNullDescriptor));
        Field[] fields = RoleDescriptor.Fields.class.getFields();

        for (Field field : fields) {
            ParseField fieldValue = (ParseField) field.get(null);
            if (fieldsWithoutDefaultValue.contains(fieldValue) == false) {
                assertThat(
                    "New RoleDescriptor field without a default value detected. "
                        + "Set a value or add to excluded list if not expected to be set to empty through role APIs",
                    serializedOutput,
                    containsString(fieldValue.getPreferredName())
                );
            }
        }
    }

    private ClusterService mockClusterServiceWithMinNodeVersion(TransportVersion transportVersion) {
        final ClusterService clusterService = mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        when(clusterService.state().getMinTransportVersion()).thenReturn(transportVersion);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return clusterService;
    }

    private ClusterState getClusterStateWithSecurityIndex() {
        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_MAIN_ALIAS + (withAlias ? "-" + randomAlphaOfLength(5) : "");

        Settings.Builder settingsBuilder = indexSettings(IndexVersion.current(), 1, 0);
        settingsBuilder.put(INDEX_FORMAT_SETTING.getKey(), SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT);
        settingsBuilder.put(VERSION_META_KEY, 1);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("_meta", Map.of(VERSION_META_KEY, 1)));
        when(mappingMetadata.getSha256()).thenReturn("test");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(securityIndexName).putMapping(mappingMetadata).settings(settingsBuilder))
            .build();

        if (withAlias) {
            metadata = SecurityTestUtils.addAliasToMetadata(metadata, securityIndexName);
        }

        Index index = metadata.index(securityIndexName).getIndex();

        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(index)
                    .addIndexShard(
                        IndexShardRoutingTable.builder(new ShardId(index, 0))
                            .addShard(
                                shardRouting.initialize(randomAlphaOfLength(8), null, shardRouting.getExpectedShardSize())
                                    .moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                            )
                    )
                    .build()
            )
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName(NativeRolesStoreTests.class.getName()))
            .metadata(metadata)
            .routingTable(routingTable)
            .putCompatibilityVersions(
                "test",
                new CompatibilityVersions(
                    TransportVersion.current(),
                    Map.of(".security-7", new SystemIndexDescriptor.MappingsVersion(1, 0))
                )
            )
            .build();

        return clusterState;
    }

    private ClusterState getEmptyClusterState() {
        return ClusterState.builder(new ClusterName(NativeRolesStoreTests.class.getName())).build();
    }
}
