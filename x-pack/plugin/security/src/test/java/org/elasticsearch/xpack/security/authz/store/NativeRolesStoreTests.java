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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.Reason;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.RoleRestrictionTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NativeRolesStoreTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool("index audit trail update mapping tests");
    }

    @After
    public void terminateThreadPool() throws Exception {
        terminate(threadPool);
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
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 2) : null,
            null
        );
        assertFalse(flsRole.getTransientMetadata().containsKey("unlicensed_features"));

        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);

        RoleDescriptor dlsRole = new RoleDescriptor(
            "dls",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").query(matchAllBytes).build() },
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 2) : null,
            null
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
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 2) : null,
            null
        );
        assertFalse(flsDlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        RoleDescriptor noFlsDlsRole = new RoleDescriptor(
            "no_fls_dls",
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").build() },
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 2) : null,
            null
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
            RoleDescriptorTests.randomApplicationPrivileges(),
            RoleDescriptorTests.randomClusterPrivileges(),
            generateRandomStringArray(5, randomIntBetween(2, 8), true, true),
            RoleDescriptorTests.randomRoleDescriptorMetadata(ESTestCase.randomBoolean()),
            null,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? RoleDescriptorTests.randomRemoteIndicesPrivileges(1, 2) : null,
            RoleRestrictionTests.randomWorkflowsRestriction(1, 2)
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
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final AtomicBoolean methodCalled = new AtomicBoolean(false);

        final SecuritySystemIndices systemIndices = new SecuritySystemIndices();
        systemIndices.init(client, clusterService);
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();

        final NativeRolesStore rolesStore = new NativeRolesStore(Settings.EMPTY, client, licenseState, securityIndex, clusterService) {
            @Override
            void innerPutRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
                if (methodCalled.compareAndSet(false, true)) {
                    listener.onResponse(true);
                } else {
                    fail("method called more than once!");
                }
            }
        };
        // setup the roles store so the security index exists
        securityIndex.clusterChanged(
            new ClusterChangedEvent("fls_dls_license", getClusterStateWithSecurityIndex(), getEmptyClusterState())
        );

        PutRoleRequest putRoleRequest = new PutRoleRequest();
        RoleDescriptor flsRole = new RoleDescriptor(
            "fls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().privileges("READ").indices("*").grantedFields("*").deniedFields("foo").build() },
            null
        );
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, flsRole, future);
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
        rolesStore.putRole(putRoleRequest, dlsRole, future);
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
        rolesStore.putRole(putRoleRequest, flsDlsRole, future);
        e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("field and document level security"));

        RoleDescriptor noFlsDlsRole = new RoleDescriptor(
            "no_fls_dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").build() },
            null
        );
        future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, noFlsDlsRole, future);
        assertTrue(future.actionGet());
    }

    public void testPutRoleWithRemoteIndicesUnsupportedMinNodeVersion() {
        final Client client = mock(Client.class);
        final TransportVersion transportVersionBeforeAdvancedRemoteClusterSecurity = TransportVersionUtils.getPreviousVersion(
            TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS
        );
        final TransportVersion minTransportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.MINIMUM_COMPATIBLE,
            transportVersionBeforeAdvancedRemoteClusterSecurity
        );
        final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(minTransportVersion);

        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final AtomicBoolean methodCalled = new AtomicBoolean(false);

        final SecuritySystemIndices systemIndices = new SecuritySystemIndices();
        systemIndices.init(client, clusterService);
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();

        final NativeRolesStore rolesStore = new NativeRolesStore(Settings.EMPTY, client, licenseState, securityIndex, clusterService) {
            @Override
            void innerPutRole(final PutRoleRequest request, final RoleDescriptor role, final ActionListener<Boolean> listener) {
                if (methodCalled.compareAndSet(false, true)) {
                    listener.onResponse(true);
                } else {
                    fail("method called more than once!");
                }
            }
        };
        // setup the roles store so the security index exists
        securityIndex.clusterChanged(new ClusterChangedEvent("source", getClusterStateWithSecurityIndex(), getEmptyClusterState()));

        PutRoleRequest putRoleRequest = new PutRoleRequest();
        RoleDescriptor remoteIndicesRole = new RoleDescriptor(
            "remote",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                RoleDescriptor.RemoteIndicesPrivileges.builder("remote").privileges("read").indices("index").build() },
            null
        );
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, remoteIndicesRole, future);
        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            containsString(
                "all nodes must have transport version ["
                    + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCS
                    + "] or higher to support remote indices privileges"
            )
        );
    }

    private ClusterService mockClusterServiceWithMinNodeVersion(TransportVersion transportVersion) {
        final ClusterService clusterService = mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        when(clusterService.state().getMinTransportVersion()).thenReturn(transportVersion);
        return clusterService;
    }

    private ClusterState getClusterStateWithSecurityIndex() {
        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_MAIN_ALIAS + (withAlias ? "-" + randomAlphaOfLength(5) : "");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(securityIndexName).settings(indexSettings(Version.CURRENT, 1, 0)))
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
            .build();

        return clusterState;
    }

    private ClusterState getEmptyClusterState() {
        return ClusterState.builder(new ClusterName(NativeRolesStoreTests.class.getName())).build();
    }
}
