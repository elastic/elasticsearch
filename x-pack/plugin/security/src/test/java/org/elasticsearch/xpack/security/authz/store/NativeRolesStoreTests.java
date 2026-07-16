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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SequenceNumbers;
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
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleRestrictionTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;
import static org.elasticsearch.indices.SystemIndexDescriptor.VERSION_META_KEY;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomApplicationPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomClusterPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRemoteIndicesPrivileges;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomRoleDescriptorMetadata;
import static org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.DatasourcePrivileges.ESQL_DATASOURCE_PRIVILEGE;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

    private NativeRolesStore createRoleStoreForTest(ProjectId projectId) {
        return createRoleStoreForTest(projectId, Settings.builder().build());
    }

    private NativeRolesStore createRoleStoreForTest(ProjectId projectId, Settings settings) {
        new ReservedRolesStore(Set.of("superuser"));
        final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(TransportVersion.current());
        final SecuritySystemIndices systemIndices = new SecuritySystemIndices(settings);
        final FeatureService featureService = mock(FeatureService.class);
        systemIndices.init(client, featureService, clusterService, TestProjectResolvers.singleProject(projectId));
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();
        // Create the index
        securityIndex.clusterChanged(
            new ClusterChangedEvent("source", getClusterStateWithSecurityIndex(projectId), getEmptyClusterState())
        );

        return new NativeRolesStore(
            settings,
            client,
            TestUtils.newTestLicenseState(),
            securityIndex,
            clusterService,
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
        final ProjectId projectId = randomProjectIdOrDefault();
        final Client client = mock(Client.class);
        final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(TransportVersion.current());
        final FeatureService featureService = mock(FeatureService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);

        final SecuritySystemIndices systemIndices = new SecuritySystemIndices(clusterService.getSettings());
        systemIndices.init(client, featureService, clusterService, TestProjectResolvers.singleProject(projectId));
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();
        // Init for validation
        new ReservedRolesStore(Set.of("superuser"));
        final NativeRolesStore rolesStore = new NativeRolesStore(
            Settings.EMPTY,
            client,
            licenseState,
            securityIndex,
            clusterService,
            mock(ReservedRoleNameChecker.class),
            mock(NamedXContentRegistry.class)
        );

        // setup the roles store so the security index exists
        securityIndex.clusterChanged(
            new ClusterChangedEvent("fls_dls_license", getClusterStateWithSecurityIndex(projectId), getEmptyClusterState())
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

    public void testPutRoleWithDatasourcePrivilegeRejectedInMixedCluster() throws IOException {
        final ProjectId projectId = randomProjectIdOrDefault();
        final Client client = mock(Client.class);
        final ClusterService clusterService = mockClusterServiceWithMinNodeVersion(
            TransportVersionUtils.getPreviousVersion(ESQL_DATASOURCE_PRIVILEGE)
        );
        final FeatureService featureService = mock(FeatureService.class);
        final SecuritySystemIndices systemIndices = new SecuritySystemIndices(clusterService.getSettings());
        systemIndices.init(client, featureService, clusterService, TestProjectResolvers.singleProject(projectId));
        final SecurityIndexManager securityIndex = systemIndices.getMainIndexManager();
        new ReservedRolesStore(Set.of("superuser"));
        final NativeRolesStore rolesStore = new NativeRolesStore(
            Settings.EMPTY,
            client,
            TestUtils.newTestLicenseState(),
            securityIndex,
            clusterService,
            mock(ReservedRoleNameChecker.class),
            mock(NamedXContentRegistry.class)
        );
        securityIndex.clusterChanged(new ClusterChangedEvent("test", getClusterStateWithSecurityIndex(projectId), getEmptyClusterState()));

        final var datasourcePrivileges = new ConfigurableClusterPrivileges.DatasourcePrivileges(
            List.of(
                new ConfigurableClusterPrivileges.DatasourcePrivileges.DatasourcePermissionGroup(
                    new String[] { "my-ds" },
                    new String[] { "read" }
                )
            )
        );
        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test-role",
            null,
            null,
            null,
            new ConfigurableClusterPrivilege[] { datasourcePrivileges },
            null,
            null,
            null
        );

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        putRole(rolesStore, roleDescriptor, future);
        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("datasource privilege"));
    }

    public void testGetRoleWhenDisabled() throws Exception {
        final Settings settings = Settings.builder().put(NativeRolesStore.NATIVE_ROLES_ENABLED, "false").build();
        NativeRolesStore store = createRoleStoreForTest(randomProjectIdOrDefault(), settings);

        final PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
        store.getRoleDescriptors(Set.of(randomAlphaOfLengthBetween(4, 12)), future);

        assertThat(future.get().isSuccess(), is(true));
        assertThat(future.get().getDescriptors(), empty());

        Mockito.verifyNoInteractions(client);
    }

    public void testReservedRole() {
        final NativeRolesStore store = createRoleStoreForTest(randomProjectIdOrDefault());
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
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());

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
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());

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
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());

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
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());
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

    /**
     * Settings that enable the expensive before/after diff capture, so that {@link NativeRolesStore} takes the GET +
     * compare-and-swap path that records reliable before-images.
     */
    private static Settings diffCaptureEnabledSettings() {
        return Settings.builder().put("xpack.security.audit.logfile.events.emit_security_config_change_diff", true).build();
    }

    /**
     * When before/after diff auditing is enabled, a bulk put-roles request fans out to per-role GET + compare-and-swap upserts
     * (instead of a single batched bulk request) and tags the response with the reliable before-image of each role that already
     * existed. Here one role pre-exists (captured as an "updated" item with a before-image) and one is new (a "created" item with
     * no before-image).
     */
    @SuppressWarnings("unchecked")
    public void testBulkPutRolesCapturesBeforeImagesWhenDiffAuditingEnabled() throws Exception {
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault(), diffCaptureEnabledSettings());

        // The authoritative "before" image that the GET will return for the pre-existing role.
        final RoleDescriptor existingBefore = new RoleDescriptor("existing-role", new String[] { "monitor" }, null, null);
        final BytesReference existingSource = BytesReference.bytes(rolesStore.createRoleXContentBuilder(existingBefore));

        // The "after" states requested by the bulk call: an update to the existing role, plus a brand-new role.
        final RoleDescriptor existingAfter = new RoleDescriptor("existing-role", new String[] { "monitor", "manage" }, null, null);
        final RoleDescriptor newRole = new RoleDescriptor("new-role", new String[] { "all" }, null, null);

        // Fresh index request builder per call so the two conditional writes do not share underlying request state.
        when(client.prepareIndex(SECURITY_MAIN_ALIAS)).thenAnswer(inv -> new IndexRequestBuilder(client));
        when(client.prepareGet(eq(SECURITY_MAIN_ALIAS), anyString())).thenAnswer(
            inv -> new GetRequestBuilder(client, SECURITY_MAIN_ALIAS).setId(inv.getArgument(1))
        );

        doAnswer(inv -> {
            final GetRequest getRequest = inv.getArgument(0);
            final ActionListener<GetResponse> getListener = (ActionListener<GetResponse>) inv.getArgument(1);
            if (getRequest.id().equals("role-existing-role")) {
                getListener.onResponse(
                    new GetResponse(new GetResult(SECURITY_MAIN_ALIAS, getRequest.id(), 3, 1, 1, true, existingSource, Map.of(), Map.of()))
                );
            } else {
                getListener.onResponse(
                    new GetResponse(
                        new GetResult(
                            SECURITY_MAIN_ALIAS,
                            getRequest.id(),
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                            -1,
                            false,
                            null,
                            Map.of(),
                            Map.of()
                        )
                    )
                );
            }
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(inv -> {
            final IndexRequest indexRequest = inv.getArgument(0);
            final ActionListener<IndexResponse> indexListener = (ActionListener<IndexResponse>) inv.getArgument(1);
            final boolean created = indexRequest.opType() == DocWriteRequest.OpType.CREATE;
            indexListener.onResponse(
                new IndexResponse(new ShardId(new Index(SECURITY_MAIN_ALIAS, "_na_"), 0), indexRequest.id(), 4, 1, 2, created)
            );
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        doAnswer(inv -> {
            final ActionListener<ClearRolesCacheResponse> cacheListener = (ActionListener<ClearRolesCacheResponse>) inv.getArgument(2);
            cacheListener.onResponse(mock(ClearRolesCacheResponse.class));
            return null;
        }).when(client).execute(eq(ClearRolesCacheAction.INSTANCE), any(), any());

        final PlainActionFuture<BulkRolesResponse> future = new PlainActionFuture<>();
        rolesStore.putRoles(WriteRequest.RefreshPolicy.IMMEDIATE, List.of(existingAfter, newRole), future);
        final BulkRolesResponse response = future.actionGet();

        // The batched bulk API is never used on the diff-capture path; each role is written with its own conditional request.
        verify(client, times(0)).bulk(any(BulkRequest.class), any());
        verify(client, times(2)).get(any(GetRequest.class), any());
        verify(client, times(2)).index(any(IndexRequest.class), any());

        final Map<String, BulkRolesResponse.Item> itemsByName = new HashMap<>();
        for (BulkRolesResponse.Item item : response.getItems()) {
            itemsByName.put(item.getRoleName(), item);
        }
        assertThat(itemsByName.keySet(), equalTo(Set.of("existing-role", "new-role")));
        assertThat(itemsByName.get("existing-role").isFailed(), is(false));
        assertThat(itemsByName.get("existing-role").getResultType(), equalTo("updated"));
        assertThat(itemsByName.get("new-role").isFailed(), is(false));
        assertThat(itemsByName.get("new-role").getResultType(), equalTo("created"));

        // The pre-existing role carries a reliable before-image; the newly created role has none.
        final RoleDescriptor capturedBefore = response.getPreviousRoleDescriptor("existing-role");
        assertThat(capturedBefore, is(notNullValue()));
        assertThat(capturedBefore.getName(), equalTo("existing-role"));
        assertThat(capturedBefore.getClusterPrivileges(), arrayContaining("monitor"));
        assertThat(response.getPreviousRoleDescriptor("new-role"), is(nullValue()));
    }

    /**
     * With diff auditing disabled (the default), a bulk put-roles request keeps using the cheap batched bulk request and never
     * issues a GET, so no before-image is captured.
     */
    public void testBulkPutRolesUsesBatchedPathWhenDiffAuditingDisabled() {
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());

        final RoleDescriptor role = new RoleDescriptor("some-role", new String[] { "monitor" }, null, null);

        final AtomicReference<BulkRolesResponse> response = new AtomicReference<>();
        final AtomicReference<Exception> exception = new AtomicReference<>();
        rolesStore.putRoles(WriteRequest.RefreshPolicy.IMMEDIATE, List.of(role), ActionListener.wrap(response::set, exception::set));

        assertNull(exception.get());
        verify(client, times(1)).bulk(any(BulkRequest.class), any());
        verify(client, times(0)).get(any(GetRequest.class), any());
    }

    public void testBulkDeleteRoles() {
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());

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
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());

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
        final NativeRolesStore rolesStore = createRoleStoreForTest(randomProjectIdOrDefault());
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
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(LoggingAuditTrail.EMIT_CONFIG_CHANGE_DIFF))
        );
        return clusterService;
    }

    private ClusterState getClusterStateWithSecurityIndex(ProjectId projectId) {
        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_MAIN_ALIAS + (withAlias ? "-" + randomAlphaOfLength(5) : "");

        Settings.Builder settingsBuilder = indexSettings(IndexVersion.current(), 1, 0);
        settingsBuilder.put(INDEX_FORMAT_SETTING.getKey(), SecuritySystemIndices.INTERNAL_MAIN_INDEX_FORMAT);
        settingsBuilder.put(VERSION_META_KEY, 1);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("_meta", Map.of(VERSION_META_KEY, 1)));
        when(mappingMetadata.getSha256()).thenReturn("test");
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder(securityIndexName).putMapping(mappingMetadata).settings(settingsBuilder).build(), true)
            .build();

        if (withAlias) {
            projectMetadata = SecurityTestUtils.addAliasToMetadata(projectMetadata, securityIndexName);
        }

        Index index = projectMetadata.index(securityIndexName).getIndex();
        Metadata metadata = Metadata.builder().put(projectMetadata).build();

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
            .putRoutingTable(projectMetadata.id(), routingTable)
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
