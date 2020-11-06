/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
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
        RoleDescriptor role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "role1",
            new BytesArray(roleString), logger, TestUtils.newTestLicenseState());
        assertNotNull(role);
        assertNotNull(role.getIndicesPrivileges());
        RoleDescriptor.IndicesPrivileges indicesPrivileges = role.getIndicesPrivileges()[0];
        assertThat(indicesPrivileges.getGrantedFields(), arrayContaining("foo", "boo"));
        assertNull(indicesPrivileges.getDeniedFields());
    }

    public void testRoleDescriptorWithFlsDlsLicensing() throws IOException {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_DLS_FLS)).thenReturn(false);
        RoleDescriptor flsRole = new RoleDescriptor("fls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().privileges("READ").indices("*")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .build() },
                null);
        assertFalse(flsRole.getTransientMetadata().containsKey("unlicensed_features"));

        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);

        RoleDescriptor dlsRole = new RoleDescriptor("dls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ")
                        .query(matchAllBytes)
                        .build() },
                null);
        assertFalse(dlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        RoleDescriptor flsDlsRole = new RoleDescriptor("fls_ dls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .query(matchAllBytes)
                        .build() },
                null);
        assertFalse(flsDlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        RoleDescriptor noFlsDlsRole = new RoleDescriptor("no_fls_dls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").build() },
                null);
        assertFalse(noFlsDlsRole.getTransientMetadata().containsKey("unlicensed_features"));

        XContentBuilder builder = flsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        RoleDescriptor role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "-fls", bytes, logger, licenseState);
        assertNotNull(role);
        assertTrue(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role.getTransientMetadata().get("unlicensed_features"), instanceOf(List.class));
        assertThat((List<String>) role.getTransientMetadata().get("unlicensed_features"), contains("fls"));

        builder = dlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertTrue(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role.getTransientMetadata().get("unlicensed_features"), instanceOf(List.class));
        assertThat((List<String>) role.getTransientMetadata().get("unlicensed_features"), contains("dls"));

        builder = flsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertTrue(role.getTransientMetadata().containsKey("unlicensed_features"));
        assertThat(role.getTransientMetadata().get("unlicensed_features"), instanceOf(List.class));
        assertThat((List<String>) role.getTransientMetadata().get("unlicensed_features"), contains("fls", "dls"));

        builder = noFlsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "no_fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));

        when(licenseState.checkFeature(Feature.SECURITY_DLS_FLS)).thenReturn(true);
        builder = flsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "fls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));

        builder = dlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));

        builder = flsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));

        builder = noFlsDlsRole.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS);
        bytes = BytesReference.bytes(builder);
        role = NativeRolesStore.transformRole(RoleDescriptor.ROLE_TYPE + "no_fls_dls", bytes, logger, licenseState);
        assertNotNull(role);
        assertFalse(role.getTransientMetadata().containsKey("unlicensed_features"));
    }

    public void testPutOfRoleWithFlsDlsUnlicensed() throws IOException {
        final Client client = mock(Client.class);
        final ClusterService clusterService = mock(ClusterService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final AtomicBoolean methodCalled = new AtomicBoolean(false);
        final SecurityIndexManager securityIndex = SecurityIndexManager.buildSecurityMainIndexManager(client, clusterService);
        final NativeRolesStore rolesStore = new NativeRolesStore(Settings.EMPTY, client, licenseState, securityIndex) {
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
        securityIndex.clusterChanged(new ClusterChangedEvent(
            "fls_dls_license", getClusterStateWithSecurityIndex(), getEmptyClusterState()));

        PutRoleRequest putRoleRequest = new PutRoleRequest();
        RoleDescriptor flsRole = new RoleDescriptor("fls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().privileges("READ").indices("*")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .build() },
                null);
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, flsRole, future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("field and document level security"));
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);

        RoleDescriptor dlsRole = new RoleDescriptor("dls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ")
                        .query(matchAllBytes)
                        .build() },
                null);
        future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, dlsRole, future);
        e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("field and document level security"));

        RoleDescriptor flsDlsRole = new RoleDescriptor("fls_ dls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .query(matchAllBytes)
                        .build() },
                null);
        future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, flsDlsRole, future);
        e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("field and document level security"));

        RoleDescriptor noFlsDlsRole = new RoleDescriptor("no_fls_dls", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("READ").build() },
                null);
        future = new PlainActionFuture<>();
        rolesStore.putRole(putRoleRequest, noFlsDlsRole, future);
        assertTrue(future.actionGet());
    }

    private ClusterState getClusterStateWithSecurityIndex() {
        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_MAIN_ALIAS + (withAlias ? "-" + randomAlphaOfLength(5) : "");

        Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder(securityIndexName).settings(settings))
                .put(new IndexTemplateMetadata(SecurityIndexManager.SECURITY_MAIN_TEMPLATE_7, 0, 0,
                        Collections.singletonList(securityIndexName), Settings.EMPTY, ImmutableOpenMap.of(),
                        ImmutableOpenMap.of()))
                .build();

        if (withAlias) {
            metadata = SecurityTestUtils.addAliasToMetadata(metadata, securityIndexName);
        }

        Index index = new Index(securityIndexName, UUID.randomUUID().toString());
        ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId(index, 0), true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE, new UnassignedInfo(Reason.INDEX_CREATED, ""));
        IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(new ShardId(index, 0))
                .addShard(shardRouting.initialize(randomAlphaOfLength(8), null, shardRouting.getExpectedShardSize()).moveToStarted())
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .add(IndexRoutingTable
                        .builder(index)
                        .addIndexShard(table)
                        .build())
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
