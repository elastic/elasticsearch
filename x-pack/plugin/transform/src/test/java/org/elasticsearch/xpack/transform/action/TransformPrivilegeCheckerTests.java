/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.transform.transforms.DestAlias;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TransformPrivilegeCheckerTests extends ESTestCase {

    private static final String OPERATION_NAME = "create";
    private static final Settings SETTINGS = Settings.builder().put("xpack.security.enabled", true).build();
    private static final String USER_NAME = "bob";
    private static final String TRANSFORM_ID = "some-id";
    private static final String SOURCE_INDEX_NAME = "some-source-index";
    private static final ResourcePrivileges SOURCE_INDEX_NAME_PRIVILEGES = ResourcePrivileges.builder(SOURCE_INDEX_NAME)
        .addPrivilege("view_index_metadata", true)
        .addPrivilege("read", true)
        .build();
    private static final String SOURCE_INDEX_NAME_2 = "some-other-source-index";
    private static final ResourcePrivileges SOURCE_INDEX_NAME_2_PRIVILEGES = ResourcePrivileges.builder(SOURCE_INDEX_NAME_2)
        .addPrivilege("view_index_metadata", true)
        .addPrivilege("read", false)
        .build();
    private static final String REMOTE_SOURCE_INDEX_NAME = "some-remote-cluster:some-remote-source-index";
    private static final String DEST_INDEX_NAME = "some-dest-index";
    private static final ResourcePrivileges DEST_INDEX_NAME_PRIVILEGES = ResourcePrivileges.builder(DEST_INDEX_NAME)
        .addPrivilege("index", true)
        .addPrivilege("read", true)
        .addPrivilege("delete", true)
        .build();
    private static final String DEST_INDEX_NAME_2 = "some-other_dest-index";
    private static final ResourcePrivileges DEST_INDEX_NAME_2_PRIVILEGES = ResourcePrivileges.builder(DEST_INDEX_NAME_2)
        .addPrivilege("index", true)
        .addPrivilege("read", false)
        .addPrivilege("delete", false)
        .build();
    private static final String DEST_ALIAS_NAME = "some-dest-alias";
    private static final TransformConfig TRANSFORM_CONFIG = new TransformConfig.Builder().setId(TRANSFORM_ID)
        .setSource(new SourceConfig(SOURCE_INDEX_NAME))
        .setDest(new DestConfig(DEST_INDEX_NAME, null, null))
        .build();
    private ThreadPool threadPool;
    private SecurityContext securityContext;
    private final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
    private MyMockClient client;

    @Before
    public void setupClient() {
        if (client != null) {
            client.close();
        }
        client = new MyMockClient(getTestName());
        threadPool = new TestThreadPool("transform_privilege_checker_tests");
        securityContext = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext()) {
            public User getUser() {
                return new User(USER_NAME);
            }
        };
    }

    @After
    public void tearDownClient() {
        client.close();
        threadPool.shutdown();
    }

    public void testCheckPrivileges_NoCheckDestIndexPrivileges() {
        TransformConfig config = new TransformConfig.Builder(TRANSFORM_CONFIG).setSource(
            new SourceConfig(SOURCE_INDEX_NAME, REMOTE_SOURCE_INDEX_NAME)
        ).build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            ClusterState.EMPTY_STATE,
            client,
            config,
            false,
            ActionTestUtils.assertNoFailureListener(aVoid -> {
                HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                assertThat(request.username(), is(equalTo(USER_NAME)));
                assertThat(request.applicationPrivileges(), is(emptyArray()));
                assertThat(request.clusterPrivileges(), is(emptyArray()));
                assertThat(request.indexPrivileges(), is(arrayWithSize(1)));  // remote index is filtered out
                RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                assertThat(sourceIndicesPrivileges.allowRestrictedIndices(), is(true));
            })
        );
    }

    public void testCheckPrivileges_NoLocalIndices_NoCheckDestIndexPrivileges() {
        TransformConfig config = new TransformConfig.Builder(TRANSFORM_CONFIG).setSource(new SourceConfig(REMOTE_SOURCE_INDEX_NAME))
            .build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            ClusterState.EMPTY_STATE,
            client,
            config,
            false,
            // _has_privileges API is not called for the remote index
            ActionTestUtils.assertNoFailureListener(aVoid -> assertThat(client.lastHasPrivilegesRequest, is(nullValue())))
        );
    }

    public void testCheckPrivileges_CheckDestIndexPrivileges_DestIndexDoesNotExist() {
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            ClusterState.EMPTY_STATE,
            client,
            TRANSFORM_CONFIG,
            true,
            ActionTestUtils.assertNoFailureListener(aVoid -> {
                HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                assertThat(request.username(), is(equalTo(USER_NAME)));
                assertThat(request.applicationPrivileges(), is(emptyArray()));
                assertThat(request.clusterPrivileges(), is(emptyArray()));
                assertThat(request.indexPrivileges(), is(arrayWithSize(2)));
                RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                assertThat(sourceIndicesPrivileges.allowRestrictedIndices(), is(true));
                RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[1];
                assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "index", "create_index")));
                assertThat(destIndicesPrivileges.allowRestrictedIndices(), is(true));
            })
        );
    }

    public void testCheckPrivileges_CheckDestIndexPrivileges_DestIndexExists() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(DEST_INDEX_NAME)
                            .settings(settings(IndexVersion.current()))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            clusterState,
            client,
            TRANSFORM_CONFIG,
            true,
            ActionTestUtils.assertNoFailureListener(aVoid -> {
                HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                assertThat(request.username(), is(equalTo(USER_NAME)));
                assertThat(request.applicationPrivileges(), is(emptyArray()));
                assertThat(request.clusterPrivileges(), is(emptyArray()));
                assertThat(request.indexPrivileges(), is(arrayWithSize(2)));
                RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                assertThat(sourceIndicesPrivileges.allowRestrictedIndices(), is(true));
                RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[1];
                assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "index")));
                assertThat(destIndicesPrivileges.allowRestrictedIndices(), is(true));
            })
        );
    }

    public void testCheckPrivileges_NoLocalIndices_CheckDestIndexPrivileges_DestIndexExists() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(DEST_INDEX_NAME)
                            .settings(settings(IndexVersion.current()))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        TransformConfig config = new TransformConfig.Builder(TRANSFORM_CONFIG).setSource(new SourceConfig(REMOTE_SOURCE_INDEX_NAME))
            .build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            clusterState,
            client,
            config,
            true,
            ActionTestUtils.assertNoFailureListener(aVoid -> {
                HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                assertThat(request.username(), is(equalTo(USER_NAME)));
                assertThat(request.applicationPrivileges(), is(emptyArray()));
                assertThat(request.clusterPrivileges(), is(emptyArray()));
                assertThat(request.indexPrivileges(), is(arrayWithSize(1)));
                RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[0];
                assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "index")));
                assertThat(destIndicesPrivileges.allowRestrictedIndices(), is(true));
            })
        );
    }

    public void testCheckPrivileges_CheckDestIndexPrivileges_DestIndexExistsWithRetentionPolicy() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(DEST_INDEX_NAME)
                            .settings(settings(IndexVersion.current()))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        TransformConfig config = new TransformConfig.Builder(TRANSFORM_CONFIG).setRetentionPolicyConfig(
            new TimeRetentionPolicyConfig("foo", TimeValue.timeValueDays(1))
        ).build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            clusterState,
            client,
            config,
            true,
            ActionTestUtils.assertNoFailureListener(aVoid -> {
                HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                assertThat(request.username(), is(equalTo(USER_NAME)));
                assertThat(request.applicationPrivileges(), is(emptyArray()));
                assertThat(request.clusterPrivileges(), is(emptyArray()));
                assertThat(request.indexPrivileges(), is(arrayWithSize(2)));
                RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                assertThat(sourceIndicesPrivileges.allowRestrictedIndices(), is(true));
                RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[1];
                assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "index", "delete")));
                assertThat(destIndicesPrivileges.allowRestrictedIndices(), is(true));
            })
        );
    }

    public void testCheckPrivileges_CheckDestIndexPrivileges_DestAliasExists() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(DEST_INDEX_NAME)
                            .settings(settings(IndexVersion.current()))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        TransformConfig config = new TransformConfig.Builder(TRANSFORM_CONFIG).setDest(
            new DestConfig(DEST_INDEX_NAME, List.of(new DestAlias(DEST_ALIAS_NAME, randomBoolean())), null)
        ).build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            clusterState,
            client,
            config,
            true,
            ActionTestUtils.assertNoFailureListener(aVoid -> {
                HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                assertThat(request.username(), is(equalTo(USER_NAME)));
                assertThat(request.applicationPrivileges(), is(emptyArray()));
                assertThat(request.clusterPrivileges(), is(emptyArray()));
                assertThat(request.indexPrivileges(), is(arrayWithSize(3)));

                RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                assertThat(sourceIndicesPrivileges.allowRestrictedIndices(), is(true));

                RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[1];
                assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_ALIAS_NAME)));
                assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("manage")));
                assertThat(destIndicesPrivileges.allowRestrictedIndices(), is(true));

                RoleDescriptor.IndicesPrivileges destAliasesPrivileges = request.indexPrivileges()[2];
                assertThat(destAliasesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                assertThat(destAliasesPrivileges.getPrivileges(), is(arrayContaining("read", "index", "manage")));
                assertThat(destAliasesPrivileges.allowRestrictedIndices(), is(true));
            })
        );
    }

    public void testCheckPrivileges_MissingPrivileges() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(DEST_INDEX_NAME_2)
                            .settings(settings(IndexVersion.current()))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();
        TransformConfig config = new TransformConfig.Builder(TRANSFORM_CONFIG).setSource(new SourceConfig(SOURCE_INDEX_NAME_2))
            .setDest(new DestConfig(DEST_INDEX_NAME_2, null, null))
            .build();
        client.nextHasPrivilegesResponse = new HasPrivilegesResponse(
            USER_NAME,
            false,
            emptyMap(),
            List.of(SOURCE_INDEX_NAME_2_PRIVILEGES, DEST_INDEX_NAME_2_PRIVILEGES),
            emptyMap()
        );
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            SETTINGS,
            securityContext,
            indexNameExpressionResolver,
            clusterState,
            client,
            config,
            true,
            ActionListener.wrap(
                aVoid -> fail(),
                e -> assertThat(
                    e.getMessage(),
                    is(
                        equalTo(
                            "Cannot create transform [some-id] because user bob lacks the required permissions "
                                + "[some-other-source-index:[read], some-other_dest-index:[delete, read]]"
                        )
                    )
                )
            )
        );
    }

    private static class MyMockClient extends NoOpClient {

        private HasPrivilegesRequest lastHasPrivilegesRequest;
        private HasPrivilegesResponse nextHasPrivilegesResponse = new HasPrivilegesResponse(
            USER_NAME,
            true,
            emptyMap(),
            List.of(SOURCE_INDEX_NAME_PRIVILEGES, DEST_INDEX_NAME_PRIVILEGES),
            emptyMap()
        );

        MyMockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if ((request instanceof HasPrivilegesRequest) == false) {
                fail("Unexpected request type: " + request.getClass().getSimpleName());
            }
            // Save the generated request for later.
            lastHasPrivilegesRequest = (HasPrivilegesRequest) request;
            listener.onResponse((Response) nextHasPrivilegesResponse);
        }
    }
}
