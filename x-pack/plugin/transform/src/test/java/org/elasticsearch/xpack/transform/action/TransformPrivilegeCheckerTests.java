/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformPrivilegeCheckerTests extends ESTestCase {

    private static final String OPERATION_NAME = "some-operation";
    private static final String USER_NAME = "bob";
    private static final String TRANSFORM_ID = "some-id";
    private static final String SOURCE_INDEX_NAME = "some-source-index";
    private static final String DEST_INDEX_NAME = "some-dest-index";
    private static final TransformConfig TRANSFORM_CONFIG =
        new TransformConfig.Builder()
            .setId(TRANSFORM_ID)
            .setSource(new SourceConfig(SOURCE_INDEX_NAME))
            .setDest(new DestConfig(DEST_INDEX_NAME, null))
            .build();

    private final SecurityContext securityContext =
        new SecurityContext(Settings.EMPTY, null) {
            public User getUser() {
                return new User(USER_NAME);
            }
        };
    private final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
    private MyMockClient client;

    @Before
    public void setupClient() {
        if (client != null) {
            client.close();
        }
        client = new MyMockClient(getTestName());
    }

    @After
    public void tearDownClient() {
        client.close();
    }

    public void testCheckPrivileges_NoCheckDestIndexPrivileges() {
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            securityContext,
            indexNameExpressionResolver,
            ClusterState.EMPTY_STATE,
            client,
            TRANSFORM_CONFIG,
            false,
            ActionListener.wrap(
                aVoid -> {
                    HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                    assertThat(request.username(), is(equalTo(USER_NAME)));
                    assertThat(request.applicationPrivileges(), is(emptyArray()));
                    assertThat(request.clusterPrivileges(), is(emptyArray()));
                    assertThat(request.indexPrivileges(), is(arrayWithSize(1)));
                    RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                    assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                    assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                },
                e -> fail(e.getMessage())
            ));
    }

    public void testCheckPrivileges_CheckDestIndexPrivileges_DestIndexDoesNotExist() {
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            securityContext,
            indexNameExpressionResolver,
            ClusterState.EMPTY_STATE,
            client,
            TRANSFORM_CONFIG,
            true,
            ActionListener.wrap(
                aVoid -> {
                    HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                    assertThat(request.username(), is(equalTo(USER_NAME)));
                    assertThat(request.applicationPrivileges(), is(emptyArray()));
                    assertThat(request.clusterPrivileges(), is(emptyArray()));
                    assertThat(request.indexPrivileges(), is(arrayWithSize(2)));
                    RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                    assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                    assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                    RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[1];
                    assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                    assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "index", "create_index")));
                },
                e -> fail(e.getMessage())
            ));
    }

    public void testCheckPrivileges_CheckDestIndexPrivileges_DestIndexExists() {
        ClusterState clusterState =
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder()
                    .put(IndexMetadata.builder(DEST_INDEX_NAME)
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(1)
                        .numberOfReplicas(0)))
                .build();
        TransformPrivilegeChecker.checkPrivileges(
            OPERATION_NAME,
            securityContext,
            indexNameExpressionResolver,
            clusterState,
            client,
            TRANSFORM_CONFIG,
            true,
            ActionListener.wrap(
                aVoid -> {
                    HasPrivilegesRequest request = client.lastHasPrivilegesRequest;
                    assertThat(request.username(), is(equalTo(USER_NAME)));
                    assertThat(request.applicationPrivileges(), is(emptyArray()));
                    assertThat(request.clusterPrivileges(), is(emptyArray()));
                    assertThat(request.indexPrivileges(), is(arrayWithSize(2)));
                    RoleDescriptor.IndicesPrivileges sourceIndicesPrivileges = request.indexPrivileges()[0];
                    assertThat(sourceIndicesPrivileges.getIndices(), is(arrayContaining(SOURCE_INDEX_NAME)));
                    assertThat(sourceIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "view_index_metadata")));
                    RoleDescriptor.IndicesPrivileges destIndicesPrivileges = request.indexPrivileges()[1];
                    assertThat(destIndicesPrivileges.getIndices(), is(arrayContaining(DEST_INDEX_NAME)));
                    assertThat(destIndicesPrivileges.getPrivileges(), is(arrayContaining("read", "index")));
                },
                e -> fail(e.getMessage())
            ));
    }

    private static class MyMockClient extends NoOpClient {

        private HasPrivilegesRequest lastHasPrivilegesRequest;

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
            listener.onResponse((Response) new HasPrivilegesResponse());
        }
    }
}
