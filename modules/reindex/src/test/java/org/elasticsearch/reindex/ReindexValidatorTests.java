/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReindexValidatorTests extends ESTestCase {
    public void testProjectRoutingIsntAllowedWhenCPSIsDisabled() {
        IndexNameExpressionResolver indexResolver = TestIndexNameExpressionResolver.newInstance();
        Settings settings = Settings.EMPTY;
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            indexResolver,
            EmptySystemIndices.INSTANCE
        );
        ReindexValidator validator = new ReindexValidator(
            settings,
            mock(ClusterService.class),
            indexResolver,
            DefaultProjectResolver.INSTANCE,
            autoCreateIndex
        );

        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("source-index");
        request.setDestIndex("dest-index");
        request.getSearchRequest().indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
        request.getSearchRequest().setProjectRouting("_alias:linked");

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> validator.initialValidation(request)
        );
        assertThat(
            e.getMessage(),
            containsString("reindex doesn't support project routing [_alias:linked] when cross-project search is disabled")
        );
    }

    public void testProjectRoutingAllowedWhenRemoteInfoIsSet() {
        IndexNameExpressionResolver indexResolver = TestIndexNameExpressionResolver.newInstance();
        Settings settings = Settings.builder().put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "127.0.0.1:9200").build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            indexResolver,
            EmptySystemIndices.INSTANCE
        );

        ProjectId projectId = randomUniqueProjectId();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        ProjectResolver projectResolver = TestProjectResolvers.singleProject(projectId);
        ReindexValidator validator = new ReindexValidator(settings, clusterService, indexResolver, projectResolver, autoCreateIndex);

        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("source-index");
        request.setDestIndex("dest-index");
        request.getSearchRequest().indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
        request.getSearchRequest().setProjectRouting("_alias:linked");
        request.setRemoteInfo(
            new RemoteInfo(
                "http",
                "127.0.0.1",
                9200,
                null,
                new BytesArray("{\"match_all\":{}}"),
                null,
                null,
                Map.of(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            )
        );

        try {
            validator.initialValidation(request);
        } catch (Exception e) {
            fail(e, "Expected no exception, but got " + e.getMessage());
        }
    }
}
