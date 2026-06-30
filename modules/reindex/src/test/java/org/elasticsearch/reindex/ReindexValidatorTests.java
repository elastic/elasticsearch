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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.SliceIndexing;
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

    public void testRejectRoutingInSliceEnabledDestination() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexValidator validator = validatorWithProject(projectMetadataWithDestinationSliceSetting(true));
        ReindexRequest request = new ReindexRequest().setSourceIndices("source-index").setDestIndex("dest-index");
        request.getDestination().routing("keep");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator.initialValidation(request));
        assertThat(
            e.getMessage(),
            containsString(
                "[routing] is not allowed in [dest] when ["
                    + IndexSettings.SLICE_ENABLED.getKey()
                    + "] is true for destination [dest-index]"
            )
        );
    }

    public void testRequireSliceInSliceEnabledDestination() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexValidator validator = validatorWithProject(projectMetadataWithDestinationSliceSetting(true));
        ReindexRequest request = new ReindexRequest().setSourceIndices("source-index").setDestIndex("dest-index");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator.initialValidation(request));
        assertThat(e.getMessage(), containsString("[" + SliceIndexing.PARAM_NAME + "] is required in [dest]"));
    }

    public void testAllowSliceInSliceEnabledDestination() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexValidator validator = validatorWithProject(projectMetadataWithDestinationSliceSetting(true));
        ReindexRequest request = new ReindexRequest().setSourceIndices("source-index").setDestIndex("dest-index");
        request.getDestination().routing("keep").setRoutingFromSlice(true);

        validator.initialValidation(request);
    }

    public void testRejectSliceInSliceDisabledDestination() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexValidator validator = validatorWithProject(projectMetadataWithDestinationSliceSetting(false));
        ReindexRequest request = new ReindexRequest().setSourceIndices("source-index").setDestIndex("dest-index");
        request.getDestination().routing("keep").setRoutingFromSlice(true);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator.initialValidation(request));
        assertThat(e.getMessage(), containsString("[" + SliceIndexing.PARAM_NAME + "] is not allowed in [dest]"));
    }

    public void testRequireSliceInSliceEnabledDestinationFromV1Template() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexValidator validator = validatorWithProject(projectMetadataWithDestinationV1TemplateSetting(true));
        ReindexRequest request = new ReindexRequest().setSourceIndices("source-index").setDestIndex("dest-auto");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator.initialValidation(request));
        assertThat(e.getMessage(), containsString("[" + SliceIndexing.PARAM_NAME + "] is required in [dest]"));
    }

    public void testRejectSliceInSliceDisabledDestinationFromV1Template() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexValidator validator = validatorWithProject(projectMetadataWithDestinationV1TemplateSetting(false));
        ReindexRequest request = new ReindexRequest().setSourceIndices("source-index").setDestIndex("dest-auto");
        request.getDestination().routing("keep").setRoutingFromSlice(true);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator.initialValidation(request));
        assertThat(e.getMessage(), containsString("[" + SliceIndexing.PARAM_NAME + "] is not allowed in [dest]"));
    }

    private ReindexValidator validatorWithProject(ProjectMetadata projectMetadata) {
        IndexNameExpressionResolver indexResolver = TestIndexNameExpressionResolver.newInstance();
        Settings settings = Settings.EMPTY;
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            indexResolver,
            EmptySystemIndices.INSTANCE
        );
        return new ReindexValidator(
            settings,
            clusterService(projectMetadata),
            indexResolver,
            TestProjectResolvers.singleProject(projectMetadata.id()),
            autoCreateIndex
        );
    }

    private static ClusterService clusterService(ProjectMetadata projectMetadata) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadata).build();
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    private ProjectMetadata projectMetadataWithDestinationSliceSetting(boolean destinationSliceEnabled) {
        return ProjectMetadata.builder(randomUniqueProjectId())
            .put(
                IndexMetadata.builder("source-index")
                    .settings(indexSettings(IndexVersion.current(), 1, 0))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                true
            )
            .put(
                IndexMetadata.builder("dest-index")
                    .settings(
                        indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.SLICE_ENABLED.getKey(), destinationSliceEnabled)
                            .build()
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                true
            )
            .build();
    }

    private ProjectMetadata projectMetadataWithDestinationV1TemplateSetting(boolean destinationSliceEnabled) {
        return ProjectMetadata.builder(randomUniqueProjectId())
            .put(
                IndexMetadata.builder("source-index")
                    .settings(indexSettings(IndexVersion.current(), 1, 0))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                true
            )
            .put(
                IndexTemplateMetadata.builder("dest-template")
                    .patterns(java.util.List.of("dest-*"))
                    .settings(
                        indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.SLICE_ENABLED.getKey(), destinationSliceEnabled)
                    )
            )
            .build();
    }
}
