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
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
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

    /**
     * {@link ReindexValidator#normalize} should set the slice field to {@link IdFieldMapper#NAME} when the client specifies
     * {@code source.slice} with id/max only (no {@code field}), matching PIT-oriented manual slicing behavior.
     */
    public void testNormalizeDefaultsManualSliceFieldToId() {
        ReindexValidator validator = newValidator();
        ReindexRequest request = new ReindexRequest().setSourceIndices("source").setDestIndex("dest");
        request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(0, 2)));
        assertThat(request.getSearchRequest().source().slice().getField(), nullValue());

        validator.normalize(request);

        assertThat(
            "manual slice without field should default to _id",
            request.getSearchRequest().source().slice().getField(),
            equalTo(IdFieldMapper.NAME)
        );
    }

    /**
     * When the search body has no {@link SearchSourceBuilder#slice()}, {@link ReindexValidator#normalize} must not add a slice
     * and must leave the existing {@link SearchSourceBuilder} instance unchanged.
     */
    public void testNormalizeDoesNothingWhenSliceNotSet() {
        ReindexValidator validator = newValidator();
        ReindexRequest request = new ReindexRequest().setSourceIndices("source").setDestIndex("dest");
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        request.getSearchRequest().source(source);
        assertThat(source.slice(), nullValue());

        validator.normalize(request);

        assertThat(request.getSearchRequest().source(), sameInstance(source));
        assertThat(request.getSearchRequest().source().slice(), nullValue());
    }

    /**
     * If {@code source.slice} already names a field (e.g. numeric doc-values slicing), {@link ReindexValidator#normalize} must not
     * overwrite id/max or replace the field with {@code _id}.
     */
    public void testNormalizePreservesExplicitSliceField() {
        ReindexValidator validator = newValidator();
        ReindexRequest request = new ReindexRequest().setSourceIndices("source").setDestIndex("dest");
        SliceBuilder originalSlice = new SliceBuilder("rank", 1, 4);
        request.getSearchRequest().source(new SearchSourceBuilder().slice(originalSlice));
        assertThat(request.getSearchRequest().source().slice().getField(), equalTo("rank"));

        validator.normalize(request);

        assertThat(request.getSearchRequest().source().slice().getField(), equalTo("rank"));
        assertThat(request.getSearchRequest().source().slice().getId(), equalTo(1));
        assertThat(request.getSearchRequest().source().slice().getMax(), equalTo(4));
    }

    /** Minimal {@link ReindexValidator} for tests that only exercise {@link ReindexValidator#normalize}. */
    private static ReindexValidator newValidator() {
        IndexNameExpressionResolver indexResolver = TestIndexNameExpressionResolver.newInstance();
        Settings settings = Settings.EMPTY;
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            indexResolver,
            EmptySystemIndices.INSTANCE
        );
        return new ReindexValidator(settings, mock(ClusterService.class), indexResolver, DefaultProjectResolver.INSTANCE, autoCreateIndex);
    }
}
