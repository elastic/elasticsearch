/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.xpack.application.analytics.AnalyticsConstants.EVENT_DATA_STREAM_INDEX_PREFIX;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsCollectionResolverTests extends ESTestCase {

    public void testResolveExistingAnalyticsCollection() {
        String collectionName = randomIdentifier();
        String dataStreamName = EVENT_DATA_STREAM_INDEX_PREFIX + collectionName;

        ClusterState state = createClusterState(dataStreamName);
        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(
            mock(IndexNameExpressionResolver.class),
            mock(ClusterService.class)
        );

        AnalyticsCollection collection = resolver.collection(state, collectionName);

        assertEquals(collection.getName(), collectionName);
        assertEquals(collection.getEventDataStream(), dataStreamName);
    }

    public void testResolveMissingAnalyticsCollection() {
        String collectionName = randomIdentifier();

        ClusterState state = createClusterState();
        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(
            mock(IndexNameExpressionResolver.class),
            mock(ClusterService.class)
        );

        expectThrows(
            ResourceNotFoundException.class,
            "no such analytics collection [" + collectionName + "]",
            () -> resolver.collection(state, collectionName)
        );
    }

    public void testResolveMatchAllCollections() {
        String collectionName1 = randomIdentifier();
        String collectionName2 = randomIdentifier();

        ClusterState state = createClusterState();
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(state), any(), any())).thenReturn(
            Arrays.asList(EVENT_DATA_STREAM_INDEX_PREFIX + collectionName1, EVENT_DATA_STREAM_INDEX_PREFIX + collectionName2)
        );

        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(indexNameExpressionResolver, mock(ClusterService.class));
        List<AnalyticsCollection> collections = resolver.collections(state, "*");

        assertThat(collections, hasSize(2));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals(collectionName1)));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals(collectionName2)));
    }

    public void testResolveEmptyExpressionCollections() {
        String collectionName = randomIdentifier();
        String dataStreamName = EVENT_DATA_STREAM_INDEX_PREFIX + collectionName;
        ClusterState state = createClusterState();
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(state), any(), any())).thenReturn(Collections.singletonList(dataStreamName));

        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(indexNameExpressionResolver, mock(ClusterService.class));
        List<AnalyticsCollection> collections = resolver.collections(state);

        assertThat(collections, hasSize(1));
        assertEquals(collections.get(0).getName(), collectionName);
    }

    public void testResolveWildcardExpressionCollections() {
        ClusterState state = createClusterState();
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(state), any(), any())).thenReturn(
            Arrays.asList(
                EVENT_DATA_STREAM_INDEX_PREFIX + "foo",
                EVENT_DATA_STREAM_INDEX_PREFIX + "bar",
                EVENT_DATA_STREAM_INDEX_PREFIX + "baz",
                EVENT_DATA_STREAM_INDEX_PREFIX + "buz"
            )
        );

        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(indexNameExpressionResolver, mock(ClusterService.class));
        List<AnalyticsCollection> collections = resolver.collections(state, "ba*");
        assertThat(collections, hasSize(2));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals("bar")));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals("baz")));
    }

    public void testResolveMultipleExpressionsCollections() {
        ClusterState state = createClusterState();
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(state), any(), any())).thenReturn(
            Arrays.asList(
                EVENT_DATA_STREAM_INDEX_PREFIX + "foo",
                EVENT_DATA_STREAM_INDEX_PREFIX + "bar",
                EVENT_DATA_STREAM_INDEX_PREFIX + "baz",
                EVENT_DATA_STREAM_INDEX_PREFIX + "buz"
            )
        );

        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(indexNameExpressionResolver, mock(ClusterService.class));
        List<AnalyticsCollection> collections = resolver.collections(state, "foo", "ba*");
        assertThat(collections, hasSize(3));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals("foo")));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals("bar")));
        assertTrue(collections.stream().anyMatch(collection -> collection.getName().equals("baz")));
    }

    public void testResolveMultipleExpressionsWithMissingCollection() {
        ClusterState state = createClusterState();
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(state), any(), any())).thenReturn(
            Collections.singletonList(EVENT_DATA_STREAM_INDEX_PREFIX + "foo")
        );

        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(indexNameExpressionResolver, mock(ClusterService.class));
        expectThrows(
            ResourceNotFoundException.class,
            "no such analytics collection [bar]",
            () -> resolver.collections(state, "foo", "bar")
        );
    }

    public void testResolveWildcardWithNoMatchCollection() {
        ClusterState state = createClusterState();
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(eq(state), any(), any())).thenReturn(
            Collections.singletonList(EVENT_DATA_STREAM_INDEX_PREFIX + "foo")
        );
        AnalyticsCollectionResolver resolver = new AnalyticsCollectionResolver(indexNameExpressionResolver, mock(ClusterService.class));
        assertTrue(resolver.collections(state, "ba*").isEmpty());
    }

    private ClusterState createClusterState(String... dataStreams) {
        ClusterState state = mock(ClusterState.class);

        Metadata.Builder metaDataBuilder = Metadata.builder();

        for (String dataStreamName : dataStreams) {
            IndexMetadata backingIndex = createBackingIndex(dataStreamName, 1).build();
            DataStream dataStream = newInstance(dataStreamName, List.of(backingIndex.getIndex()));
            metaDataBuilder.put(backingIndex, false).put(dataStream);
        }

        when(state.metadata()).thenReturn(metaDataBuilder.build());

        return state;
    }
}
