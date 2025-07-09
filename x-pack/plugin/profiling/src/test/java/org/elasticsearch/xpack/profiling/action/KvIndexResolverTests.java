/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KvIndexResolverTests extends ESTestCase {
    private KvIndexResolver resolver;
    private IndexNameExpressionResolver mockIndexResolver;

    @Before
    public void create() {
        mockIndexResolver = mock(IndexNameExpressionResolver.class);
        resolver = new KvIndexResolver(mockIndexResolver, TimeValue.timeValueHours(3));
    }

    private Index idx(String name) {
        return new Index(name, UUID.randomUUID().toString());
    }

    public void testResolveSingleIndex() {
        String indexPattern = "profiling-stacktraces";
        Index[] concreteIndices = new Index[] { idx(".profiling-stacktraces-v001-000001") };
        when(mockIndexResolver.concreteIndices(any(ClusterState.class), eq(IndicesOptions.STRICT_EXPAND_OPEN), eq(indexPattern)))
            .thenReturn(concreteIndices);

        List<Index> resolvedIndices = resolver.resolve(ClusterState.EMPTY_STATE, indexPattern, Instant.MIN, Instant.MAX);
        assertEquals(1, resolvedIndices.size());
        assertEquals(concreteIndices[0], resolvedIndices.get(0));
    }

    public void testResolveRangeOfIndices() {
        String indexPattern = "profiling-stacktraces";
        Index stGen1 = idx(".profiling-stacktraces-v001-000001");
        Index stGen2 = idx(".profiling-stacktraces-v001-000002");
        Index stGen3 = idx(".profiling-stacktraces-v001-000003");
        Index[] concreteIndices = new Index[] { stGen1, stGen2, stGen3 };
        when(mockIndexResolver.concreteIndices(any(ClusterState.class), eq(IndicesOptions.STRICT_EXPAND_OPEN), eq(indexPattern)))
            .thenReturn(concreteIndices);

        Metadata.Builder metaBuilder = new Metadata.Builder();
        metaBuilder.indices(
            Map.of(
                // Jan 15, 2023
                stGen1.getName(),
                metadata(stGen1, 1673740800000L),
                // Feb 1, 2023
                stGen2.getName(),
                metadata(stGen2, 1675209600000L),
                // Feb 15, 2023
                stGen3.getName(),
                metadata(stGen3, 1676419200000L)
            )
        );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metaBuilder).build();

        List<Index> resolvedIndices = resolver.resolve(
            clusterState,
            indexPattern,
            // Feb 3, 2023 -> Feb 18, 2023
            Instant.ofEpochMilli(1675382400000L),
            Instant.ofEpochMilli(1676678400000L)
        );
        assertEquals(2, resolvedIndices.size());
        // resolved indices are sorted from newest to oldest
        assertEquals(stGen3, resolvedIndices.get(0));
        assertEquals(stGen2, resolvedIndices.get(1));
    }

    public void testResolveRangeOfIndicesAtBoundary() {
        String indexPattern = "profiling-stacktraces";
        Index stGen1 = idx(".profiling-stacktraces-v001-000001");
        Index stGen2 = idx(".profiling-stacktraces-v001-000002");
        Index stGen3 = idx(".profiling-stacktraces-v001-000003");
        Index[] concreteIndices = new Index[] { stGen1, stGen2, stGen3 };
        when(mockIndexResolver.concreteIndices(any(ClusterState.class), eq(IndicesOptions.STRICT_EXPAND_OPEN), eq(indexPattern)))
            .thenReturn(concreteIndices);

        Metadata.Builder metaBuilder = new Metadata.Builder();
        metaBuilder.indices(
            Map.of(
                // Jan 15, 2023
                stGen1.getName(),
                metadata(stGen1, 1673740800000L),
                // Feb 1, 2023
                stGen2.getName(),
                metadata(stGen2, 1675209600000L),
                // Feb 15, 2023
                stGen3.getName(),
                metadata(stGen3, 1676419200000L)
            )
        );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metaBuilder).build();

        List<Index> resolvedIndices = resolver.resolve(
            clusterState,
            indexPattern,
            // Feb 1, 2023 -> Feb 5, 2023
            Instant.ofEpochMilli(1675209600000L),
            Instant.ofEpochMilli(1675555200000L)
        );

        assertEquals(2, resolvedIndices.size());
        // should consider the first two indices because the first index overlaps by a small period
        assertEquals(stGen2, resolvedIndices.get(0));
        assertEquals(stGen1, resolvedIndices.get(1));
    }

    private IndexMetadata metadata(Index index, long creationDate) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return IndexMetadata.builder(index.getName())
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(creationDate)
            .build();
    }

    public void testResolveAllIndices() {
        String indexPattern = "profiling-stacktraces";
        Index stV1 = idx(".profiling-stacktraces-v001-000001");
        Index stV2 = idx(".profiling-stacktraces-v002-000001");
        Index[] concreteIndices = new Index[] { stV1, stV2 };
        when(mockIndexResolver.concreteIndices(any(ClusterState.class), eq(IndicesOptions.STRICT_EXPAND_OPEN), eq(indexPattern)))
            .thenReturn(concreteIndices);

        Metadata.Builder metaBuilder = new Metadata.Builder();
        metaBuilder.indices(
            Map.of(
                // Feb 1, 2023
                stV1.getName(),
                metadata(stV1, 1675209600000L),
                // Feb 15, 2023
                stV2.getName(),
                metadata(stV2, 1676419200000L)
            )
        );

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metaBuilder).build();

        List<Index> resolvedIndices = resolver.resolve(
            clusterState,
            indexPattern,
            // Jan 1, 2023 -> Jan 2, 2023
            Instant.ofEpochMilli(1672531200000L),
            Instant.ofEpochMilli(1672631200000L)
        );
        assertEquals(2, resolvedIndices.size());
        assertEquals(stV1, resolvedIndices.get(0));
        assertEquals(stV2, resolvedIndices.get(1));
    }
}
