/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStreamsActionUtilTests extends ESTestCase {

    public void testDataStreamsResolveConcreteIndexNames() {

        var index1 = new Index("foo1", IndexMetadata.INDEX_UUID_NA_VALUE);
        var index3 = new Index("bar", IndexMetadata.INDEX_UUID_NA_VALUE);

        var dataStreamIndex1 = new Index(".ds-foo1", IndexMetadata.INDEX_UUID_NA_VALUE);
        var dataStreamIndex2 = new Index(".ds-bar2", IndexMetadata.INDEX_UUID_NA_VALUE);
        var dataStreamIndex3 = new Index(".ds-foo2", IndexMetadata.INDEX_UUID_NA_VALUE);
        var dataStreamIndex4 = new Index(".ds-baz1", IndexMetadata.INDEX_UUID_NA_VALUE);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        DataStreamMetadata.TYPE,
                        new DataStreamMetadata(
                            ImmutableOpenMap.<String, DataStream>builder()
                                .fPut("fooDs", DataStreamTestHelper.newInstance("fooDs", List.of(dataStreamIndex1)))
                                .fPut("barDs", DataStreamTestHelper.newInstance("barDs", List.of(dataStreamIndex2)))
                                .fPut("foo2Ds", DataStreamTestHelper.newInstance("foo2Ds", List.of(dataStreamIndex3)))
                                .fPut("bazDs", DataStreamTestHelper.newInstance("bazDs", List.of(dataStreamIndex4)))
                                .build(),
                            ImmutableOpenMap.of()
                        )
                    )
                    .indices(
                        createLocalOnlyIndicesMetadata(
                            index1,
                            index3,
                            dataStreamIndex1,
                            dataStreamIndex2,
                            dataStreamIndex3,
                            dataStreamIndex4
                        )
                    )
                    .build()
            )
            .build();

        var query = new String[] { "foo*", "baz*" };
        var indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(indexNameExpressionResolver.dataStreamNames(any(), any(), eq(query))).thenReturn(List.of("fooDs", "foo2Ds", "bazDs"));

        var resolved = DataStreamsActionUtil.resolveConcreteIndexNames(
            indexNameExpressionResolver,
            clusterState,
            query,
            IndicesOptions.builder().wildcardOptions(IndicesOptions.WildcardOptions.builder().includeHidden(true)).build()
        ).toList();

        assertThat(resolved, containsInAnyOrder(".ds-foo1", ".ds-foo2", ".ds-baz1"));
    }

    private Map<String, IndexMetadata> createLocalOnlyIndicesMetadata(Index... indices) {
        return Arrays.stream(indices)
            .map(
                index1 -> Map.entry(
                    index1.getName(),
                    IndexMetadata.builder(index1.getName())
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                        .numberOfReplicas(0)
                        .numberOfShards(1)
                        .build()
                )
            )
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
