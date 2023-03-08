/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsCollectionResolverTests extends ESTestCase {

    public void testCreateClusterState() {
        createClusterState("datastream");
    }

    private ClusterState createClusterState(String... dataStreams) {
        ClusterState state = mock(ClusterState.class);
        Metadata meta = mock(Metadata.class);

        Metadata.Builder metaDataBuilder = Metadata.builder();

        for (String dataStreamName: dataStreams) {
            IndexMetadata backingIndex = createBackingIndex(dataStreamName, 1).build();
            DataStream dataStream = newInstance(dataStreamName, List.of(backingIndex.getIndex()));
            metaDataBuilder.put(backingIndex, false).put(dataStream);
        }

        when(state.metadata()).thenReturn(metaDataBuilder.build());

        return state;
    }
}
