/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;
import org.junit.Before;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class VectorsFeatureSetTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void init() {
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() throws Exception {
        VectorsFeatureSet featureSet = new VectorsFeatureSet(clusterService);
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertThat(usage.available(), is(true));
    }

    public void testUsageStats() throws Exception {
        Metadata.Builder metadata = Metadata.builder();
        IndexMetadata.Builder index1 = IndexMetadata.builder("test-index1")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
            .putMapping("_doc",
                "{\"properties\":{\"my_dense_vector1\":{\"type\":\"dense_vector\",\"dims\": 10}," +
                    "\"my_dense_vector2\":{\"type\":\"dense_vector\",\"dims\": 30} }}");
        IndexMetadata.Builder index2 = IndexMetadata.builder("test-index2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
            .putMapping("_doc",
                "{\"properties\":{\"my_dense_vector3\":{\"type\":\"dense_vector\",\"dims\": 20}," +
                    "\"my_sparse_vector1\":{\"type\":\"sparse_vector\"} }}");
        metadata.put(index1);
        metadata.put(index2);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_testcluster")).metadata(metadata).build();

        Mockito.when(clusterService.state()).thenReturn(clusterState);

        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        VectorsFeatureSet vectorsFeatureSet = new VectorsFeatureSet(clusterService);
        vectorsFeatureSet.usage(future);
        VectorsFeatureSetUsage vectorUsage = (VectorsFeatureSetUsage) future.get();
        assertEquals(true, vectorUsage.enabled());
        assertEquals(true, vectorUsage.available());
        assertEquals(3, vectorUsage.numDenseVectorFields());
        assertEquals(1, vectorUsage.numSparseVectorFields());
        assertEquals(20, vectorUsage.avgDenseVectorDims());
    }
}
