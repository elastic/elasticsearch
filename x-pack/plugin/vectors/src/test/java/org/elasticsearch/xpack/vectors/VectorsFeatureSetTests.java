/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;
import org.junit.Before;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VectorsFeatureSetTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private ClusterService clusterService;

    @Before
    public void init() {
        licenseState = mock(XPackLicenseState.class);
        clusterService = mock(ClusterService.class);
    }

    public void testAvailable() throws Exception {
        VectorsFeatureSet featureSet = new VectorsFeatureSet(licenseState, clusterService);
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.VECTORS)).thenReturn(available);
        assertEquals(available, featureSet.available());

        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertEquals(available, usage.available());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new VectorsFeatureSetUsage(out.bytes().streamInput());
        assertEquals(available, serializedUsage.available());
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
        when(licenseState.isAllowed(XPackLicenseState.Feature.VECTORS)).thenReturn(true);

        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        VectorsFeatureSet vectorsFeatureSet = new VectorsFeatureSet(licenseState, clusterService);
        vectorsFeatureSet.usage(future);
        VectorsFeatureSetUsage vectorUsage = (VectorsFeatureSetUsage) future.get();
        assertEquals(true, vectorUsage.enabled());
        assertEquals(true, vectorUsage.available());
        assertEquals(3, vectorUsage.numDenseVectorFields());
        assertEquals(1, vectorUsage.numSparseVectorFields());
        assertEquals(20, vectorUsage.avgDenseVectorDims());
    }
}
