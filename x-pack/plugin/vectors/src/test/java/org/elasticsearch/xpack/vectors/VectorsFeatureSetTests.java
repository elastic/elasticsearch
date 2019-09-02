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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
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
        VectorsFeatureSet featureSet = new VectorsFeatureSet(Settings.EMPTY, licenseState, clusterService);
        boolean available = randomBoolean();
        when(licenseState.isVectorsAllowed()).thenReturn(available);
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

    public void testEnabled() throws Exception {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        if (enabled) {
            if (randomBoolean()) {
                settings.put("xpack.vectors.enabled", enabled);
            }
        } else {
            settings.put("xpack.vectors.enabled", enabled);
        }
        VectorsFeatureSet featureSet = new VectorsFeatureSet(settings.build(), licenseState, clusterService);
        assertEquals(enabled, featureSet.enabled());
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage usage = future.get();
        assertEquals(enabled, usage.enabled());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new VectorsFeatureSetUsage(out.bytes().streamInput());
        assertEquals(enabled, serializedUsage.enabled());
    }

    public void testUsageStats() throws Exception {
        MetaData.Builder metaData = MetaData.builder();
        IndexMetaData.Builder index1 = IndexMetaData.builder("test-index1")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
            .putMapping("_doc",
                "{\"properties\":{\"my_dense_vector1\":{\"type\":\"dense_vector\",\"dims\": 10}," +
                    "\"my_dense_vector2\":{\"type\":\"dense_vector\",\"dims\": 30} }}");
        IndexMetaData.Builder index2 = IndexMetaData.builder("test-index2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
            .putMapping("_doc",
                "{\"properties\":{\"my_dense_vector3\":{\"type\":\"dense_vector\",\"dims\": 20}," +
                    "\"my_sparse_vector1\":{\"type\":\"sparse_vector\"} }}");
        metaData.put(index1);
        metaData.put(index2);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_testcluster")).metaData(metaData).build();

        Mockito.when(clusterService.state()).thenReturn(clusterState);
        when(licenseState.isVectorsAllowed()).thenReturn(true);
        Settings.Builder settings = Settings.builder();
        settings.put("xpack.vectors.enabled", true);

        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        VectorsFeatureSet vectorsFeatureSet = new VectorsFeatureSet(settings.build(), licenseState, clusterService);
        vectorsFeatureSet.usage(future);
        VectorsFeatureSetUsage vectorUsage = (VectorsFeatureSetUsage) future.get();
        assertEquals(true, vectorUsage.enabled());
        assertEquals(true, vectorUsage.available());
        assertEquals(3, vectorUsage.numDenseVectorFields());
        assertEquals(1, vectorUsage.numSparseVectorFields());
        assertEquals(20, vectorUsage.avgDenseVectorDims());
    }
}
