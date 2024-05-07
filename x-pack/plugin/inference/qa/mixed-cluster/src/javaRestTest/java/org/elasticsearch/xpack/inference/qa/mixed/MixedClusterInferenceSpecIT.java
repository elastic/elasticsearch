/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.TestFeatureService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.hasSize;

public class MixedClusterInferenceSpecIT extends InferenceBaseRestTest {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version bwcVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    private static TestFeatureService oldClusterTestFeatureService = null;

    @Before
    public void extractOldClusterFeatures() {
        if (oldClusterTestFeatureService == null) {
            oldClusterTestFeatureService = testFeatureService;
        }
    }

    protected static boolean oldClusterHasFeature(String featureId) {
        assert oldClusterTestFeatureService != null;
        return oldClusterTestFeatureService.clusterHasFeature(featureId);
    }

    protected static boolean oldClusterHasFeature(NodeFeature feature) {
        return oldClusterHasFeature(feature.id());
    }

    @AfterClass
    public static void cleanUp() {
        oldClusterTestFeatureService = null;
    }

    @SuppressWarnings("unchecked")
    public void testGet() throws IOException {
        for (int i = 0; i < 5; i++) {
            putModel("se_model_" + i, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            putModel("te_model_" + i, mockSparseServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        }

        var getAllModels = getAllModels();
        assertThat(getAllModels, hasSize(9));

        var getSparseModels = getModels("_all", TaskType.SPARSE_EMBEDDING);
        assertThat(getSparseModels, hasSize(5));
        for (var sparseModel : getSparseModels) {
            assertEquals("sparse_embedding", sparseModel.get("task_type"));
        }

        var getDenseModels = getModels("_all", TaskType.TEXT_EMBEDDING);
        assertThat(getDenseModels, hasSize(4));
        for (var denseModel : getDenseModels) {
            assertEquals("text_embedding", denseModel.get("task_type"));
        }

        var singleModel = getModels("se_model_1", TaskType.SPARSE_EMBEDDING);
        assertThat(singleModel, hasSize(1));
        assertEquals("se_model_1", singleModel.get(0).get("model_id"));

        for (int i = 0; i < 5; i++) {
            deleteModel("se_model_" + i, TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            deleteModel("te_model_" + i, TaskType.TEXT_EMBEDDING);
        }
    }

}
