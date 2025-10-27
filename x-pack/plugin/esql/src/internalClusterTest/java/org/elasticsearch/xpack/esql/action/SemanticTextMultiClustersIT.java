/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.ccs.AbstractSemanticCrossClusterSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class SemanticTextMultiClustersIT extends AbstractSemanticCrossClusterSearchTestCase {
    private static final String LOCAL_INDEX_NAME = "local_index";
    private static final String REMOTE_INDEX_NAME = "remote_index";

    // Boost the local index so that we can use the same doc values for local and remote indices and have consistent relevance
    private static final List<IndexWithBoost> QUERY_INDICES = List.of(
        new IndexWithBoost(LOCAL_INDEX_NAME, 10.0f),
        new IndexWithBoost(fullyQualifiedIndexName(REMOTE_CLUSTER, REMOTE_INDEX_NAME))
    );

    private static final String COMMON_INFERENCE_ID_FIELD = "common_inference_id_field";
    private static final String VARIABLE_INFERENCE_ID_FIELD = "variable_inference_id_field";
    private static final String MIXED_TYPE_FIELD_1 = "mixed_type_field_1";
    private static final String MIXED_TYPE_FIELD_2 = "mixed_type_field_2";
    private static final String TEXT_FIELD = "text_field";

    boolean clustersConfigured = false;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return plugins;
    }

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (clustersConfigured == false) {
            configureClusters();
            clustersConfigured = true;
        }
    }

    public void testQuerySemanticTextField() {
        EsqlQueryRequest request = new EsqlQueryRequest().query("""
            FROM local_index, cluster_a:remote_index |
            WHERE MATCH(common_inference_id_field, "a") |
            KEEP common_inference_id_field |
            LIMIT 10
            """);

        try (EsqlQueryResponse response = runQuery(request)) {
            List<List<Object>> values = getValuesList(response);
            assertThat(values, hasSize(2));

            List<String> fieldValues = values.stream().map(Object::toString).toList();
            assertThat(fieldValues, equalTo(List.of("[a]", "[a]")));

            Map<String, EsqlExecutionInfo.Cluster> clusters = response.getExecutionInfo().getClusters();
            assertThat(clusters.size(), equalTo(2));

            EsqlExecutionInfo.Cluster localCluster = clusters.get(LOCAL_CLUSTER);
            assertThat(localCluster, notNullValue());
            assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));

            EsqlExecutionInfo.Cluster remoteCluster = clusters.get(REMOTE_CLUSTER);
            assertThat(remoteCluster, notNullValue());
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteCluster.getTotalShards()));
        }
    }

    private EsqlQueryResponse runQuery(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }

    private void configureClusters() throws Exception {
        final String commonInferenceId = "common-inference-id";
        final String localInferenceId = "local-inference-id";
        final String remoteInferenceId = "remote-inference-id";

        final Map<String, Map<String, Object>> docs = Map.of(
            getDocId(COMMON_INFERENCE_ID_FIELD),
            Map.of(COMMON_INFERENCE_ID_FIELD, "a"),
            getDocId(VARIABLE_INFERENCE_ID_FIELD),
            Map.of(VARIABLE_INFERENCE_ID_FIELD, "b"),
            getDocId(MIXED_TYPE_FIELD_1),
            Map.of(MIXED_TYPE_FIELD_1, "c"),
            getDocId(MIXED_TYPE_FIELD_2),
            Map.of(MIXED_TYPE_FIELD_2, "d"),
            getDocId(TEXT_FIELD),
            Map.of(TEXT_FIELD, "e")
        );

        final TestIndexInfo localIndexInfo = new TestIndexInfo(
            LOCAL_INDEX_NAME,
            Map.of(commonInferenceId, sparseEmbeddingServiceSettings(), localInferenceId, sparseEmbeddingServiceSettings()),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(commonInferenceId),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(localInferenceId),
                MIXED_TYPE_FIELD_1,
                semanticTextMapping(localInferenceId),
                MIXED_TYPE_FIELD_2,
                textMapping(),
                TEXT_FIELD,
                textMapping()
            ),
            docs
        );
        final TestIndexInfo remoteIndexInfo = new TestIndexInfo(
            REMOTE_INDEX_NAME,
            Map.of(
                commonInferenceId,
                textEmbeddingServiceSettings(256, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
                remoteInferenceId,
                textEmbeddingServiceSettings(384, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            ),
            Map.of(
                COMMON_INFERENCE_ID_FIELD,
                semanticTextMapping(commonInferenceId),
                VARIABLE_INFERENCE_ID_FIELD,
                semanticTextMapping(remoteInferenceId),
                MIXED_TYPE_FIELD_1,
                textMapping(),
                MIXED_TYPE_FIELD_2,
                semanticTextMapping(remoteInferenceId),
                TEXT_FIELD,
                textMapping()
            ),
            docs
        );
        setupTwoClusters(localIndexInfo, remoteIndexInfo);
    }

    private static String getDocId(String field) {
        return field + "_doc";
    }
}
