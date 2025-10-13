/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

public class SemanticTextClusterMetadataTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    public void testCreateIndexWithSemanticTextField() {
        final IndexService indexService = createIndex(
            "test",
            client().admin().indices().prepareCreate("test").setMapping("field", "type=semantic_text,inference_id=test_model")
        );
        assertEquals(indexService.getMetadata().getInferenceFields().get("field").getInferenceId(), "test_model");
    }

    public void testSingleSourceSemanticTextField() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                { "properties": { "field": { "type": "semantic_text", "inference_id": "test_model" }}}""",
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        assertEquals(resultingState.metadata().getProject().index("test").getInferenceFields().get("field").getInferenceId(), "test_model");
    }

    public void testCopyToSemanticTextField() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            """
                {
                  "properties": {
                    "semantic": {
                      "type": "semantic_text",
                      "inference_id": "test_model"
                    },
                    "copy_origin_1": {
                      "type": "text",
                      "copy_to": "semantic"
                    },
                    "copy_origin_2": {
                      "type": "text",
                      "copy_to": "semantic"
                    }
                  }
                }
                """,
            false,
            indexService.index()
        );
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        IndexMetadata indexMetadata = resultingState.metadata().getProject().index("test");
        InferenceFieldMetadata inferenceFieldMetadata = indexMetadata.getInferenceFields().get("semantic");
        assertThat(inferenceFieldMetadata.getInferenceId(), equalTo("test_model"));
        assertThat(
            Arrays.asList(inferenceFieldMetadata.getSourceFields()),
            Matchers.containsInAnyOrder("semantic", "copy_origin_1", "copy_origin_2")
        );
    }

    private static List<MetadataMappingService.PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new MetadataMappingService.PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        })));
    }
}
