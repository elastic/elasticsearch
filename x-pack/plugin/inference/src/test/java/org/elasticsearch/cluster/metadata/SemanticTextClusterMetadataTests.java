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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SemanticTextClusterMetadataTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InferencePlugin.class);
    }

    public void testCreateIndexWithSemanticTextField() {
        final IndexService indexService = createIndex(
            "test",
            client().admin().indices().prepareCreate("test").setMapping("field", "type=semantic_text,model_id=test_model")
        );
        assertEquals(
            indexService.getMetadata().getFieldInferenceMetadata().getFieldInferenceOptions().get("field").inferenceId(),
            "test_model"
        );
    }

    public void testSingleSourceSemanticTextField() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest("""
            { "properties": { "field": { "type": "semantic_text", "model_id": "test_model" }}}""");
        request.indices(new Index[] { indexService.index() });
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        FieldInferenceMetadata.FieldInferenceOptions fieldInferenceOptions = resultingState.metadata().index("test").getFieldInferenceMetadata().getFieldInferenceOptions().get("field");
        assertEquals(fieldInferenceOptions.inferenceId(), "test_model");
        assertEquals(fieldInferenceOptions.sourceFields(), Set.of("field"));
    }

    public void testMultiFieldsSemanticTextField() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest("""
            {
              "properties": {
                "top_field": {
                    "type": "text",
                    "fields": {
                      "semantic": {
                        "type": "semantic_text",
                        "model_id": "test_model"
                      }
                    }
                }
              }
            }
            """);
        request.indices(new Index[] { indexService.index() });
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        IndexMetadata indexMetadata = resultingState.metadata().index("test");
        FieldInferenceMetadata.FieldInferenceOptions fieldInferenceOptions = indexMetadata.getFieldInferenceMetadata().getFieldInferenceOptions().get("top_field.semantic");
        assertEquals(fieldInferenceOptions.inferenceId(), "test_model");
        assertEquals(fieldInferenceOptions.sourceFields(), Set.of("top_field"));
    }

    public void testCopyToSemanticTextField() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest("""
            {
              "properties": {
                "semantic": {
                  "type": "semantic_text",
                  "model_id": "test_model"
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
            """);
        request.indices(new Index[] { indexService.index() });
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        IndexMetadata indexMetadata = resultingState.metadata().index("test");
        FieldInferenceMetadata.FieldInferenceOptions fieldInferenceOptions = indexMetadata.getFieldInferenceMetadata().getFieldInferenceOptions().get("semantic");
        assertEquals(fieldInferenceOptions.inferenceId(), "test_model");
        assertEquals(fieldInferenceOptions.sourceFields(), Set.of("semantic", "copy_origin_1", "copy_origin_2"));
    }

    public void testCopyToAndMultiFieldsSemanticTextField() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final MetadataMappingService.PutMappingExecutor putMappingExecutor = mappingService.new PutMappingExecutor();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest("""
            {
              "properties": {
                "top_field": {
                  "type": "text",
                  "fields": {
                    "semantic": {
                      "type": "semantic_text",
                      "model_id": "test_model"
                    }
                  }
                },
                "copy_origin_1": {
                  "type": "text",
                  "copy_to": "top_field"
                },
                "copy_origin_2": {
                  "type": "text",
                  "copy_to": "top_field"
                }
              }
            }
            """);
        request.indices(new Index[] { indexService.index() });
        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterService.state(),
            putMappingExecutor,
            singleTask(request)
        );
        IndexMetadata indexMetadata = resultingState.metadata().index("test");
        FieldInferenceMetadata.FieldInferenceOptions fieldInferenceOptions = indexMetadata.getFieldInferenceMetadata().getFieldInferenceOptions().get("top_field.semantic");
        assertEquals(fieldInferenceOptions.inferenceId(), "test_model");
        assertEquals(fieldInferenceOptions.sourceFields(), Set.of("top_field", "copy_origin_1", "copy_origin_2"));
    }

    private static List<MetadataMappingService.PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new MetadataMappingService.PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        })));
    }
}
