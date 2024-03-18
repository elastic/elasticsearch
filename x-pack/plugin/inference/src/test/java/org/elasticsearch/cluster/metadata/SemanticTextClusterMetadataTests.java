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

    public void testAddSemanticTextField() throws Exception {
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
        assertEquals(
            resultingState.metadata().index("test").getFieldInferenceMetadata().getFieldInferenceOptions().get("field").inferenceId(),
            "test_model"
        );
    }

    private static List<MetadataMappingService.PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new MetadataMappingService.PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        })));
    }
}
