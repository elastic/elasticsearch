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
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SemanticTextMetadataMappingServiceTests extends MlSingleNodeTestCase {
    public void testCreateIndexWithSemanticTextField() {
        final IndexService indexService = createIndex(
            "test",
            client().admin().indices().prepareCreate("test").setMapping("field", "type=semantic_text,model_id=test_model")
        );
        assertEquals(Map.of("test_model", Set.of("field")), indexService.getMetadata().getFieldsForModels());
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
        assertEquals(Map.of("test_model", Set.of("field")), resultingState.metadata().index("test").getFieldsForModels());
    }

    private static List<MetadataMappingService.PutMappingClusterStateUpdateTask> singleTask(PutMappingClusterStateUpdateRequest request) {
        return Collections.singletonList(new MetadataMappingService.PutMappingClusterStateUpdateTask(request, ActionListener.running(() -> {
            throw new AssertionError("task should not complete publication");
        })));
    }
}
