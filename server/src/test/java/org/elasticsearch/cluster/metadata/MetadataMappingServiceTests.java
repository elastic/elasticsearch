/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MetadataMappingServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testMappingClusterStateUpdateDoesntChangeExistingIndices() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").setMapping());
        final CompressedXContent currentMapping = indexService.mapperService().documentMapper().mappingSource();

        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        // TODO - it will be nice to get a random mapping generator
        final PutMappingClusterStateUpdateRequest request
            = new PutMappingClusterStateUpdateRequest("{ \"properties\": { \"field\": { \"type\": \"text\" }}}");
        request.indices(new Index[] {indexService.index()});
        final ClusterStateTaskExecutor.ClusterTasksResult<PutMappingClusterStateUpdateRequest> result =
                mappingService.putMappingExecutor.execute(clusterService.state(), Collections.singletonList(request));
        // the task completed successfully
        assertThat(result.executionResults.size(), equalTo(1));
        assertTrue(result.executionResults.values().iterator().next().isSuccess());
        // the task really was a mapping update
        assertThat(
                indexService.mapperService().documentMapper().mappingSource(),
                not(equalTo(result.resultingState.metadata().index("test").mapping().source())));
        // since we never committed the cluster state update, the in-memory state is unchanged
        assertThat(indexService.mapperService().documentMapper().mappingSource(), equalTo(currentMapping));
    }

    public void testClusterStateIsNotChangedWithIdenticalMappings() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test"));

        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request
            = new PutMappingClusterStateUpdateRequest("{ \"properties\" { \"field\": { \"type\": \"text\" }}}");
        ClusterState result = mappingService.putMappingExecutor.execute(clusterService.state(), Collections.singletonList(request))
            .resultingState;

        assertFalse(result != clusterService.state());

        ClusterState result2 = mappingService.putMappingExecutor.execute(result, Collections.singletonList(request))
            .resultingState;

        assertSame(result, result2);
    }

    public void testMappingVersion() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test"));
        final long previousVersion = indexService.getMetadata().getMappingVersion();
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request
            = new PutMappingClusterStateUpdateRequest("{ \"properties\": { \"field\": { \"type\": \"text\" }}}");
        request.indices(new Index[] {indexService.index()});
        final ClusterStateTaskExecutor.ClusterTasksResult<PutMappingClusterStateUpdateRequest> result =
                mappingService.putMappingExecutor.execute(clusterService.state(), Collections.singletonList(request));
        assertThat(result.executionResults.size(), equalTo(1));
        assertTrue(result.executionResults.values().iterator().next().isSuccess());
        assertThat(result.resultingState.metadata().index("test").getMappingVersion(), equalTo(1 + previousVersion));
    }

    public void testMappingVersionUnchanged() throws Exception {
        final IndexService indexService = createIndex("test", client().admin().indices().prepareCreate("test").setMapping());
        final long previousVersion = indexService.getMetadata().getMappingVersion();
        final MetadataMappingService mappingService = getInstanceFromNode(MetadataMappingService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final PutMappingClusterStateUpdateRequest request = new PutMappingClusterStateUpdateRequest("{ \"properties\": {}}");
        request.indices(new Index[] {indexService.index()});
        final ClusterStateTaskExecutor.ClusterTasksResult<PutMappingClusterStateUpdateRequest> result =
                mappingService.putMappingExecutor.execute(clusterService.state(), Collections.singletonList(request));
        assertThat(result.executionResults.size(), equalTo(1));
        assertTrue(result.executionResults.values().iterator().next().isSuccess());
        assertThat(result.resultingState.metadata().index("test").getMappingVersion(), equalTo(previousVersion));
    }

}
