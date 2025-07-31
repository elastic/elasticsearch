/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction.checkBlocksAndFilterIndices;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportFieldCapabilitiesActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCCSCompatibilityCheck() {
        Settings settings = Settings.builder()
            .put("node.name", TransportFieldCapabilitiesActionTests.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_CCS_VERSION, true);
        try {
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                transportVersion,
                threadPool
            );

            FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest();
            fieldCapsRequest.indexFilter(new DummyQueryBuilder() {
                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                    if (out.getTransportVersion().before(transportVersion)) {
                        throw new IllegalArgumentException("This query isn't serializable before transport version " + transportVersion);
                    }
                }
            });

            IndicesService indicesService = mock(IndicesService.class);
            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            TransportFieldCapabilitiesAction action = new TransportFieldCapabilitiesAction(
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                indicesService,
                null,
                null
            );

            IllegalArgumentException ex = safeAwaitFailure(
                IllegalArgumentException.class,
                FieldCapabilitiesResponse.class,
                l -> action.doExecute(null, fieldCapsRequest, l)
            );

            assertThat(
                ex.getMessage(),
                containsString("[class org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest] is not compatible with version")
            );
            assertThat(ex.getMessage(), containsString("and the 'search.check_ccs_compatibility' setting is enabled."));
            assertEquals("This query isn't serializable before transport version " + transportVersion, ex.getCause().getMessage());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    public void testCheckBlocksAndFilterIndices() {
        final ProjectId projectId = randomProjectIdOrDefault();
        String[] concreteIndices = { "index1", "index2", "index3" };

        // No blocks
        {
            final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(ProjectMetadata.builder(projectId).build())
                .build();
            final ProjectState projectState = clusterState.projectState(projectId);
            String[] result = checkBlocksAndFilterIndices(projectState, concreteIndices);
            assertArrayEquals(concreteIndices, result);
        }

        // Global READ block
        {
            final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(ProjectMetadata.builder(projectId).build())
                .blocks(
                    ClusterBlocks.builder()
                        .addGlobalBlock(
                            new ClusterBlock(
                                0,
                                "id",
                                false,
                                false,
                                false,
                                RestStatus.SERVICE_UNAVAILABLE,
                                EnumSet.of(ClusterBlockLevel.READ)
                            )
                        )
                )
                .build();
            final ProjectState projectState = clusterState.projectState(projectId);
            expectThrows(ClusterBlockException.class, () -> checkBlocksAndFilterIndices(projectState, concreteIndices));
        }

        // Index-level READ block
        {
            final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(ProjectMetadata.builder(projectId).build())
                .blocks(ClusterBlocks.builder().addIndexBlock(projectId, "index1", IndexMetadata.INDEX_READ_BLOCK))
                .build();
            final ProjectState projectState = clusterState.projectState(projectId);
            expectThrows(ClusterBlockException.class, () -> checkBlocksAndFilterIndices(projectState, concreteIndices));
        }

        // Index-level INDEX_REFRESH_BLOCK
        {
            final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(ProjectMetadata.builder(projectId).build())
                .blocks(ClusterBlocks.builder().addIndexBlock(projectId, "index2", IndexMetadata.INDEX_REFRESH_BLOCK))
                .build();
            final ProjectState projectState = clusterState.projectState(projectId);
            String[] result = checkBlocksAndFilterIndices(projectState, concreteIndices);
            assertArrayEquals(new String[] { "index1", "index3" }, result);
        }
    }
}
