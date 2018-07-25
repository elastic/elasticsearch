/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.main;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MainActionTests extends ESTestCase {

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        ClusterBlocks blocks;
        if (randomBoolean()) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder()
                    .addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 400", randomBoolean(), randomBoolean(),
                        false, RestStatus.BAD_REQUEST, ClusterBlockLevel.ALL))
                    .build();
            }
        } else {
            blocks = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 503", randomBoolean(), randomBoolean(),
                    false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL))
                .build();
        }
        ClusterState state = ClusterState.builder(clusterName).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null, TransportService
            .NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportMainAction action = new TransportMainAction(settings, transportService, mock(ActionFilters.class), clusterService);
        AtomicReference<MainResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error", e);
            }
        });

        assertNotNull(responseRef.get());
        verify(clusterService, times(1)).state();
    }
}
