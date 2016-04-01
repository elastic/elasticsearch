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

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MainActionTests extends ESTestCase {

    public void testMainResponseSerialization() throws IOException {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final boolean available = randomBoolean();
        final Version version = Version.CURRENT;
        final Build build = Build.CURRENT;

        final MainResponse mainResponse = new MainResponse(nodeName, version, clusterName, build, available);
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        mainResponse.writeTo(streamOutput);
        final MainResponse serialized = new MainResponse();
        serialized.readFrom(new ByteBufferStreamInput(ByteBuffer.wrap(streamOutput.bytes().toBytes())));

        assertThat(serialized.getNodeName(), equalTo(nodeName));
        assertThat(serialized.getClusterName(), equalTo(clusterName));
        assertThat(serialized.getBuild(), equalTo(build));
        assertThat(serialized.isAvailable(), equalTo(available));
        assertThat(serialized.getVersion(), equalTo(version));
    }

    public void testMainResponseXContent() throws IOException {
        final MainResponse mainResponse = new MainResponse("node1", Version.CURRENT, new ClusterName("cluster1"), Build.CURRENT, false);
        final String expected = "{\"name\":\"node1\",\"cluster_name\":\"cluster1\",\"version\":{\"number\":\"" + Version.CURRENT.toString()
            + "\",\"build_hash\":\"" + Build.CURRENT.shortHash() + "\",\"build_date\":\"" + Build.CURRENT.date() + "\"," +
            "\"build_snapshot\":" + Build.CURRENT.isSnapshot() + ",\"lucene_version\":\"" + Version.CURRENT.luceneVersion.toString() +
            "\"},\"tagline\":\"You Know, for Search\"}";

        XContentBuilder builder = XContentFactory.jsonBuilder();
        mainResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String xContent = builder.string();

        assertEquals(expected, xContent);
    }

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        final boolean available = randomBoolean();
        ClusterBlocks blocks;
        if (available) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder()
                    .addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 400", randomBoolean(), randomBoolean(),
                        RestStatus.BAD_REQUEST, ClusterBlockLevel.ALL))
                    .build();
            }
        } else {
            blocks = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 503", randomBoolean(), randomBoolean(),
                    RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL))
                .build();
        }
        ClusterState state = ClusterState.builder(clusterName).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportMainAction action = new TransportMainAction(settings, mock(ThreadPool.class), mock(TransportService.class),
            mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), clusterService, Version.CURRENT);
        AtomicReference<MainResponse> responseRef = new AtomicReference<>();
        action.doExecute(new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("unexpected error", e);
            }
        });

        assertNotNull(responseRef.get());
        assertEquals(available, responseRef.get().isAvailable());
        verify(clusterService, times(1)).state();
    }
}
