/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.List;

public class BaseNodesXContentResponseTests extends ESTestCase {
    public void testFragmentFlag() {
        final var node = DiscoveryNodeUtils.create("test");

        class TestNodeResponse extends BaseNodeResponse {
            protected TestNodeResponse(DiscoveryNode node) {
                super(node);
            }
        }

        final var fullResponse = new BaseNodesXContentResponse<>(ClusterName.DEFAULT, List.of(new TestNodeResponse(node)), List.of()) {
            @Override
            protected Iterator<? extends ToXContent> xContentChunks(ToXContent.Params outerParams) {
                return ChunkedToXContentHelper.chunk((b, p) -> b.startObject("content").endObject());
            }

            @Override
            protected List<TestNodeResponse> readNodesFrom(StreamInput in) {
                return TransportAction.localOnly();
            }

            @Override
            protected void writeNodesTo(StreamOutput out, List<TestNodeResponse> nodes) {
                TransportAction.localOnly();
            }
        };

        assertFalse(fullResponse.isFragment());

        assertEquals("""
            {
              "_nodes" : {
                "total" : 1,
                "successful" : 1,
                "failed" : 0
              },
              "cluster_name" : "elasticsearch",
              "content" : { }
            }""", Strings.toString(fullResponse, true, false));
    }
}
