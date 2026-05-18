/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.hotthreads;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.rest.RestResponseUtils.getTextBodyContent;

public class NodesHotThreadsResponseTests extends ESTestCase {
    public void testGetTextChunks() {
        final var node0 = DiscoveryNodeUtils.create("node-0");
        final var node1 = DiscoveryNodeUtils.create("node-1");
        final var response = new NodesHotThreadsResponse(
            ClusterName.DEFAULT,
            List.of(

                new NodeHotThreads(node0, ReleasableBytesReference.wrap(new BytesArray("""
                    node 0 line 1
                    node 0 line 2"""))),

                new NodeHotThreads(node1, ReleasableBytesReference.wrap(new BytesArray("""
                    node 1 line 1
                    node 1 line 2""")))
            ),
            List.of()
        );
        try {
            assertEquals(Strings.format("""
                ::: %s
                   node 0 line 1
                   node 0 line 2

                ::: %s
                   node 1 line 1
                   node 1 line 2

                """, node0, node1), getTextBodyContent(response.getTextChunks()));
        } finally {
            response.decRef();
        }
    }
}
