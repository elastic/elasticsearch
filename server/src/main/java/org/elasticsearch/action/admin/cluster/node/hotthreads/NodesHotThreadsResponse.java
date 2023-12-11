/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.CheckedConsumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class NodesHotThreadsResponse extends BaseNodesResponse<NodeHotThreads> {

    public NodesHotThreadsResponse(ClusterName clusterName, List<NodeHotThreads> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public Iterator<CheckedConsumer<java.io.Writer, IOException>> getTextChunks() {
        return Iterators.flatMap(
            getNodes().iterator(),
            node -> Iterators.concat(
                Iterators.single(writer -> writer.append("::: ").append(node.getNode().toString()).append('\n')),
                Iterators.map(new LinesIterator(node.getHotThreads()), line -> writer -> writer.append("   ").append(line).append('\n')),
                Iterators.single(writer -> writer.append('\n'))
            )
        );
    }

    @Override
    protected List<NodeHotThreads> readNodesFrom(StreamInput in) throws IOException {
        return in.readCollectionAsList(NodeHotThreads::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeHotThreads> nodes) throws IOException {
        TransportAction.localOnly();
    }

    private static class LinesIterator implements Iterator<String> {
        final BufferedReader reader;
        String nextLine;

        private LinesIterator(String input) {
            reader = new BufferedReader(new StringReader(Objects.requireNonNull(input)));
            advance();
        }

        private void advance() {
            try {
                nextLine = reader.readLine();
            } catch (IOException e) {
                assert false : e; // no actual IO happens here
            }
        }

        @Override
        public boolean hasNext() {
            return nextLine != null;
        }

        @Override
        public String next() {
            if (nextLine == null) {
                throw new NoSuchElementException();
            }
            try {
                return nextLine;
            } finally {
                advance();
            }
        }
    }
}
