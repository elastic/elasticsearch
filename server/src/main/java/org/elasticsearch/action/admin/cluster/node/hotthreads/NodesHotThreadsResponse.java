/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.LeakTracker;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class NodesHotThreadsResponse extends BaseNodesResponse<NodeHotThreads> {

    @SuppressWarnings("this-escape")
    private final RefCounted refs = LeakTracker.wrap(
        AbstractRefCounted.of(() -> Releasables.wrap(Iterators.map(getNodes().iterator(), n -> n::decRef)).close())
    );

    @SuppressWarnings("this-escape")
    public NodesHotThreadsResponse(ClusterName clusterName, List<NodeHotThreads> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        for (NodeHotThreads nodeHotThreads : getNodes()) {
            nodeHotThreads.mustIncRef();
        }
    }

    public Iterator<CheckedConsumer<java.io.Writer, IOException>> getTextChunks() {
        return Iterators.flatMap(
            getNodes().iterator(),
            node -> Iterators.concat(
                Iterators.single(writer -> writer.append("::: ").append(node.getNode().toString()).append('\n')),
                Iterators.map(
                    new LinesIterator(node.getHotThreadsReader()),
                    line -> writer -> writer.append("   ").append(line).append('\n')
                ),
                Iterators.single(writer -> {
                    assert hasReferences();
                    writer.append('\n');
                })
            )
        );
    }

    @Override
    protected List<NodeHotThreads> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeHotThreads> nodes) throws IOException {
        TransportAction.localOnly();
    }

    private static class LinesIterator implements Iterator<String> {
        final BufferedReader reader;
        String nextLine;

        private LinesIterator(java.io.Reader reader) {
            this.reader = new BufferedReader(reader);
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

    @Override
    public void incRef() {
        refs.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refs.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refs.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refs.hasReferences();
    }
}
