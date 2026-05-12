/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Encodes local {@code _doc} references into transport-safe remote fetch handles.
 */
public final class RemoteFetchHandleOperator extends AbstractPageMappingOperator {
    public record Factory(String nodeId, String sessionId, int docChannel) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RemoteFetchHandleOperator(driverContext, nodeId, sessionId, docChannel);
        }

        @Override
        public String describe() {
            return "RemoteFetchHandleOperator[channel=" + docChannel + "]";
        }
    }

    private final DriverContext driverContext;
    private final String nodeId;
    private final String sessionId;
    private final int docChannel;

    RemoteFetchHandleOperator(DriverContext driverContext, String nodeId, String sessionId, int docChannel) {
        this.driverContext = driverContext;
        this.nodeId = nodeId;
        this.sessionId = sessionId;
        this.docChannel = docChannel;
    }

    @Override
    protected Page process(Page page) {
        Block[] blocks = new Block[page.getBlockCount()];
        boolean success = false;
        try (BytesRefBlock.Builder handleBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(page.getPositionCount())) {
            DocVector docVector = ((DocBlock) page.getBlock(docChannel)).asVector();
            BytesRef scratch = new BytesRef();
            for (int position = 0; position < page.getPositionCount(); position++) {
                handleBuilder.appendBytesRef(
                    new RemoteFetchHandle(
                        nodeId,
                        sessionId,
                        docVector.shards().getInt(position),
                        docVector.segments().getInt(position),
                        docVector.docs().getInt(position)
                    ).toBytesRef()
                );
            }
            for (int block = 0; block < blocks.length; block++) {
                if (block == docChannel) {
                    blocks[block] = handleBuilder.build();
                } else {
                    blocks[block] = page.getBlock(block);
                    blocks[block].incRef();
                }
            }
            Page output = new Page(page.getPositionCount(), blocks);
            success = true;
            return output;
        } finally {
            page.releaseBlocks();
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(blocks)));
            }
        }
    }

    @Override
    public String toString() {
        return "RemoteFetchHandleOperator[channel=" + docChannel + "]";
    }
}
