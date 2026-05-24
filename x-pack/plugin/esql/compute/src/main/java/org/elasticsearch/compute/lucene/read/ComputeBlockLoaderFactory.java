/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;

class ComputeBlockLoaderFactory extends DelegatingBlockLoaderFactory implements Releasable {
    private Block nullBlock;

    ComputeBlockLoaderFactory(BlockFactory factory) {
        super(factory);
    }

    @Override
    public Block constantNulls(int count) {
        if (nullBlock == null) {
            nullBlock = factory.newConstantNullBlock(count);
        } else {
            if (nullBlock.getPositionCount() != count) {
                nullBlock.close();
                nullBlock = factory.newConstantNullBlock(count);
            }
        }
        nullBlock.incRef();
        return nullBlock;
    }

    @Override
    public void close() {
        if (nullBlock != null) {
            nullBlock.close();
        }
    }
}
