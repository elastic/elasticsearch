/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;

class ComputeBlockLoaderFactory extends DelegatingBlockLoaderFactory implements Releasable {
    private final int pageSize;
    private Block nullBlock;

    ComputeBlockLoaderFactory(BlockFactory factory, int pageSize) {
        super(factory);
        this.pageSize = pageSize;
    }

    @Override
    public Block constantNulls() {
        if (nullBlock == null) {
            nullBlock = factory.newConstantNullBlock(pageSize);
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

    @Override
    public BytesRefBlock constantBytes(BytesRef value) {
        return factory.newConstantBytesRefBlockWith(value, pageSize);
    }
}
