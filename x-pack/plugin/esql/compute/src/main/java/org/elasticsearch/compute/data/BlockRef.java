/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

public class BlockRef implements Releasable {

    private static class RefCount implements RefCounted {
        private int i = 1;

        @Override
        public void incRef() {
            i++;
        }

        @Override
        public boolean tryIncRef() {
            if (i <= 0) {
                return false;
            }
            incRef();
            return true;
        }

        @Override
        public boolean decRef() {
            i--;

            return i <= 0;
        }

        @Override
        public boolean hasReferences() {
            return i >= 1;
        }
    }

    private final Block block;
    private final RefCount refs;
    private boolean isReleased;

    public BlockRef(Block block) {
        this(block, new RefCount());
    }

    private BlockRef(Block block, RefCount refs) {
        this.block = block;
        this.refs = refs;
        isReleased = false;
    }

    public <B extends Block> B get() {
        if (isReleased) {
            throw new IllegalStateException("cannot get block from released reference");
        }
        assert block.isReleased() == false;

        @SuppressWarnings("unchecked")
        B castBlock = (B) block;

        return castBlock;
    }

    public BlockRef shallowCopy() {
        if (refs.tryIncRef() == false) {
            throw new IllegalStateException("cannot copy already closed reference");
        }

        return new BlockRef(block, refs);
    }

    @Override
    public String toString() {
        return "BlockRef{" + "block=" + block.toString() + '}';
    }

    @Override
    public void close() {
        if (isReleased) {
            throw new IllegalStateException("cannot release block reference twice");
        }
        isReleased = true;
        if (refs.decRef()) {
            block.close();
        }
    }
}
