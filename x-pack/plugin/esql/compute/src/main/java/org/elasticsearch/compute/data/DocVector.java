/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

public class DocVector extends AbstractVector implements Vector {
    private final IntVector shards;
    private final IntVector segments;
    private final IntVector docs;

    public DocVector(IntVector shards, IntVector segments, IntVector docs) {
        super(shards.getPositionCount());
        this.shards = shards;
        this.segments = segments;
        this.docs = docs;
        if (shards.getPositionCount() != segments.getPositionCount()) {
            throw new IllegalArgumentException(
                "invalid position count [" + shards.getPositionCount() + " != " + segments.getPositionCount() + "]"
            );
        }
        if (shards.getPositionCount() != docs.getPositionCount()) {
            throw new IllegalArgumentException(
                "invalid position count [" + shards.getPositionCount() + " != " + docs.getPositionCount() + "]"
            );
        }
    }

    public IntVector shards() {
        return shards;
    }

    public IntVector segments() {
        return segments;
    }

    public IntVector docs() {
        return docs;
    }

    @Override
    public DocBlock asBlock() {
        return new DocBlock(this);
    }

    @Override
    public DocVector filter(int... positions) {
        return new DocVector(shards.filter(positions), segments.filter(positions), docs.filter(positions));
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC;
    }

    @Override
    public boolean isConstant() {
        return shards.isConstant() && segments.isConstant() && docs.isConstant();
    }
}
