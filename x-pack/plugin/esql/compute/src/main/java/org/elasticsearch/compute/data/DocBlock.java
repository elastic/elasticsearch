/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DocBlock extends AbstractVectorBlock implements Block {
    private final DocVector vector;

    DocBlock(DocVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocVector asVector() {
        return vector;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC;
    }

    @Override
    public Block filter(int... positions) {
        return new DocBlock(asVector().filter(positions));
    }
}
