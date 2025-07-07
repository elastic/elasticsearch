/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class ValuesReader implements ReleasableIterator<Block[]> {
    protected final ValuesSourceReaderOperator operator;
    protected final DocVector docs;
    private int offset;

    ValuesReader(ValuesSourceReaderOperator operator, DocVector docs) {
        this.operator = operator;
        this.docs = docs;
    }

    @Override
    public boolean hasNext() {
        return offset < docs.getPositionCount();
    }

    @Override
    public Block[] next() {
        Block[] target = new Block[operator.fields.length];
        boolean success = false;
        try {
            load(target, offset);
            success = true;
            for (Block b : target) {
                operator.valuesLoaded += b.getTotalValueCount();
            }
            offset += target[0].getPositionCount();
            return target;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(target);
            }
        }
    }

    protected abstract void load(Block[] target, int offset) throws IOException;

    @Override
    public void close() {}
}
