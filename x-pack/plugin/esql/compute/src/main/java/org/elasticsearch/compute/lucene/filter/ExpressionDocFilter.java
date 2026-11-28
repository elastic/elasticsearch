/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

/**
 * An ordinary {@link ExpressionEvaluator} used as a node of the tree. It loads only the columns it reads, and only for
 * the docs still in play, evaluates them as one page, and keeps the docs whose result is true. A false or null result
 * rejects the doc. Closing the node closes the evaluator.
 */
public final class ExpressionDocFilter implements DocFilter {
    private final ExpressionEvaluator evaluator;
    private final List<String> fields;
    private final DocColumns columns;

    public ExpressionDocFilter(ExpressionEvaluator evaluator, List<String> fields, DocColumns columns) {
        this.evaluator = evaluator;
        this.fields = List.copyOf(fields);
        this.columns = columns;
    }

    @Override
    public void filter(DocWindow window, FixedBitSet inPlay) throws IOException {
        int count = inPlay.cardinality();
        if (count == 0) {
            return;
        }
        int[] docs = new int[count];
        int next = 0;
        for (int bit = inPlay.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bit + 1 < inPlay.length()
            ? inPlay.nextSetBit(bit + 1)
            : DocIdSetIterator.NO_MORE_DOCS) {
            docs[next++] = window.from() + bit;
        }
        Block[] blocks = new Block[fields.size()];
        boolean loaded = false;
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = columns.load(fields.get(i), window.leaf(), docs);
            }
            loaded = true;
        } finally {
            if (loaded == false) {
                Releasables.closeWhileHandlingException(blocks);
            }
        }
        try (Page page = new Page(count, blocks); BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
            for (int i = 0; i < count; i++) {
                if (result.isNull(i) || result.getBoolean(result.getFirstValueIndex(i)) == false) {
                    inPlay.clear(docs[i] - window.from());
                }
            }
        }
    }

    @Override
    public void close() {
        evaluator.close();
    }
}
