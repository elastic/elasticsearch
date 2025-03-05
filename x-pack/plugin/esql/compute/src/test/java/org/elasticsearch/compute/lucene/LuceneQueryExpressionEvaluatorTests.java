/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.lucene.LuceneQueryEvaluator.DenseCollector;

public class LuceneQueryExpressionEvaluatorTests extends LuceneQueryEvaluatorTests<BooleanVector, BooleanVector.Builder> {

    @Override
    protected DenseCollector<BooleanVector.Builder> createDensecollector(int min, int max) {
        return new LuceneQueryEvaluator.DenseCollector<>(
            min,
            max,
            blockFactory().newBooleanVectorFixedBuilder(max - min + 1),
            b -> b.appendBoolean(false),
            (b, s) -> b.appendBoolean(true));
    }

    @Override
    protected Scorable getScorer() {
        return null;
    }

    @Override
    protected Object getValueAt(BooleanVector vector, int i) {
        return vector.getBoolean(i);
    }

    @Override
    protected Object valueForMatch() {
        return true;
    }

    @Override
    protected Object valueForNoMatch() {
        return false;
    }


}
