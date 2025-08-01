/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;

import java.io.IOException;

public class HighlighterExpressionEvaluator extends LuceneQueryEvaluator<BytesRefVector.Builder>
    implements
        EvalOperator.ExpressionEvaluator {

    HighlighterExpressionEvaluator(BlockFactory blockFactory, ShardConfig[] shardConfigs) {
        super(blockFactory, shardConfigs);
    }

    @Override
    protected ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected Vector createNoMatchVector(BlockFactory blockFactory, int size) {
        return blockFactory.newConstantBytesRefVector(new BytesRef(), size);
    }

    @Override
    protected BytesRefVector.Builder createVectorBuilder(BlockFactory blockFactory, int size) {
        return blockFactory.newBytesRefVectorBuilder(size);
    }

    @Override
    protected void appendMatch(BytesRefVector.Builder builder, Scorable scorer) throws IOException {
        // TODO: add snippets here
        builder.appendBytesRef(new BytesRef("I am a snippet")); // Placeholder for actual highlighted text
    }

    @Override
    protected void appendNoMatch(BytesRefVector.Builder builder) {
        // NOTE: Carlos originally suggested that we add null here, but that doesn't work - errors on missing key
        builder.appendBytesRef(new BytesRef());
    }

    @Override
    public Block eval(Page page) {
        return executeQuery(page);
    }

    public record Factory(ShardConfig[] shardConfigs) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            // TODO: Is it possible to add the highlight query here, rather than in ExtractSnippets? Would require ShardConfig having access
            // to context
            return new HighlighterExpressionEvaluator(context.blockFactory(), shardConfigs);
        }
    }
}
