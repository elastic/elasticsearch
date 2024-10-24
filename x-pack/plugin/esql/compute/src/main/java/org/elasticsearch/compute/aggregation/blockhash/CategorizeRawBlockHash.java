/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;

public class CategorizeRawBlockHash extends AbstractCategorizeBlockHash {
    private final CategorizeEvaluator evaluator;

    CategorizeRawBlockHash(
        BlockFactory blockFactory,
        int channel,
        boolean outputPartial,
        CategorizationAnalyzer analyzer,
        TokenListCategorizer.CloseableTokenListCategorizer categorizer
    ) {
        super(blockFactory, channel, outputPartial, categorizer);
        this.evaluator = new CategorizeEvaluator(analyzer, categorizer, blockFactory);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        IntBlock result = (IntBlock) evaluator.eval(page.getBlock(channel()));
        addInput.add(0, result);
    }

    @Override
    public IntVector nonEmpty() {
        // TODO
        return null;
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        // TODO
        return null;
    }

    @Override
    public void close() {
        evaluator.close();
    }

    /**
     * NOCOMMIT: Super-duper copy-pasted from the actually generated evaluator; needs cleanup.
     */
    public static final class CategorizeEvaluator implements Releasable {
        private final CategorizationAnalyzer analyzer;

        private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

        private final BlockFactory blockFactory;

        static int process(
            BytesRef v,
            @Fixed(includeInToString = false, build = true) CategorizationAnalyzer analyzer,
            @Fixed(includeInToString = false, build = true) TokenListCategorizer.CloseableTokenListCategorizer categorizer
        ) {
            String s = v.utf8ToString();
            try (TokenStream ts = analyzer.tokenStream("text", s)) {
                return categorizer.computeCategory(ts, s.length(), 1).getId();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public CategorizeEvaluator(
            CategorizationAnalyzer analyzer,
            TokenListCategorizer.CloseableTokenListCategorizer categorizer,
            BlockFactory blockFactory
        ) {
            this.analyzer = analyzer;
            this.categorizer = categorizer;
            this.blockFactory = blockFactory;
        }

        public Block eval(BytesRefBlock vBlock) {
            BytesRefVector vVector = vBlock.asVector();
            if (vVector == null) {
                return eval(vBlock.getPositionCount(), vBlock);
            }
            try (IntVector vector = eval(vBlock.getPositionCount(), vVector)) {
                return vector.asBlock();
            }
        }

        public IntBlock eval(int positionCount, BytesRefBlock vBlock) {
            try (IntBlock.Builder result = blockFactory.newIntBlockBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                position: for (int p = 0; p < positionCount; p++) {
                    if (vBlock.isNull(p)) {
                        result.appendNull();
                        continue position;
                    }
                    if (vBlock.getValueCount(p) != 1) {
                        if (vBlock.getValueCount(p) > 1) {
                            // TODO: handle multi-values
                        }
                        result.appendNull();
                        continue position;
                    }
                    result.appendInt(process(vBlock.getBytesRef(vBlock.getFirstValueIndex(p), vScratch), this.analyzer, this.categorizer));
                }
                return result.build();
            }
        }

        public IntVector eval(int positionCount, BytesRefVector vVector) {
            try (IntVector.FixedBuilder result = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                position: for (int p = 0; p < positionCount; p++) {
                    result.appendInt(p, process(vVector.getBytesRef(p, vScratch), this.analyzer, this.categorizer));
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "CategorizeEvaluator";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(analyzer, categorizer);
        }
    }
}
