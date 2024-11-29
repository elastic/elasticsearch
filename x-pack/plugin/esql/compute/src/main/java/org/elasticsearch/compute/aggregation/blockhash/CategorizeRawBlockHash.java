/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.List;

/**
 * BlockHash implementation for {@code Categorize} grouping function.
 * <p>
 *     This implementation expects rows, and can't deserialize intermediate states coming from other nodes.
 * </p>
 */
public class CategorizeRawBlockHash extends AbstractCategorizeBlockHash {
    private static final CategorizationAnalyzerConfig ANALYZER_CONFIG = CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(
        List.of()
    );

    private final CategorizeEvaluator evaluator;

    CategorizeRawBlockHash(int channel, BlockFactory blockFactory, boolean outputPartial, AnalysisRegistry analysisRegistry) {
        super(blockFactory, channel, outputPartial);

        CategorizationAnalyzer analyzer;
        try {
            analyzer = new CategorizationAnalyzer(analysisRegistry, ANALYZER_CONFIG);
        } catch (IOException e) {
            categorizer.close();
            throw new RuntimeException(e);
        }

        this.evaluator = new CategorizeEvaluator(analyzer, categorizer, blockFactory);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        try (IntBlock result = (IntBlock) evaluator.eval(page.getBlock(channel()))) {
            addInput.add(0, result);
        }
    }

    @Override
    public void close() {
        evaluator.close();
    }

    /**
     * Similar implementation to an Evaluator.
     */
    public final class CategorizeEvaluator implements Releasable {
        private final CategorizationAnalyzer analyzer;

        private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

        private final BlockFactory blockFactory;

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
            IntVector vector = eval(vBlock.getPositionCount(), vVector);
            return vector.asBlock();
        }

        public IntBlock eval(int positionCount, BytesRefBlock vBlock) {
            try (IntBlock.Builder result = blockFactory.newIntBlockBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    if (vBlock.isNull(p)) {
                        seenNull = true;
                        result.appendInt(NULL_ORD);
                        continue;
                    }
                    int first = vBlock.getFirstValueIndex(p);
                    int count = vBlock.getValueCount(p);
                    if (count == 1) {
                        result.appendInt(process(vBlock.getBytesRef(first, vScratch)));
                        continue;
                    }
                    int end = first + count;
                    result.beginPositionEntry();
                    for (int i = first; i < end; i++) {
                        result.appendInt(process(vBlock.getBytesRef(i, vScratch)));
                    }
                    result.endPositionEntry();
                }
                return result.build();
            }
        }

        public IntVector eval(int positionCount, BytesRefVector vVector) {
            try (IntVector.FixedBuilder result = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    result.appendInt(p, process(vVector.getBytesRef(p, vScratch)));
                }
                return result.build();
            }
        }

        private int process(BytesRef v) {
            var category = categorizer.computeCategory(v.utf8ToString(), analyzer);
            if (category == null) {
                seenNull = true;
                return NULL_ORD;
            }
            return category.getId() + 1;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(analyzer, categorizer);
        }
    }
}
