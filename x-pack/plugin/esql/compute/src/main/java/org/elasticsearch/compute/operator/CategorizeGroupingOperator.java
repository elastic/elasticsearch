/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;

/**
 * Data-node operator for distributed {@code LIMIT BY CATEGORIZE} and {@code TOPN BY CATEGORIZE}.
 *
 * <p>Wraps an inner grouping operator (e.g. {@link GroupedLimitOperator} or
 * {@link org.elasticsearch.compute.operator.topn.GroupedTopNOperator}). On each call to {@link #addInput}, the text field at
 * {@code textChannel} is classified by the ML categorizer and the resulting integer category-ID
 * block is appended to the page before delegating to the inner operator.
 *
 * <p>On each call to {@link #getOutput}, the inner operator's output page is retrieved and the
 * current categorizer state is serialized and appended as a constant {@link BytesRefBlock}. This
 * per-page state allows the coordinator to merge shard models and remap category IDs without
 * buffering all output — categories are monotonically growing and {@code mergeWireCategory} is
 * idempotent, so any snapshot is valid.
 *
 * <p>When {@code isSingleNode=true} (local-only queries without an exchange), the state channel
 * is not appended; the inner operator's output is returned directly.
 */
public class CategorizeGroupingOperator implements Operator {

    public static final class Factory implements Operator.OperatorFactory {
        private final int textChannel;
        private final CategorizeDef categorizeDef;
        private final AnalysisRegistry analysisRegistry;
        private final Operator.OperatorFactory innerFactory;
        private final boolean isSingleNode;

        public Factory(
            int textChannel,
            CategorizeDef categorizeDef,
            AnalysisRegistry analysisRegistry,
            Operator.OperatorFactory innerFactory,
            boolean isSingleNode
        ) {
            this.textChannel = textChannel;
            this.categorizeDef = categorizeDef;
            this.analysisRegistry = analysisRegistry;
            this.innerFactory = innerFactory;
            this.isSingleNode = isSingleNode;
        }

        @Override
        public CategorizeGroupingOperator get(DriverContext driverContext) {
            return new CategorizeGroupingOperator(
                textChannel,
                categorizeDef,
                analysisRegistry,
                innerFactory.get(driverContext),
                isSingleNode,
                driverContext.blockFactory()
            );
        }

        @Override
        public String describe() {
            return "CategorizeGroupingOperator[channel=" + textChannel + ", inner=" + innerFactory.describe() + "]";
        }
    }

    private static final CategorizationAnalyzerConfig DEFAULT_ANALYZER_CONFIG = CategorizationAnalyzerConfig
        .buildStandardEsqlCategorizationAnalyzer();

    /** Ordinal reserved for null values and strings that produce no tokens after analysis. */
    private static final int NULL_ORD = 0;

    private final int textChannel;
    private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;
    private final CategorizationAnalyzer analyzer;
    private final Operator inner;
    private final boolean isSingleNode;
    private final BlockFactory blockFactory;

    private CategorizeGroupingOperator(
        int textChannel,
        CategorizeDef categorizeDef,
        AnalysisRegistry analysisRegistry,
        Operator inner,
        boolean isSingleNode,
        BlockFactory blockFactory
    ) {
        this.textChannel = textChannel;
        this.isSingleNode = isSingleNode;
        this.blockFactory = blockFactory;
        this.inner = inner;
        this.categorizer = new TokenListCategorizer.CloseableTokenListCategorizer(
            new CategorizationBytesRefHash(new BytesRefHash(2048, blockFactory.bigArrays())),
            CategorizationPartOfSpeechDictionary.getInstance(),
            categorizeDef.similarityThreshold() / 100.0f
        );
        try {
            CategorizationAnalyzerConfig config = categorizeDef.analyzer() == null
                ? DEFAULT_ANALYZER_CONFIG
                : new CategorizationAnalyzerConfig.Builder().setAnalyzer(categorizeDef.analyzer()).build();
            this.analyzer = new CategorizationAnalyzer(analysisRegistry, config);
        } catch (IOException e) {
            categorizer.close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean needsInput() {
        return inner.needsInput();
    }

    @Override
    public void addInput(Page page) {
        IntBlock catIds = categorize(page.getBlock(textChannel));
        boolean success = false;
        try {
            Page withCatIds = page.appendBlock(catIds);
            success = true;
            inner.addInput(withCatIds);
        } finally {
            if (success == false) {
                catIds.close();
            }
        }
    }

    @Override
    public void finish() {
        inner.finish();
    }

    @Override
    public boolean isFinished() {
        return inner.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return inner.canProduceMoreDataWithoutExtraInput();
    }

    @Override
    public Page getOutput() {
        Page p = inner.getOutput();
        if (p == null || isSingleNode) {
            return p;
        }
        BytesRefBlock stateBlock = null;
        boolean success = false;
        try {
            BytesRef state = serializeCategorizer();
            stateBlock = blockFactory.newConstantBytesRefBlockWith(state, p.getPositionCount());
            Page result = p.appendBlock(stateBlock);
            success = true;
            return result;
        } finally {
            if (success == false) {
                if (stateBlock != null) {
                    stateBlock.close();
                }
                p.releaseBlocks();
            }
        }
    }

    private IntBlock categorize(BytesRefBlock vBlock) {
        BytesRefVector vVector = vBlock.asVector();
        if (vVector != null) {
            try (IntVector.FixedBuilder result = blockFactory.newIntVectorFixedBuilder(vBlock.getPositionCount())) {
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < vBlock.getPositionCount(); p++) {
                    result.appendInt(p, computeCategory(vVector.getBytesRef(p, scratch)));
                }
                return result.build().asBlock();
            }
        }
        try (IntBlock.Builder result = blockFactory.newIntBlockBuilder(vBlock.getPositionCount())) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < vBlock.getPositionCount(); p++) {
                if (vBlock.isNull(p)) {
                    result.appendInt(NULL_ORD);
                    continue;
                }
                int first = vBlock.getFirstValueIndex(p);
                int count = vBlock.getValueCount(p);
                if (count == 1) {
                    result.appendInt(computeCategory(vBlock.getBytesRef(first, scratch)));
                    continue;
                }
                result.beginPositionEntry();
                for (int i = first; i < first + count; i++) {
                    result.appendInt(computeCategory(vBlock.getBytesRef(i, scratch)));
                }
                result.endPositionEntry();
            }
            return result.build();
        }
    }

    private int computeCategory(BytesRef v) {
        var category = categorizer.computeCategory(v.utf8ToString(), analyzer);
        if (category == null) {
            return NULL_ORD;
        }
        return category.getId() + 1;
    }

    /**
     * Serializes the current categorizer state as a {@link BytesRef}.
     * Wire format mirrors {@code CategorizeBlockHash.serializeCategorizer()}.
     */
    private BytesRef serializeCategorizer() {
        return CategorizerStateCodec.serialize(categorizer);
    }

    @Override
    public String toString() {
        return "CategorizeGroupingOperator[channel=" + textChannel + ", inner=" + inner + "]";
    }

    @Override
    public void close() {
        Releasables.close(inner, categorizer, analyzer);
    }
}
