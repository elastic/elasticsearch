/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;

/**
 * A stateful page-mapping operator that evaluates {@code CATEGORIZE(field)} for use in
 * {@code LIMIT BY} and {@code TOPN BY}. Unlike
 * {@link org.elasticsearch.compute.aggregation.blockhash.CategorizeBlockHash}, this operator
 * does NOT unroll multivalued fields. A position with N text values produces a multivalued
 * {@code IntBlock} with N ordered category IDs, so that {@code [a, b]} and {@code [b, a]}
 * map to different groups.
 *
 * The category 0 is reserved for null fields. Any non-null field would produce a positive integer.
 *
 * <p>The operator appends the new {@code IntBlock} as an extra channel on each output page.
 */
public class CategorizeEvalOperator extends AbstractPageMappingOperator {

    public static final class Factory implements Operator.OperatorFactory {
        private final int textChannel;
        private final CategorizeDef categorizeDef;
        private final AnalysisRegistry analysisRegistry;
        /**
         * Optional single-element array; when non-null, the created operator is stored at index 0
         * so that a downstream {@link CategorizeStateEmitOperator} can read the categorizer state.
         */
        private final CategorizeEvalOperator[] captureInto;

        public Factory(int textChannel, CategorizeDef categorizeDef, AnalysisRegistry analysisRegistry) {
            this(textChannel, categorizeDef, analysisRegistry, null);
        }

        public Factory(
            int textChannel,
            CategorizeDef categorizeDef,
            AnalysisRegistry analysisRegistry,
            CategorizeEvalOperator[] captureInto
        ) {
            this.textChannel = textChannel;
            this.categorizeDef = categorizeDef;
            this.analysisRegistry = analysisRegistry;
            this.captureInto = captureInto;
        }

        @Override
        public CategorizeEvalOperator get(DriverContext driverContext) {
            CategorizeEvalOperator op = new CategorizeEvalOperator(
                textChannel,
                categorizeDef,
                analysisRegistry,
                driverContext.blockFactory()
            );
            if (captureInto != null) {
                captureInto[0] = op;
            }
            return op;
        }

        @Override
        public String describe() {
            return "CategorizeEvalOperator[channel=" + textChannel + "]";
        }
    }

    private static final CategorizationAnalyzerConfig DEFAULT_ANALYZER_CONFIG = CategorizationAnalyzerConfig
        .buildStandardEsqlCategorizationAnalyzer();

    /**
     * Ordinal reserved for null values and strings that produce no tokens after analysis
     * (empty strings, pure numbers, stop-words, etc.).
     */
    private static final int NULL_ORD = 0;

    private final int textChannel;
    private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;
    private final CategorizationAnalyzer analyzer;
    private final BlockFactory blockFactory;

    private CategorizeEvalOperator(
        int textChannel,
        CategorizeDef categorizeDef,
        AnalysisRegistry analysisRegistry,
        BlockFactory blockFactory
    ) {
        this.textChannel = textChannel;
        this.blockFactory = blockFactory;
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
    protected Page process(Page page) {
        IntBlock categorized = categorize(page.getBlock(textChannel));
        boolean success = false;
        try {
            Page result = page.appendBlock(categorized);
            success = true;
            return result;
        } finally {
            if (success == false) {
                categorized.close();
            }
        }
    }

    private IntBlock categorize(BytesRefBlock vBlock) {
        BytesRefVector vVector = vBlock.asVector();
        if (vVector != null) {
            try (IntVector.FixedBuilder result = blockFactory.newIntVectorFixedBuilder(vBlock.getPositionCount())) {
                BytesRef scratch = new BytesRef();
                for (int p = 0; p < vBlock.getPositionCount(); p++) {
                    result.appendInt(p, process(vVector.getBytesRef(p, scratch)));
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
                    result.appendInt(process(vBlock.getBytesRef(first, scratch)));
                    continue;
                }
                result.beginPositionEntry();
                for (int i = first; i < first + count; i++) {
                    result.appendInt(process(vBlock.getBytesRef(i, scratch)));
                }
                result.endPositionEntry();
            }
            return result.build();
        }
    }

    private int process(BytesRef v) {
        var category = categorizer.computeCategory(v.utf8ToString(), analyzer);
        if (category == null) {
            return NULL_ORD;
        }
        return category.getId() + 1;
    }

    /**
     * Serializes the current categorizer state as a {@link BytesRef}.
     * Wire format: {@code writeBoolean(false)} (seenNull always false here) +
     * {@code writeVInt(count)} + each {@link SerializableTokenListCategory#writeTo}.
     * Mirrors {@code CategorizeBlockHash.serializeCategorizer()}.
     */
    BytesRef serializeCategorizer() {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeBoolean(false); // seenNull: CategorizeEvalOperator uses NULL_ORD=0, not a separate null flag
            int count = categorizer.getCategoryCount();
            out.writeVInt(count);
            for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                category.writeTo(out);
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "CategorizeEvalOperator[channel=" + textChannel + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, categorizer, analyzer);
    }
}
