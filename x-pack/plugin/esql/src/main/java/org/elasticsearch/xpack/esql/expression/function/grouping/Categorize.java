/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingKey;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Categorizes text messages.
 *
 * This implementation is incomplete and comes with the following caveats:
 * - it only works correctly on a single node.
 * - when running on multiple nodes, category IDs of the different nodes are
 *   aggregated, even though the same ID can correspond to a totally different
 *   category
 * - the output consists of category IDs, which should be replaced by category
 *   regexes or keys
 *
 * TODO(jan, nik): fix this
 */
public class Categorize extends GroupingFunction implements Validatable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Categorize",
        Categorize::new
    );

    private final Expression field;

    @FunctionInfo(returnType = { "integer" }, description = "Categorizes text messages")
    public Categorize(
        Source source,
        @Param(name = "field", type = { "text", "keyword" }, description = "Expression to categorize") Expression field
    ) {
        super(source, List.of(field));
        this.field = field;
    }

    private Categorize(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Evaluator
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

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return new CategorizeEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            context -> new CategorizationAnalyzer(
                // TODO(jan): get the correct analyzer in here, see CategorizationAnalyzerConfig::buildStandardCategorizationAnalyzer
                new CustomAnalyzer(
                    TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                    new CharFilterFactory[0],
                    new TokenFilterFactory[0]
                ),
                true
            ),
            context -> new TokenListCategorizer.CloseableTokenListCategorizer(
                new CategorizationBytesRefHash(new BytesRefHash(2048, context.bigArrays())),
                CategorizationPartOfSpeechDictionary.getInstance(),
                0.70f
            )
        );
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Categorize(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Categorize::new, field);
    }

    public Expression field() {
        return field;
    }

    @Override
    public String toString() {
        return "Categorize{field=" + field + "}";
    }

    public GroupingKey.Supplier groupingKey(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return mode -> new GroupingKeyFactory(source(), toEvaluator.apply(field), mode);
    }

    record GroupingKeyFactory(Source source, ExpressionEvaluator.Factory field, AggregatorMode mode) implements GroupingKey.Factory {
        @Override
        public GroupingKey apply(DriverContext context, int resultOffset) {
            ExpressionEvaluator field = this.field.get(context);
            CategorizeEvaluator evaluator = null;
            TokenListCategorizer.CloseableTokenListCategorizer categorizer = null;
            try {
                categorizer = new TokenListCategorizer.CloseableTokenListCategorizer(
                    new CategorizationBytesRefHash(new BytesRefHash(2048, context.bigArrays())),
                    CategorizationPartOfSpeechDictionary.getInstance(),
                    0.70f
                );
                evaluator = new CategorizeEvaluator(
                    source,
                    field,
                    new CategorizationAnalyzer(
                        // TODO(jan): get the correct analyzer in here, see
                        // CategorizationAnalyzerConfig::buildStandardCategorizationAnalyzer
                        new CustomAnalyzer(
                            TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                            new CharFilterFactory[0],
                            new TokenFilterFactory[0]
                        ),
                        true
                    ),
                    categorizer,
                    context
                );
                field = null;
                GroupingKey result = new GroupingKey(
                    mode,
                    new GroupingKeyThing(resultOffset, evaluator, categorizer),
                    context.blockFactory()
                );
                categorizer = null;
                evaluator = null;
                return result;
            } finally {
                Releasables.close(field, evaluator, categorizer);
            }

        }

        @Override
        public ElementType intermediateElementType() {
            return ElementType.INT;
        }

        @Override
        public GroupingAggregator.Factory valuesAggregatorForGroupingsInTimeSeries(int timeBucketChannel) {
            throw new UnsupportedOperationException("not supported in time series");
        }
    }

    private static class GroupingKeyThing implements GroupingKey.Thing {
        private final int resultOffset;
        private final CategorizeEvaluator evaluator;
        private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

        private GroupingKeyThing(
            int resultOffset,
            CategorizeEvaluator evaluator,
            TokenListCategorizer.CloseableTokenListCategorizer categorizer
        ) {
            this.resultOffset = resultOffset;
            this.evaluator = evaluator;
            this.categorizer = categorizer;
        }

        @Override
        public Block evalRawInput(Page page) {
            return evaluator.eval(page);
        }

        @Override
        public int extraIntermediateBlocks() {
            return 1;
        }

        @Override
        public void fetchIntermediateState(BlockFactory blockFactory, Block[] blocks, int positionCount) {
            blocks[resultOffset + 1] = buildIntermediateBlock(blockFactory, positionCount);
        }

        @Override
        public Block evalIntermediateInput(BlockFactory blockFactory, Page page) {
            BytesRefBlock intermediate = page.getBlock(resultOffset + 1);
            Map<Integer, Integer> idMap;
            if (intermediate.areAllValuesNull() == false) {
                idMap = readIntermediate(intermediate.getBytesRef(0, new BytesRef()));
            } else {
                idMap = Collections.emptyMap();
            }
            IntBlock oldIds = page.getBlock(resultOffset);
            try (IntBlock.Builder newIds = blockFactory.newIntBlockBuilder(oldIds.getTotalValueCount())) {
                for (int i = 0; i < oldIds.getTotalValueCount(); i++) {
                    newIds.appendInt(idMap.get(i));
                }
                return newIds.build();
            }
        }

        @Override
        public void replaceIntermediateKeys(BlockFactory blockFactory, Block[] blocks) {
            // NOCOMMIT this offset can't be the same in the result array and intermediate array
            IntBlock keys = (IntBlock) blocks[resultOffset];
            blocks[resultOffset] = finalKeys(blockFactory, keys);
            System.err.println(blocks[resultOffset]);
        }

        @Override
        public void close() {
            evaluator.close();
        }

        private Block buildIntermediateBlock(BlockFactory blockFactory, int positionCount) {
            if (categorizer.getCategoryCount() == 0) {
                return blockFactory.newConstantNullBlock(positionCount);
            }
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                // TODO be more careful here.
                out.writeVInt(categorizer.getCategoryCount());
                for (SerializableTokenListCategory category : categorizer.toCategories(categorizer.getCategoryCount())) {
                    category.writeTo(out);
                }
                return blockFactory.newConstantBytesRefBlockWith(out.bytes().toBytesRef(), positionCount);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private Map<Integer, Integer> readIntermediate(BytesRef bytes) {
            Map<Integer, Integer> idMap = new HashMap<>();
            try (StreamInput in = new BytesArray(bytes).streamInput()) {
                int count = in.readVInt();
                for (int oldCategoryId = 0; oldCategoryId < count; oldCategoryId++) {
                    SerializableTokenListCategory category = new SerializableTokenListCategory(in);
                    int newCategoryId = categorizer.mergeWireCategory(category).getId();
                    System.err.println("category id map: " + oldCategoryId + " -> " + newCategoryId);
                    idMap.put(oldCategoryId, newCategoryId);
                }
                return idMap;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private OrdinalBytesRefBlock finalKeys(BlockFactory blockFactory, IntBlock keys) {
            return new OrdinalBytesRefBlock(keys, finalBytes(blockFactory));
        }

        /**
         * A lookup table containing the category names.
         */
        private BytesRefVector finalBytes(BlockFactory blockFactory) {
            try (BytesRefVector.Builder result = blockFactory.newBytesRefVectorBuilder(categorizer.getCategoryCount())) {
                BytesRefBuilder scratch = new BytesRefBuilder();
                for (SerializableTokenListCategory category : categorizer.toCategories(categorizer.getCategoryCount())) {
                    scratch.copyChars(category.getRegex());
                    result.appendBytesRef(scratch.get());
                    scratch.clear();
                }
                return result.build();
            }
        }
    }
}
