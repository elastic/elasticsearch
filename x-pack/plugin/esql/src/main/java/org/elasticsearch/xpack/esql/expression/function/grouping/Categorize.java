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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.LongBlockHash;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.ReleasableIterator;
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

    @FunctionInfo(returnType = { "integer" }, description = "Categorizes text messages.")
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
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
        return DataType.INTEGER;
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

    private abstract class AbstractCategorizeBlockHash extends BlockHash {
        private final boolean outputPartial;
        protected final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

        @Override
        public Block[] getKeys() {
            if (outputPartial)  {
                // NOCOMMIT load partial
                Block state = null;
                Block keys ; // NOCOMMIT do we even need to send the keys? it's just going to be 0 to the length of state
                return new Block[] {new CompositeBlock()};
            }

            // NOCOMMIT load final
            return new Block[0];
        }

        @Override
        public final ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
            throw new UnsupportedOperationException();
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
    }

    private class CategorizeRawBlockHash extends AbstractCategorizeBlockHash {
        private final CategorizeEvaluator evaluator;

        @Override
        public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
            addInput.add(0, evaluator.eval(page));
        }

    }

    private class CategorizedIntermediateBlockHash extends AbstractCategorizeBlockHash {
        private final LongBlockHash hash;
        private final int channel;

        public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
            CompositeBlock block = page.getBlock(channel);
            BytesRefBlock groupingState = block.getBlock(0);
            BytesRefBlock groups = block.getBlock(0);
            Map<Integer, Integer> idMap;
            if (groupingState.areAllValuesNull() == false) {
                idMap = readIntermediate(groupingState.getBytesRef(0, new BytesRef()));
            } else {
                idMap = Collections.emptyMap();
            }
            try (IntBlock.Builder newIds = blockFactory.newIntBlockBuilder(groups.getTotalValueCount())) {
                for (int i = 0; i < groups.getTotalValueCount(); i++) {
                    newIds.appendInt(idMap.get(i));
                }
                addInput.add(page, hash.add(newIds.build()));
            }
        }

        private Map<Integer, Integer> readIntermediate(BytesRef bytes) {
            Map<Integer, Integer> idMap = new HashMap<>();
            try (StreamInput in = new BytesArray(bytes).streamInput()) {
                int count = in.readVInt();
                for (int oldCategoryId = 0; oldCategoryId < count; oldCategoryId++) {
                    SerializableTokenListCategory category = new SerializableTokenListCategory(in);
                    int newCategoryId = categorizer.mergeWireCategory(category).getId();
                    System.err.println("category id map: " + oldCategoryId + " -> " + newCategoryId + " (" + category.getRegex() + ")");
                    idMap.put(oldCategoryId, newCategoryId);
                }
                return idMap;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public IntVector nonEmpty() {
            return hash.nonEmpty();
        }

        @Override
        public BitArray seenGroupIds(BigArrays bigArrays) {
            return hash.seenGroupIds(bigArrays);
        }

        @Override
        public void close() {

        }
    }
}
