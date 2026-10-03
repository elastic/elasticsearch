/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The field of a runtime {@code MATCH} scored with BM25, carrying the collection statistics its stats pass computed: the document
 * count, the summed length, and the document frequency of each of the (analyzed) query {@code terms}. The statistics are columns
 * holding the same value on every row: references to the pass's {@code INLINE STATS} output, which become literals once it ran.
 * <p>
 * Evaluates to its field unchanged, so filtering is unaffected; only {@link Match#toScorer} reads the statistics.
 */
public class RuntimeBm25Field extends EsqlScalarFunction implements AnalyzedTextExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RuntimeBm25Field",
        RuntimeBm25Field::new
    );

    private static final int FIELD = 0;
    private static final int DOC_COUNT = 1;
    private static final int SUM_TOTAL_TERM_FREQ = 2;
    private static final int FIRST_DOC_FREQ = 3;

    private final List<BytesRef> terms;
    private final List<Integer> weights;

    public RuntimeBm25Field(
        Source source,
        Expression field,
        Expression docCount,
        Expression sumTotalTermFreq,
        List<? extends Expression> docFreqs,
        List<BytesRef> terms,
        List<Integer> weights
    ) {
        this(source, children(field, docCount, sumTotalTermFreq, docFreqs), terms, weights);
    }

    private RuntimeBm25Field(Source source, List<Expression> children, List<BytesRef> terms, List<Integer> weights) {
        super(source, children);
        if (children.size() - FIRST_DOC_FREQ != terms.size() || terms.size() != weights.size()) {
            throw new IllegalArgumentException("expected one document frequency and one weight per term");
        }
        this.terms = terms;
        this.weights = weights;
    }

    private RuntimeBm25Field(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readCollectionAsList(StreamInput::readBytesRef),
            in.readCollectionAsList(StreamInput::readVInt)
        );
    }

    private static List<Expression> children(Expression field, Expression docCount, Expression sum, List<? extends Expression> dfs) {
        List<Expression> children = new ArrayList<>(FIRST_DOC_FREQ + dfs.size());
        children.add(field);
        children.add(docCount);
        children.add(sum);
        children.addAll(dfs);
        return children;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
        out.writeCollection(terms, StreamOutput::writeBytesRef);
        out.writeCollection(weights, StreamOutput::writeVInt);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression field() {
        return children().get(FIELD);
    }

    @Override
    public String valuesAnalyzer() {
        return AnalyzedTextExpression.valuesAnalyzerOf(field());
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RuntimeBm25Field(source(), newChildren, terms, weights);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        List<Expression> children = children();
        return NodeInfo.create(
            this,
            RuntimeBm25Field::new,
            children.get(FIELD),
            children.get(DOC_COUNT),
            children.get(SUM_TOTAL_TERM_FREQ),
            children.subList(FIRST_DOC_FREQ, children.size()),
            terms,
            weights
        );
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return toEvaluator.apply(field());
    }

    /** Scores each row with BM25 over the carried statistics; called for the {@code MATCH} this is the field of. */
    ExpressionEvaluator.Factory toScorer(EvaluatorMapper.ToEvaluator toEvaluator) {
        List<ExpressionEvaluator.Factory> inputs = children().stream().map(toEvaluator::apply).toList();
        Analyzer analyzer = RuntimeBm25.valuesAnalyzer(field(), toEvaluator);
        Map<BytesRef, Integer> termIndex = new HashMap<>();
        for (int i = 0; i < terms.size(); i++) {
            termIndex.put(terms.get(i), i);
        }
        int[] termWeights = weights.stream().mapToInt(Integer::intValue).toArray();
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                ExpressionEvaluator[] evaluators = new ExpressionEvaluator[inputs.size()];
                for (int i = 0; i < evaluators.length; i++) {
                    evaluators[i] = inputs.get(i).get(context);
                }
                return new ScoreEvaluator(evaluators, analyzer, terms, termIndex, termWeights, context);
            }

            @Override
            public String toString() {
                return "RuntimeBm25ScoreEvaluator[inputs=" + inputs + ", terms=" + terms.size() + "]";
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        RuntimeBm25Field other = (RuntimeBm25Field) obj;
        return terms.equals(other.terms) && weights.equals(other.weights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), terms, weights);
    }

    private static final class ScoreEvaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ScoreEvaluator.class);

        private final ExpressionEvaluator[] inputs;
        private final Analyzer analyzer;
        private final List<BytesRef> terms;
        private final Map<BytesRef, Integer> termIndex;
        private final int[] weights;
        private final DriverContext context;
        private final RuntimeBm25.RowStats row;
        private final BytesRef scratch = new BytesRef();
        /** The statistics the {@link #scorers} were built for: they are the same on every page, so build the scorers once. */
        private long[] scorersStats;
        private SimScorer[] scorers;

        ScoreEvaluator(
            ExpressionEvaluator[] inputs,
            Analyzer analyzer,
            List<BytesRef> terms,
            Map<BytesRef, Integer> termIndex,
            int[] weights,
            DriverContext context
        ) {
            this.inputs = inputs;
            this.analyzer = analyzer;
            this.terms = terms;
            this.termIndex = termIndex;
            this.weights = weights;
            this.context = context;
            this.row = new RuntimeBm25.RowStats(terms.size());
        }

        @Override
        public Block eval(Page page) {
            int positionCount = page.getPositionCount();
            Block[] blocks = new Block[inputs.length];
            try {
                for (int i = 0; i < inputs.length; i++) {
                    blocks[i] = inputs[i].eval(page);
                }
                SimScorer[] pageScorers = positionCount == 0 ? null : scorers(blocks);
                BytesRefBlock values = (BytesRefBlock) blocks[FIELD];
                // Score blocks must be dense: ScoreOperator reads them positionally.
                try (DoubleBlock.Builder result = context.blockFactory().newDoubleBlockBuilder(positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        result.appendDouble(pageScorers == null ? 0.0 : score(values, p, pageScorers));
                    }
                    return result.build();
                }
            } finally {
                Releasables.closeExpectNoException(blocks);
            }
        }

        private SimScorer[] scorers(Block[] blocks) {
            long[] stats = new long[blocks.length - DOC_COUNT];
            for (int i = 0; i < stats.length; i++) {
                stats[i] = firstLong(blocks[DOC_COUNT + i]);
            }
            if (Arrays.equals(stats, scorersStats) == false) {
                long[] docFreqs = Arrays.copyOfRange(stats, FIRST_DOC_FREQ - DOC_COUNT, stats.length);
                scorers = RuntimeBm25.scorers(stats[0], stats[SUM_TOTAL_TERM_FREQ - DOC_COUNT], docFreqs, terms);
                scorersStats = stats;
            }
            return scorers;
        }

        /** A statistic's value, the same on every position; {@code null} (no row counted) reads as 0. */
        private static long firstLong(Block block) {
            if (block.isNull(0)) {
                return 0;
            }
            return ((LongBlock) block).getLong(block.getFirstValueIndex(0));
        }

        private double score(BytesRefBlock values, int position, SimScorer[] pageScorers) {
            if (values.isNull(position)) {
                return 0.0;
            }
            row.reset();
            int start = values.getFirstValueIndex(position);
            for (int i = start; i < start + values.getValueCount(position); i++) {
                row.add(analyzer, values.getBytesRef(i, scratch).utf8ToString(), termIndex);
            }
            return RuntimeBm25.score(row, weights, pageScorers);
        }

        @Override
        public long baseRamBytesUsed() {
            long bytes = BASE_RAM_BYTES_USED;
            for (ExpressionEvaluator input : inputs) {
                bytes += input.baseRamBytesUsed();
            }
            return bytes;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(inputs);
        }
    }
}
