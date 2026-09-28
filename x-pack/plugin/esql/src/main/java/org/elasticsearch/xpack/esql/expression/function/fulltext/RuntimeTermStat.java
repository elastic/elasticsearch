/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Internal, per-row input of the stats pass a BM25-scored runtime {@code MATCH} is rewritten into. Without a {@code term} it is the
 * number of tokens the row's values analyze into; with an (already analyzed) {@code term}, how often that term occurs. Either way it
 * is {@code null} rather than {@code 0}, so that {@code COUNT} of it counts the rows that have tokens (the collection's document
 * count) or contain the term (its document frequency), while {@code SUM} adds up the lengths.
 */
public class RuntimeTermStat extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RuntimeTermStat",
        RuntimeTermStat::new
    );

    private final Expression field;
    @Nullable
    private final BytesRef term;

    public RuntimeTermStat(Source source, Expression field, @Nullable BytesRef term) {
        super(source, List.of(field));
        this.field = field;
        this.term = term;
    }

    private RuntimeTermStat(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readOptional(StreamInput::readBytesRef));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeOptional(StreamOutput::writeBytesRef, term);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression field() {
        return field;
    }

    @Nullable
    public BytesRef term() {
        return term;
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RuntimeTermStat(source(), newChildren.getFirst(), term);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RuntimeTermStat::new, field, term);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory fieldFactory = toEvaluator.apply(field);
        Analyzer analyzer = RuntimeBm25.valuesAnalyzer(field, toEvaluator);
        Map<BytesRef, Integer> termIndex = term == null ? Map.of() : Map.of(term, 0);
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new Evaluator(fieldFactory.get(context), analyzer, termIndex, term == null, context);
            }

            @Override
            public String toString() {
                return "RuntimeTermStatEvaluator[field=" + fieldFactory + ", term=" + (term == null ? null : term.utf8ToString()) + "]";
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(term, ((RuntimeTermStat) obj).term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term);
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final ExpressionEvaluator field;
        private final Analyzer analyzer;
        private final Map<BytesRef, Integer> termIndex;
        private final boolean countLength;
        private final DriverContext context;
        private final RuntimeBm25.RowStats row;
        private final BytesRef scratch = new BytesRef();

        Evaluator(ExpressionEvaluator field, Analyzer analyzer, Map<BytesRef, Integer> termIndex, boolean countLength, DriverContext ctx) {
            this.field = field;
            this.analyzer = analyzer;
            this.termIndex = termIndex;
            this.countLength = countLength;
            this.context = ctx;
            this.row = new RuntimeBm25.RowStats(termIndex.size());
        }

        @Override
        public Block eval(Page page) {
            try (
                BytesRefBlock values = (BytesRefBlock) field.eval(page);
                LongBlock.Builder result = context.blockFactory().newLongBlockBuilder(page.getPositionCount())
            ) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    long stat = stat(values, p);
                    if (stat == 0) {
                        result.appendNull();
                    } else {
                        result.appendLong(stat);
                    }
                }
                return result.build();
            }
        }

        private long stat(BytesRefBlock values, int position) {
            if (values.isNull(position)) {
                return 0;
            }
            row.reset();
            int start = values.getFirstValueIndex(position);
            for (int i = start; i < start + values.getValueCount(position); i++) {
                row.add(analyzer, values.getBytesRef(i, scratch).utf8ToString(), termIndex);
            }
            return countLength ? row.length : row.termFreqs[0];
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
        }

        @Override
        public void close() {
            field.close();
        }
    }
}
