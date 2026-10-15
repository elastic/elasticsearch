/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;

/**
 * Any-value affix matcher: the {@code literal*} / {@code *literal} / {@code *literal*} fast paths of {@link MvLike},
 * open-coded over {@link ByteMatchers} rather than the automaton (see {@link MvLike#buildEvaluator} for why).
 * <p>
 * Like {@link MvAutomataMatch} this is hand-written so it can take a {@link BytesRefBlock#asVector() single-valued}
 * fast path — one value per position, no per-position {@code getValueCount}/{@code getFirstValueIndex}. The
 * {@link Shape} is branched on once per page, outside the row loop, so each loop stays monomorphic and the
 * {@code ByteMatchers} call inlines.
 */
public class MvLikeAffixMatch {
    public enum Shape {
        PREFIX,
        SUFFIX,
        CONTAINS
    }

    public static ExpressionEvaluator.Factory toEvaluator(Source source, ExpressionEvaluator.Factory field, Shape shape, BytesRef affix) {
        // source is accepted for call-site symmetry with the generated evaluators, but this two-valued function never
        // emits a warning, so nothing downstream needs it.
        return new Factory(field, shape, affix);
    }

    private record Factory(ExpressionEvaluator.Factory field, Shape shape, BytesRef affix) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(field.get(context), shape, affix, context);
        }

        @Override
        public String toString() {
            return "MvLikeAffixMatchEvaluator[field=" + field + ", shape=" + shape + ", affix=" + affix.utf8ToString() + "]";
        }
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final ExpressionEvaluator field;
        private final Shape shape;
        private final BytesRef affix;
        private final DriverContext driverContext;

        Evaluator(ExpressionEvaluator field, Shape shape, BytesRef affix, DriverContext driverContext) {
            this.field = field;
            this.shape = shape;
            this.affix = affix;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
                int positionCount = page.getPositionCount();
                BytesRefVector fieldVector = fieldBlock.asVector();
                return fieldVector != null ? evalVector(positionCount, fieldVector) : evalBlock(positionCount, fieldBlock);
            }
        }

        private boolean matches(BytesRef value) {
            return switch (shape) {
                case PREFIX -> ByteMatchers.startsWith(value, affix);
                case SUFFIX -> ByteMatchers.endsWith(value, affix);
                case CONTAINS -> ByteMatchers.containsLiteral(value, affix);
            };
        }

        private BooleanBlock evalVector(int positionCount, BytesRefVector field) {
            BytesRef scratch = new BytesRef();
            try (BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
                // Branch on shape once, outside the loop, so each loop is monomorphic and the ByteMatchers call inlines.
                switch (shape) {
                    case PREFIX -> {
                        for (int p = 0; p < positionCount; p++) {
                            result.appendBoolean(p, ByteMatchers.startsWith(field.getBytesRef(p, scratch), affix));
                        }
                    }
                    case SUFFIX -> {
                        for (int p = 0; p < positionCount; p++) {
                            result.appendBoolean(p, ByteMatchers.endsWith(field.getBytesRef(p, scratch), affix));
                        }
                    }
                    case CONTAINS -> {
                        for (int p = 0; p < positionCount; p++) {
                            result.appendBoolean(p, ByteMatchers.containsLiteral(field.getBytesRef(p, scratch), affix));
                        }
                    }
                }
                return result.build().asBlock();
            }
        }

        private BooleanBlock evalBlock(int positionCount, BytesRefBlock field) {
            BytesRef scratch = new BytesRef();
            try (BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    int start = field.getFirstValueIndex(p);
                    int end = start + field.getValueCount(p);
                    boolean matched = false;
                    for (int i = start; i < end; i++) {
                        if (matches(field.getBytesRef(i, scratch))) {
                            matched = true;
                            break;
                        }
                    }
                    result.appendBoolean(p, matched);
                }
                return result.build().asBlock();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
        }

        @Override
        public String toString() {
            return "MvLikeAffixMatchEvaluator[field=" + field + ", shape=" + shape + ", affix=" + affix.utf8ToString() + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field);
        }
    }
}
