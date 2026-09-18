/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.AutomataMatch;

/**
 * The multivalue analogue of {@link AutomataMatch}: matches a whole position's worth of
 * {@link BytesRef}s against an {@link Automaton} under an <em>any-value</em> reduction.
 * <p>
 * Where {@link AutomataMatch} answers "does this value match", this answers "does <em>any</em> value
 * of this (possibly multivalued) field match" — the pattern-matching analogue of how {@code mv_contains}
 * reduces equality, and the semantics a Lucene {@code wildcard}/{@code regexp} query already has over a
 * multivalued field.
 * <p>
 * The reduction is two-valued: a position with no values (a null or empty field) matches nothing and
 * yields {@code false} rather than {@code null}, so the predicate composes through {@code AND}/{@code OR}/{@code NOT}.
 * <p>
 * The evaluator is hand-written (rather than {@code @Evaluator}-generated) so it can take a
 * {@link BytesRefBlock#asVector() single-valued} fast path: when the field block has one value per position and no
 * nulls, it walks values directly, skipping the per-position {@code getValueCount}/{@code getFirstValueIndex} that the
 * generated per-position loop pays on every row. Single-valued fields are the common case, and on the dataset path
 * (no Lucene index, so no pushdown) this evaluator is the only thing that runs.
 */
public class MvAutomataMatch {
    /**
     * Build an {@link ExpressionEvaluator.Factory} that matches any value of a position against
     * {@code utf32Automaton}.
     */
    public static ExpressionEvaluator.Factory toEvaluator(Source source, ExpressionEvaluator.Factory field, Automaton utf32Automaton) {
        /*
         * Convert to UTF-8 ourselves rather than letting ByteRunAutomaton do it, so the automaton we
         * hand to toDot is the one actually being run — same reasoning as AutomataMatch.
         */
        Automaton automaton;
        try {
            automaton = Operations.determinize(new UTF32ToUTF8().convert(utf32Automaton), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        } catch (TooComplexToDeterminizeException e) {
            throw new IllegalArgumentException("Pattern was too complex to determinize", e);
        }

        ByteRunAutomaton run = new ByteRunAutomaton(automaton, true);
        // source is accepted for call-site symmetry with MvLikeAffixMatch and the generated evaluators, but these
        // two-valued functions never emit a warning, so nothing downstream needs it.
        return new Factory(field, run, AutomataMatch.toDot(automaton));
    }

    private record Factory(ExpressionEvaluator.Factory field, ByteRunAutomaton automaton, String pattern)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new Evaluator(field.get(context), automaton, pattern, context);
        }

        @Override
        public String toString() {
            return "MvAutomataMatchEvaluator[field=" + field + ", pattern=" + pattern + "]";
        }
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final ExpressionEvaluator field;
        private final ByteRunAutomaton automaton;
        private final String pattern;
        private final DriverContext driverContext;

        Evaluator(ExpressionEvaluator field, ByteRunAutomaton automaton, String pattern, DriverContext driverContext) {
            this.field = field;
            this.automaton = automaton;
            this.pattern = pattern;
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

        /** Single-valued, no nulls: exactly one value per position, so match it directly. */
        private BooleanBlock evalVector(int positionCount, BytesRefVector field) {
            BytesRef scratch = new BytesRef();
            try (BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    BytesRef v = field.getBytesRef(p, scratch);
                    result.appendBoolean(p, automaton.run(v.bytes, v.offset, v.length));
                }
                return result.build().asBlock();
            }
        }

        /** The general path: reduce any-value over each position's values (a missing/empty position yields false). */
        private BooleanBlock evalBlock(int positionCount, BytesRefBlock field) {
            BytesRef scratch = new BytesRef();
            try (BooleanVector.FixedBuilder result = driverContext.blockFactory().newBooleanVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    int start = field.getFirstValueIndex(p);
                    int end = start + field.getValueCount(p);
                    boolean matched = false;
                    for (int i = start; i < end; i++) {
                        BytesRef v = field.getBytesRef(i, scratch);
                        if (automaton.run(v.bytes, v.offset, v.length)) {
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
            return "MvAutomataMatchEvaluator[field=" + field + ", pattern=" + pattern + "]";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field);
        }
    }
}
