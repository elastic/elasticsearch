/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link FuzzyQuery} that builds its Levenshtein automata once, in the constructor.
 * <p>
 * Runtime {@code match} rewrites its query per row against a fresh single-document
 * {@link org.apache.lucene.index.memory.MemoryIndex}, and a plain {@link FuzzyQuery} rebuilds its automata on every
 * rewrite. Lucene avoids keeping automata on the query because query caches use queries as keys. That doesn't apply
 * here: the query lives only as long as the evaluator factory. No circuit breaker accounts for the automata. Instead,
 * {@link RuntimeSearch#prebuildFuzzyAutomata} keeps the {@link #ramBytesUsed} of all prebuilt clauses in a compiled
 * query under a fixed budget and leaves the rest as plain {@link FuzzyQuery}s.
 * <p>
 * One instance serves the evaluators of every driver running the query. That is safe because the terms enum only reads
 * the automata. {@link CompiledAutomaton#floor} writes to scratch state inside the automaton and must not be called
 * on them.
 * <p>
 * Hits and scores are identical to {@link FuzzyQuery}: the terms enum sets the same {@link BoostAttribute} as
 * {@code FuzzyTermsEnum}, and the inherited rewrite method selects and scores the terms. The enum doesn't lower the
 * edit distance once the top-terms queue is full, which {@code FuzzyTermsEnum} does only as an early exit;
 * {@link TopTermsRewrite} drops non-competitive terms either way.
 */
final class PrebuiltFuzzyQuery extends FuzzyQuery implements Accountable {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PrebuiltFuzzyQuery.class);

    /** Indexed by edit distance; index 0 is unused because an exact match is a bytes comparison. */
    private final CompiledAutomaton[] automata;
    private final int termLength;

    /**
     * @throws TooComplexToDeterminizeException if an automaton for the term is too complex to determinize
     */
    PrebuiltFuzzyQuery(FuzzyQuery query) {
        super(
            query.getTerm(),
            query.getMaxEdits(),
            query.getPrefixLength(),
            // FuzzyQuery has no maxExpansions getter; it only feeds equals/hashCode and validation, so take it from the rewrite method
            query.getRewriteMethod() instanceof TopTermsRewrite<?> topTerms ? topTerms.getSize() : FuzzyQuery.defaultMaxExpansions,
            query.getTranspositions(),
            query.getRewriteMethod()
        );
        if (query.getMaxEdits() == 0) {
            throw new IllegalArgumentException("an exact FuzzyQuery has no automata to prebuild");
        }
        // The automata FuzzyQuery builds: the prefix matches exactly and the edits apply to the rest of the term
        int[] codePoints = query.getTerm().text().codePoints().toArray();
        this.termLength = codePoints.length;
        int prefixLength = Math.min(query.getPrefixLength(), termLength);
        String prefix = UnicodeUtil.newString(codePoints, 0, prefixLength);
        int[] suffix = Arrays.copyOfRange(codePoints, prefixLength, termLength);
        LevenshteinAutomata levenshtein = new LevenshteinAutomata(suffix, Character.MAX_CODE_POINT, query.getTranspositions());
        this.automata = new CompiledAutomaton[query.getMaxEdits() + 1];
        for (int k = 1; k <= query.getMaxEdits(); k++) {
            automata[k] = new CompiledAutomaton(levenshtein.toAutomaton(k, prefix), true, false);
        }
    }

    /**
     * Ignores {@code atts}. {@code FuzzyTermsEnum} uses it to share automata between segments, which is unnecessary
     * here, and the rewrite methods read the {@link BoostAttribute} from the returned enum's own
     * {@link TermsEnum#attributes()}.
     */
    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return new PrebuiltFuzzyTermsEnum(terms, automata, getTerm().bytes(), termLength);
    }

    /**
     * Returns the prebuilt automaton for the maximum edit distance. {@link FuzzyQuery#getAutomata} builds a new one on
     * every call.
     */
    @Override
    public CompiledAutomaton getAutomata() {
        return automata[automata.length - 1];
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(getField())) {
            CompiledAutomaton automaton = getAutomata();
            visitor.consumeTermsMatching(this, getField(), () -> automaton.runAutomaton);
        }
    }

    @Override
    public long ramBytesUsed() {
        long bytes = SHALLOW_SIZE + getTerm().ramBytesUsed() + RamUsageEstimator.shallowSizeOf(automata);
        for (int k = 1; k < automata.length; k++) {
            bytes += automata[k].ramBytesUsed();
        }
        return bytes;
    }

    /**
     * Iterates the terms the max-edit automaton accepts and boosts each one by its exact edit distance, the way
     * {@code FuzzyTermsEnum} does.
     */
    private static final class PrebuiltFuzzyTermsEnum extends FilteredTermsEnum {
        private final CompiledAutomaton[] automata;
        private final BytesRef term;
        private final int termLength;
        private final BoostAttribute boostAtt;

        PrebuiltFuzzyTermsEnum(Terms terms, CompiledAutomaton[] automata, BytesRef term, int termLength) throws IOException {
            super(terms.intersect(automata[automata.length - 1], null), false);
            this.automata = automata;
            this.term = term;
            this.termLength = termLength;
            this.boostAtt = attributes().addAttribute(BoostAttribute.class);
        }

        @Override
        protected AcceptStatus accept(BytesRef candidate) {
            // the candidate's exact edit distance: step down while a tighter automaton still matches
            int ed = automata.length - 1;
            while (ed > 0 && matches(candidate, ed - 1)) {
                ed--;
            }
            if (ed == 0) {
                boostAtt.setBoost(1.0f);
            } else {
                int minTermLength = Math.min(UnicodeUtil.codePointCount(candidate), termLength);
                boostAtt.setBoost(1.0f - (float) ed / (float) minTermLength);
            }
            return AcceptStatus.YES;
        }

        private boolean matches(BytesRef candidate, int k) {
            return k == 0 ? candidate.equals(term) : automata[k].runAutomaton.run(candidate.bytes, candidate.offset, candidate.length);
        }
    }
}
