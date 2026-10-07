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
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;

/**
 * A {@link FuzzyQuery} whose Levenshtein automata are built once, in the constructor. Runtime {@code match} rewrites
 * its query against a fresh single-document {@link org.apache.lucene.index.memory.MemoryIndex} per row, and a plain
 * {@link FuzzyQuery} rebuilds its automata on every rewrite. Lucene doesn't keep them on the query on purpose, since
 * query caches hold queries as keys; that doesn't apply here, where the query lives only as long as the evaluator
 * factory.
 * <p>
 * Terms get the same {@link BoostAttribute} as in {@code FuzzyTermsEnum}, and the inherited rewrite method selects
 * and scores them, so matching and scores are unchanged. Unlike {@code FuzzyTermsEnum}, the enum doesn't lower the
 * edit distance once the top-terms queue is full; that is only an early exit, since {@link TopTermsRewrite} drops
 * non-competitive terms anyway.
 */
final class PrebuiltFuzzyQuery extends FuzzyQuery {
    /** Indexed by edit distance; index 0 is unused because an exact match is a bytes comparison. */
    private final CompiledAutomaton[] automata;
    private final int termLength;

    PrebuiltFuzzyQuery(FuzzyQuery query) {
        super(
            query.getTerm(),
            query.getMaxEdits(),
            query.getPrefixLength(),
            // FuzzyQuery has no getter for maxExpansions; here it only feeds equals/hashCode and validation
            query.getRewriteMethod() instanceof TopTermsRewrite<?> topTerms ? topTerms.getSize() : FuzzyQuery.defaultMaxExpansions,
            query.getTranspositions(),
            query.getRewriteMethod()
        );
        String text = query.getTerm().text();
        this.termLength = text.codePointCount(0, text.length());
        this.automata = new CompiledAutomaton[query.getMaxEdits() + 1];
        for (int k = 1; k <= query.getMaxEdits(); k++) {
            automata[k] = FuzzyQuery.getFuzzyAutomaton(text, k, query.getPrefixLength(), query.getTranspositions());
        }
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        if (getMaxEdits() == 0) {
            return super.getTermsEnum(terms, atts);
        }
        return new PrebuiltFuzzyTermsEnum(terms, automata, getTerm().bytes(), termLength);
    }

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
