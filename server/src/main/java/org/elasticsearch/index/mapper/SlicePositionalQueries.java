/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.index.SliceIndexing;

import java.util.List;
import java.util.function.Supplier;

/**
 * Copies a query so that every term it names is looked up under one slice. A query that matches positions says which
 * terms it wants and where it wants them; only the terms change, since a slice index spells a term with the slice it
 * belongs to in front of it.
 *
 * <p>Each kind of query is copied explicitly. One that is not recognized is refused unless it names no term at all,
 * because returning it unchanged would ask a slice index for a term that is not in its dictionary and match nothing.
 */
final class SlicePositionalQueries {

    private SlicePositionalQueries() {}

    /** {@code query} with the terms of {@code slice} in place of the plain ones it was built with. */
    static Query inSlice(Query query, String slice) {
        return copy(query, SliceIndexing.termPrefixBytes(slice));
    }

    /** Whether every one of {@code queries} is a span query, which may only ever hold other span queries. */
    static boolean areSpans(List<Query> queries) {
        for (Query query : queries) {
            if (query instanceof SpanQuery == false) {
                return false;
            }
        }
        return true;
    }

    private static Query copy(Query query, byte[] prefix) {
        return switch (query) {
            case TermQuery term -> new TermQuery(term(term.getTerm(), prefix));
            case PhraseQuery phrase -> {
                final PhraseQuery.Builder builder = new PhraseQuery.Builder().setSlop(phrase.getSlop());
                final Term[] terms = phrase.getTerms();
                final int[] positions = phrase.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    builder.add(term(terms[i], prefix), positions[i]);
                }
                yield builder.build();
            }
            case MultiPhraseQuery phrase -> {
                final MultiPhraseQuery.Builder builder = new MultiPhraseQuery.Builder().setSlop(phrase.getSlop());
                final Term[][] terms = phrase.getTermArrays();
                final int[] positions = phrase.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    builder.add(terms(terms[i], prefix), positions[i]);
                }
                yield builder.build();
            }
            case MultiPhrasePrefixQuery phrase -> {
                // The slice goes in front of the prefix the last position asks for, which leaves it a prefix of the
                // terms of that slice and of no other.
                final MultiPhrasePrefixQuery copy = new MultiPhrasePrefixQuery(phrase.getField());
                copy.setSlop(phrase.getSlop());
                copy.setMaxExpansions(phrase.getMaxExpansions());
                final Term[][] terms = phrase.getTerms();
                final int[] positions = phrase.getPositions();
                for (int i = 0; i < terms.length; i++) {
                    copy.add(terms(terms[i], prefix), positions[i]);
                }
                yield copy;
            }
            // A synonym query a phrase prefix builds weighs its terms alike; the synonyms of a search that weighs them
            // apart are put under their slice as they are built, see MatchQueryParser.
            case SynonymQuery synonyms -> {
                final SynonymQuery.Builder builder = new SynonymQuery.Builder(synonyms.getField());
                for (Term synonym : synonyms.getTerms()) {
                    builder.addTerm(term(synonym, prefix));
                }
                yield builder.build();
            }
            case SpanTermQuery span -> new SpanTermQuery(term(span.getTerm(), prefix));
            case SpanNearQuery near -> new SpanNearQuery(spans(near.getClauses(), prefix), near.getSlop(), near.isInOrder());
            case SpanOrQuery or -> new SpanOrQuery(spans(or.getClauses(), prefix));
            case FieldMaskingSpanQuery masked -> new FieldMaskingSpanQuery(span(masked.getMaskedQuery(), prefix), masked.getField());
            case BooleanQuery bool -> {
                final BooleanQuery.Builder builder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(
                    bool.getMinimumNumberShouldMatch()
                );
                for (BooleanClause clause : bool) {
                    builder.add(copy(clause.query(), prefix), clause.occur());
                }
                yield builder.build();
            }
            case BoostQuery boost -> new BoostQuery(copy(boost.getQuery(), prefix), boost.getBoost());
            case ConstantScoreQuery constant -> new ConstantScoreQuery(copy(constant.getQuery(), prefix));
            default -> {
                if (readsNoTerms(query)) {
                    // There is nothing to put under a slice, as for a gap in a span or a query over doc values.
                    yield query;
                }
                throw new IllegalArgumentException(
                    "a ["
                        + query.getClass().getSimpleName()
                        + "] cannot be confined to a slice; the terms of a slice index carry the slice they belong to, so a query "
                        + "that names a term has to be built for one slice at a time"
                );
            }
        };
    }

    /**
     * Whether the query asks the terms dictionary for nothing, by name or by automaton, which is what a query that
     * reads terms reports through its visitor. Such a query is the same in every slice.
     */
    private static boolean readsNoTerms(Query query) {
        final boolean[] reads = new boolean[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                reads[0] = true;
            }

            @Override
            public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
                reads[0] = true;
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                return this;
            }
        });
        return reads[0] == false;
    }

    private static SpanQuery[] spans(SpanQuery[] clauses, byte[] prefix) {
        final SpanQuery[] copy = new SpanQuery[clauses.length];
        for (int i = 0; i < clauses.length; i++) {
            copy[i] = span(clauses[i], prefix);
        }
        return copy;
    }

    private static SpanQuery span(SpanQuery query, byte[] prefix) {
        final Query copy = copy(query, prefix);
        assert copy instanceof SpanQuery : copy.getClass();
        return (SpanQuery) copy;
    }

    private static Term[] terms(Term[] terms, byte[] prefix) {
        final Term[] copy = new Term[terms.length];
        for (int i = 0; i < terms.length; i++) {
            copy[i] = term(terms[i], prefix);
        }
        return copy;
    }

    /**
     * The term as its slice spells it, or the term itself on a field a slice index indexes plainly, such as the
     * {@code _}-prefixed fields {@link SliceIndexing#prefixesTerms} leaves out.
     */
    private static Term term(Term term, byte[] prefix) {
        if (SliceIndexing.prefixesTerms(term.field()) == false) {
            return term;
        }
        return new Term(term.field(), SliceIndexing.prefixTerm(prefix, term.bytes()));
    }
}
