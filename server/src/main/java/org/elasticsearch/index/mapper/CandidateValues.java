/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A prefilter that scans a text value's raw bytes for the query's terms before the value is analyzed.
 *
 * <p>For the analyzers accepted by {@link #valuesSpellTheirTokens}, every token is a substring of the value, possibly
 * lowercased. So a value that contains none of the query's terms, ignoring case, cannot produce any of them as a
 * token and can be rejected without analysis. The filter is conservative: a value that contains a term's bytes may
 * still not hold it as a token ({@code cat} appears in {@code catalog}), so such values are still analyzed. A value
 * is only rejected when analysis would reject it too.
 *
 * <p>With {@link #readingTokensOf}, the filter can also accept a value: when the bytes around an occurrence show it
 * is a whole token (see {@link TokenBoundaries}) and the query is answered by that term alone, the value matches
 * without analysis.
 *
 * <p>Lowercasing can map several characters onto the same one, so a term can appear in a value under more than one
 * spelling. {@link CaseSpellings} lists them and each spelling is searched for. For ASCII terms, the case-insensitive
 * byte comparison already covers every spelling, so there is a single one.
 *
 * <p>The filter only reads bytes, so any source of byte values can use it: a binary doc-values column, an ES|QL
 * block, an external dataset. Build it from a Lucene query with {@link #of(Query, String)}, or from a list of terms
 * with {@link #anyOf} and {@link #allOf}.
 */
public final class CandidateValues {

    /** Accepts every value. Used when the query or the analyzer gives nothing to filter on. */
    public static final CandidateValues ANY = new CandidateValues(List.of());

    /**
     * The maximum number of byte patterns (terms and their case spellings) a value is scanned for. Each pattern is a
     * separate scan of the value, and a value that is not rejected pays for these scans on top of its analysis.
     * Queries that need more patterns are not filtered.
     */
    static final int MAX_NEEDLES = 16;

    /** The conjunction of clauses a value must satisfy: a value that matches none of the needles of a clause is rejected. */
    private final List<Clause> clauses;

    /**
     * A disjunction of terms. {@code needles} are the byte patterns to search for, compared ignoring ASCII case: every
     * case spelling of every term. {@code terms} are the terms whose occurrences can be confirmed as tokens: all of
     * the clause's terms until {@link #readingTokensOf} keeps only those {@link TokenBoundaries#canReadTokensOf}.
     */
    private record Clause(BytesRef[] needles, BytesRef[] terms) {}

    /** Detects whole tokens from the surrounding bytes, or null when the filter can only reject values. */
    @Nullable
    private final TokenBoundaries boundaries;

    /** Whether satisfying the clauses is enough to match the query, rather than only necessary. */
    private final boolean spellingAnswers;

    /**
     * The maximum number of occurrences of one needle checked for token boundaries. A value with more occurrences
     * than this, none of them a whole token, is analyzed instead.
     */
    static final int MAX_OCCURRENCES_PER_NEEDLE = 32;

    /** The outcome of scanning a value's bytes. */
    public enum Answer {
        /** The value matches the query; no need to analyze it. */
        YES,
        /** The value cannot match the query; no need to analyze it. */
        NO,
        /** The bytes are not conclusive; the value must be analyzed. */
        MAYBE
    }

    private CandidateValues(List<Clause> clauses) {
        this(clauses, null, false);
    }

    private CandidateValues(List<Clause> clauses, @Nullable TokenBoundaries boundaries, boolean spellingAnswers) {
        this.clauses = clauses;
        this.boundaries = boundaries;
        this.spellingAnswers = spellingAnswers;
    }

    /**
     * Returns a copy that can also answer {@link Answer#YES}, using the token boundaries of {@code analyzer}. Returns
     * this filter unchanged when the analyzer's boundaries are not known or when matching the clauses is not
     * sufficient to match the query.
     */
    public CandidateValues readingTokensOf(@Nullable Analyzer analyzer) {
        if (reads() == false || spellingAnswers == false) {
            return this;
        }
        final TokenBoundaries reading = TokenBoundaries.of(analyzer);
        if (reading == null) {
            return this;
        }
        final List<Clause> withTokens = new ArrayList<>(clauses.size());
        for (Clause clause : clauses) {
            final List<BytesRef> readable = new ArrayList<>(clause.terms().length);
            for (BytesRef term : clause.terms()) {
                if (reading.canReadTokensOf(term)) {
                    readable.add(term);
                }
            }
            withTokens.add(new Clause(clause.needles(), readable.toArray(BytesRef[]::new)));
        }
        return new CandidateValues(withTokens, reading, true);
    }

    /** Values containing any of {@code terms}. Containing one of them as a token matches. */
    public static CandidateValues anyOf(Collection<BytesRef> terms) {
        return terms.isEmpty() ? ANY : readingFor(List.<BytesRef[]>of(terms.toArray(BytesRef[]::new)), true);
    }

    /**
     * Values containing all of {@code terms}, as required by a phrase. This never answers {@link Answer#YES}, since
     * the bytes do not tell whether the terms are in the right order.
     */
    public static CandidateValues allOf(Collection<BytesRef> terms) {
        final List<BytesRef[]> clauses = new ArrayList<>(terms.size());
        for (BytesRef term : new LinkedHashSet<>(terms)) {
            clauses.add(new BytesRef[] { term });
        }
        return readingFor(clauses, false);
    }

    /**
     * Builds the filter for these clauses, or returns {@link #ANY} when they need more than {@link #MAX_NEEDLES}
     * needles in total. A clause is never searched for a subset of its spellings, since that would reject values
     * holding one of the others.
     */
    private static CandidateValues readingFor(List<BytesRef[]> terms, boolean spellingAnswers) {
        final List<Clause> clauses = new ArrayList<>(terms.size());
        int needles = 0;
        for (BytesRef[] alternatives : terms) {
            final Set<BytesRef> ofClause = new LinkedHashSet<>();
            for (BytesRef term : alternatives) {
                final List<BytesRef> spellings = CaseSpellings.of(term, MAX_NEEDLES);
                if (spellings == null) {
                    return ANY;
                }
                ofClause.addAll(spellings);
            }
            needles += ofClause.size();
            clauses.add(new Clause(ofClause.toArray(BytesRef[]::new), alternatives));
        }
        return needles == 0 || needles > MAX_NEEDLES ? ANY : new CandidateValues(clauses, null, spellingAnswers);
    }

    /** Whether this filter checks anything. When false, every value is accepted and callers can skip it. */
    public boolean reads() {
        return clauses.isEmpty() == false;
    }

    /**
     * Whether every token produced by {@code analyzer} is a substring of its input, possibly lowercased, which is what
     * the filter relies on.
     *
     * <p>This is an <b>allowlist of analyzers</b>, checked by class: each splits its input into substrings and at most
     * lowercases them or drops some, and none rewrites a token into text the input does not contain. These classes
     * are final, so a subclass cannot change that. Their options (stop words, maximum token length) only drop tokens
     * or split them shorter, so the property holds whatever the configuration. Custom analyzers built from index
     * settings are not recognized and are not filtered.
     */
    public static boolean valuesSpellTheirTokens(@Nullable Analyzer analyzer) {
        Analyzer unwrapped = analyzer;
        while (unwrapped instanceof NamedAnalyzer named) {
            unwrapped = named.analyzer();
        }
        return unwrapped instanceof StandardAnalyzer
            || unwrapped instanceof WhitespaceAnalyzer
            || unwrapped instanceof SimpleAnalyzer
            || unwrapped instanceof StopAnalyzer
            || unwrapped instanceof KeywordAnalyzer;
    }

    /**
     * The filter for {@code query} over the values of {@code field}, or {@link #ANY} when the query has no required
     * terms to scan for: for instance a prefix or fuzzy query, which does not list its terms, or a query that can
     * match through another field.
     */
    public static CandidateValues of(Query query, String field) {
        final List<BytesRef[]> clauses = new ArrayList<>();
        final boolean[] spellingAnswers = { true };
        return required(query, field, clauses, spellingAnswers) ? readingFor(clauses, spellingAnswers[0]) : ANY;
    }

    /** Whether one value may match, that is whether it is not rejected. */
    public boolean mayHold(BytesRef value) {
        for (Clause clause : clauses) {
            if (clauseAnswer(value, clause) == Answer.NO) {
                return false;
            }
        }
        return true;
    }

    /** Scans one value. */
    public Answer held(BytesRef value) {
        Answer answer = Answer.YES;
        for (Clause clause : clauses) {
            final Answer ofClause = clauseAnswer(value, clause);
            if (ofClause == Answer.NO) {
                return Answer.NO;
            }
            if (ofClause == Answer.MAYBE) {
                answer = Answer.MAYBE;
            }
        }
        return spellingAnswers ? answer : (answer == Answer.NO ? Answer.NO : Answer.MAYBE);
    }

    /**
     * Scans the values of one document. Like a multi-valued field, a clause is satisfied if any of the values
     * satisfies it.
     */
    public Answer held(List<Object> values) {
        Answer answer = Answer.YES;
        for (Clause clause : clauses) {
            final Answer ofClause = valuesAnswer(values, clause);
            if (ofClause == Answer.NO) {
                return Answer.NO;
            }
            if (ofClause == Answer.MAYBE) {
                answer = Answer.MAYBE;
            }
        }
        return spellingAnswers ? answer : (answer == Answer.NO ? Answer.NO : Answer.MAYBE);
    }

    private Answer valuesAnswer(List<Object> values, Clause clause) {
        Answer answer = Answer.NO;
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (value instanceof BytesRef bytes) {
                final Answer ofValue = clauseAnswer(bytes, clause);
                if (ofValue == Answer.YES) {
                    return Answer.YES;
                }
                if (ofValue == Answer.MAYBE) {
                    answer = Answer.MAYBE;
                }
            } else {
                // Encoding a non-bytes value to UTF-8 would cost about as much as analyzing it, so it is not scanned.
                return Answer.MAYBE;
            }
        }
        return answer;
    }

    /** Gives access to the values of one document by index. */
    @FunctionalInterface
    public interface ValueReader {
        BytesRef value(int index) throws IOException;
    }

    /**
     * Whether the {@code count} values of one document may match. A clause is satisfied if any of the values
     * satisfies it, so the terms of a conjunction may be spread across values.
     */
    public boolean mayHold(int count, ValueReader values) throws IOException {
        for (Clause clause : clauses) {
            boolean spelled = false;
            for (int value = 0; value < count && spelled == false; value++) {
                spelled = clauseAnswer(values.value(value), clause) != Answer.NO;
            }
            if (spelled == false) {
                return false;
            }
        }
        return true;
    }

    /** Scans the {@code count} values of one document. */
    public Answer held(int count, ValueReader values) throws IOException {
        Answer answer = Answer.YES;
        for (Clause clause : clauses) {
            Answer ofClause = Answer.NO;
            for (int value = 0; value < count && ofClause != Answer.YES; value++) {
                final Answer ofValue = clauseAnswer(values.value(value), clause);
                if (ofValue != Answer.NO) {
                    ofClause = ofValue;
                }
            }
            if (ofClause == Answer.NO) {
                return Answer.NO;
            }
            if (ofClause == Answer.MAYBE) {
                answer = Answer.MAYBE;
            }
        }
        return spellingAnswers ? answer : (answer == Answer.NO ? Answer.NO : Answer.MAYBE);
    }

    /** Whether the values of one document may match. Values that are not {@link BytesRef} are not scanned. */
    public boolean mayHold(List<Object> values) {
        for (Clause clause : clauses) {
            if (valuesAnswer(values, clause) == Answer.NO) {
                return false;
            }
        }
        return true;
    }

    /**
     * Scans one value for one clause. Each occurrence of each needle is checked: a whole token answers
     * {@link Answer#YES}, an occurrence inside a longer token is skipped, and an occurrence whose boundaries are
     * unclear makes the answer {@link Answer#MAYBE}. Without {@link #boundaries}, any occurrence gives
     * {@link Answer#MAYBE}.
     */
    private Answer clauseAnswer(BytesRef value, Clause clause) {
        if (boundaries == null) {
            // Only rejection is possible, so finding any occurrence is enough.
            return someNeedleMayBeHeld(value, clause) ? Answer.MAYBE : Answer.NO;
        }
        boolean unsettled = false;
        for (BytesRef needle : clause.needles()) {
            int from = value.offset;
            int remaining = value.length;
            for (int occurrences = 0; occurrences < MAX_OCCURRENCES_PER_NEEDLE; occurrences++) {
                final int at = ESVectorUtil.indexOfCandidateIgnoringAsciiCase(
                    value.bytes,
                    from,
                    remaining,
                    needle.bytes,
                    needle.offset,
                    needle.length
                );
                if (at < 0) {
                    break;
                }
                switch (verdictOf(value, at, needle, clause)) {
                    case TOKEN -> {
                        return Answer.YES;
                    }
                    case UNKNOWN -> unsettled = true;
                    case PART_OF_ONE -> {
                    }
                }
                from = at + 1;
                remaining = value.offset + value.length - from;
                if (remaining < needle.length) {
                    break;
                }
                if (occurrences + 1 == MAX_OCCURRENCES_PER_NEEDLE) {
                    // Too many occurrences that are not whole tokens: stop scanning and let analysis decide.
                    unsettled = true;
                }
            }
        }
        return unsettled ? Answer.MAYBE : Answer.NO;
    }

    /** Whether any needle of the clause occurs in the value. */
    private static boolean someNeedleMayBeHeld(BytesRef value, Clause clause) {
        for (BytesRef needle : clause.needles()) {
            if (ESVectorUtil.mayContainIgnoringAsciiCase(
                value.bytes,
                value.offset,
                value.length,
                needle.bytes,
                needle.offset,
                needle.length
            )) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether the occurrence of {@code needle} at {@code at} is a whole token. Boundaries are only checked when
     * the bytes lowercase exactly to one of the clause's terms (the needle search is looser than that) and the term
     * can be recognized as a token at all (see {@link TokenBoundaries#canReadTokensOf}).
     */
    private TokenBoundaries.Verdict verdictOf(BytesRef value, int at, BytesRef needle, Clause clause) {
        if (boundaries == null) {
            return TokenBoundaries.Verdict.UNKNOWN;
        }
        for (BytesRef term : clause.terms()) {
            if (term.length == needle.length && TokenBoundaries.spellsTerm(value, at, term)) {
                return boundaries.verdictAt(value, at, needle.length);
            }
        }
        // A looser match of the needle, or a term whose tokens cannot be recognized: let analysis decide.
        return TokenBoundaries.Verdict.UNKNOWN;
    }

    /**
     * Adds the clauses that any match of {@code query} must satisfy to {@code clauses}, and returns whether there are
     * any. Clears {@code spellingAnswers[0]} when satisfying them is not sufficient to match the query.
     */
    private static boolean required(Query query, String field, List<BytesRef[]> clauses, boolean[] spellingAnswers) {
        if (query instanceof BoostQuery boost) {
            return required(boost.getQuery(), field, clauses, spellingAnswers);
        }
        if (query instanceof ConstantScoreQuery constant) {
            return required(constant.getQuery(), field, clauses, spellingAnswers);
        }
        if (query instanceof TermQuery term) {
            if (term.getTerm().field().equals(field) == false) {
                return false;
            }
            clauses.add(new BytesRef[] { term.getTerm().bytes() });
            return true;
        }
        if (query instanceof TermInSetQuery terms) {
            // One clause with the terms as alternatives. The count is checked before copying the terms, since a
            // terms query can hold a great many of them.
            if (terms.getField().equals(field) == false || terms.getTermsCount() > MAX_NEEDLES) {
                return false;
            }
            final List<BytesRef> alternatives = new ArrayList<>((int) terms.getTermsCount());
            try {
                final BytesRefIterator spellings = terms.getBytesRefIterator();
                for (BytesRef term = spellings.next(); term != null; term = spellings.next()) {
                    alternatives.add(BytesRef.deepCopyOf(term));
                }
            } catch (IOException e) {
                // The terms are held in memory, so reading them cannot fail.
                throw new UncheckedIOException(e);
            }
            clauses.add(alternatives.toArray(BytesRef[]::new));
            return true;
        }
        if (query instanceof PhraseQuery phrase) {
            if (phrase.getTerms().length == 0 || phrase.getField().equals(field) == false) {
                return false;
            }
            // Whatever the slop, a match contains every term of the phrase. Positions are not checked, so
            // containing them is not sufficient.
            spellingAnswers[0] = false;
            for (Term term : phrase.getTerms()) {
                clauses.add(new BytesRef[] { term.bytes() });
            }
            return true;
        }
        if (query instanceof BooleanQuery bool) {
            return required(bool, field, clauses, spellingAnswers);
        }
        return false;
    }

    private static boolean required(BooleanQuery query, String field, List<BytesRef[]> clauses, boolean[] spellingAnswers) {
        final List<Query> answeredByEvery = new ArrayList<>();
        final List<Query> answeredByOne = new ArrayList<>();
        for (BooleanClause clause : query) {
            switch (clause.occur()) {
                case MUST, FILTER -> answeredByEvery.add(clause.query());
                case SHOULD -> answeredByOne.add(clause.query());
                // An exclusion requires nothing, but it is not checked, so the other clauses are no longer sufficient.
                case MUST_NOT -> spellingAnswers[0] = false;
            }
        }
        if (query.getMinimumNumberShouldMatch() > Math.min(1, answeredByOne.size())) {
            // The filter checks that one optional clause matches. Requiring more (or more than there are) makes that
            // insufficient.
            spellingAnswers[0] = false;
        }
        if (answeredByEvery.isEmpty() == false) {
            // Every required clause must match, so the terms each of them requires are required.
            boolean anyRequires = false;
            for (Query clause : answeredByEvery) {
                final List<BytesRef[]> ofOne = new ArrayList<>();
                if (required(clause, field, ofOne, spellingAnswers)) {
                    clauses.addAll(ofOne);
                    anyRequires = true;
                } else {
                    // This clause is not checked, so the others are no longer sufficient.
                    spellingAnswers[0] = false;
                }
            }
            if (answeredByOne.isEmpty() == false) {
                // Optional clauses may add minimum_should_match constraints that are not checked.
                spellingAnswers[0] = false;
            }
            return anyRequires;
        }
        if (answeredByOne.isEmpty()) {
            return false;
        }
        // At least one optional clause must match, so a match contains one of the terms that each optional clause
        // requires first. Those terms form the alternatives of a single clause.
        final List<BytesRef> alternatives = new ArrayList<>();
        for (Query clause : answeredByOne) {
            final List<BytesRef[]> ofOne = new ArrayList<>();
            if (required(clause, field, ofOne, spellingAnswers) == false) {
                // This clause can match without any listed term, so nothing can be required.
                return false;
            }
            // Only the first required clause of each optional clause is used as an alternative.
            assert ofOne.isEmpty() == false : "a clause requiring terms named none: " + clause;
            if (ofOne.size() > 1) {
                // The other requirements of this clause are not checked, so the filter is no longer sufficient.
                spellingAnswers[0] = false;
            }
            alternatives.addAll(List.of(ofOne.get(0)));
        }
        clauses.add(alternatives.toArray(BytesRef[]::new));
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder spelled = new StringBuilder("CandidateValues(");
        for (int i = 0; i < clauses.size(); i++) {
            spelled.append(i == 0 ? "" : " and ").append("any of ");
            final BytesRef[] needles = clauses.get(i).needles();
            for (int j = 0; j < needles.length; j++) {
                spelled.append(j == 0 ? "" : ",").append(needles[j].utf8ToString());
            }
        }
        return spelled.append(')').toString();
    }
}
