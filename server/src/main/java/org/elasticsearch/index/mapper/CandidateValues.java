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
 * Which values are worth analyzing, read from the values' own bytes. A token an analyzer leaves is a stretch of the
 * value spelled as the value spelled it, so a value spelling none of a query's terms cannot analyze into any of
 * them.
 *
 * <p>It names a superset, where the postings of a field that holds terms stand: a value it keeps is analyzed
 * exactly as it would have been and answered by that, so only the values it rules out are answered here, and only
 * with no. A wider answer costs time; a narrower one would be wrong.
 *
 * <p>What it reads is that the term's bytes appear, not that they appear as a token: {@code cat} is spelled by
 * {@code catalog}, which holds no such token. So every value it keeps is analyzed.
 *
 * <p>A value spells a term it holds in one of a few ways, since an analyzer that lowers a token leaves it spelled
 * by whichever characters lower onto its own. {@link CaseSpellings} says which those are, so each is looked for;
 * a term of ASCII, and one of Latin letters with their accents, come to a single pattern, since comparing the
 * ASCII letters without their case already takes those spellings for the same.
 *
 * <p>It reads bytes and nothing else, so it serves every source that hands its values over as bytes: a column of a
 * field that indexes no terms, a runtime column, an external dataset. Three seams carry it further: an analyzer is
 * recognised by {@link #valuesSpellTheirTokens}, a query's own shape by {@link #of(Query, String)}, and a matcher's
 * terms by {@link #anyOf} and {@link #allOf}.
 */
public final class CandidateValues {

    /** Reads nothing, so every value is a candidate: what a query or an analyzer leaves nothing to go by. */
    public static final CandidateValues ANY = new CandidateValues(List.of());

    /**
     * The most terms a read looks for. A value is read once per term, so the read of a query naming many of them is
     * what a document answering it pays on top of the analysis the read did not save. This bounds that: a query
     * naming more terms is left unread.
     */
    static final int MOST_TERMS_READ_FOR = 16;

    /**
     * What a value must spell, as clauses of alternatives: one of every clause, so a value spelling none of a
     * clause's is ruled out.
     */
    private final List<Clause> clauses;

    /**
     * One clause: a value spelling any of {@code needles}, compared without ASCII case, holds what it asks. The
     * needles are every way a value could spell one of the clause's terms, so a value spelling none of them holds
     * none of those terms.
     */
    private record Clause(BytesRef[] needles) {}

    private CandidateValues(List<Clause> clauses) {
        this.clauses = clauses;
    }

    /** Values spelling any one of {@code terms}, which is what a match asks of them. */
    public static CandidateValues anyOf(Collection<BytesRef> terms) {
        return terms.isEmpty() ? ANY : readingFor(List.<BytesRef[]>of(terms.toArray(BytesRef[]::new)));
    }

    /** Values spelling every one of {@code terms}, which is what a phrase asks of a value. */
    public static CandidateValues allOf(Collection<BytesRef> terms) {
        final List<BytesRef[]> clauses = new ArrayList<>(terms.size());
        for (BytesRef term : new LinkedHashSet<>(terms)) {
            clauses.add(new BytesRef[] { term });
        }
        return readingFor(clauses);
    }

    /**
     * The read these terms ask for, or {@link #ANY} where they ask for more of a value than one is worth. A clause
     * whose terms can be spelled more ways than that is not read for in part: a part of a clause would rule out a
     * value that spells one of the ways left out.
     */
    private static CandidateValues readingFor(List<BytesRef[]> terms) {
        final List<Clause> clauses = new ArrayList<>(terms.size());
        int needles = 0;
        for (BytesRef[] alternatives : terms) {
            final Set<BytesRef> ofClause = new LinkedHashSet<>();
            for (BytesRef term : alternatives) {
                final List<BytesRef> spellings = CaseSpellings.of(term, MOST_TERMS_READ_FOR);
                if (spellings == null) {
                    return ANY;
                }
                ofClause.addAll(spellings);
            }
            needles += ofClause.size();
            clauses.add(new Clause(ofClause.toArray(BytesRef[]::new)));
        }
        return needles == 0 || needles > MOST_TERMS_READ_FOR ? ANY : new CandidateValues(clauses);
    }

    /** Whether this reads anything at all, which a caller skipping the read for a whole document goes by. */
    public boolean reads() {
        return clauses.isEmpty() == false;
    }

    /**
     * Whether the tokens of a field analyzed by {@code analyzer} are spelled by the values they come from, which is
     * what reading a value's bytes for a term goes by.
     *
     * <p>This is an <b>allowlist of analyzers</b>, by what they are rather than by what they carry: each cuts its
     * input into stretches of itself and at most lowercases them or leaves some out, and none rewrites a token into
     * something its value does not spell. Each of these classes is final, so what one does cannot be changed under
     * the check. That holds however one is configured, since what they take — stop words
     * to drop, the longest token to leave whole — only drops tokens or cuts them shorter. An analyzer assembled
     * from an index's own settings is not recognised, and its fields' values are analyzed as they were.
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
     * What {@code query} requires of the values of {@code field}, or {@link #ANY} where it requires nothing that can
     * be read from them: a query naming its terms by a range of them, as a prefix or a fuzziness does, names none of
     * them here, and nor does one answered by a term of another field, whose values these are not.
     */
    public static CandidateValues of(Query query, String field) {
        final List<BytesRef[]> clauses = new ArrayList<>();
        return required(query, field, clauses) ? readingFor(clauses) : ANY;
    }

    /** Whether one value's bytes could hold what is required, which is the only answer read from them. */
    public boolean mayHold(BytesRef value) {
        for (Clause clause : clauses) {
            if (someNeedleMayBeHeld(value, clause) == false) {
                return false;
            }
        }
        return true;
    }

    /** Reads one of a document's values, for a caller holding them in a shape of its own. */
    @FunctionalInterface
    public interface ValueReader {
        BytesRef value(int index) throws IOException;
    }

    /**
     * Whether the {@code count} values of one document could spell what is required, where a term spelled by any of
     * them is spelled by the document. That is what a multivalued field answers, so a conjunction is kept by values
     * that spell its terms between them.
     */
    public boolean mayHold(int count, ValueReader values) throws IOException {
        for (Clause clause : clauses) {
            boolean spelled = false;
            for (int value = 0; value < count && spelled == false; value++) {
                spelled = someNeedleMayBeHeld(values.value(value), clause);
            }
            if (spelled == false) {
                return false;
            }
        }
        return true;
    }

    /** The same over values already spelled out, where one not read as bytes is left undecided. */
    public boolean mayHold(List<Object> values) {
        for (Clause clause : clauses) {
            if (someValueMayHold(values, clause) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean someValueMayHold(List<Object> values, Clause clause) {
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (value instanceof BytesRef bytes) {
                if (someNeedleMayBeHeld(bytes, clause)) {
                    return true;
                }
            } else {
                // A value that did not arrive as bytes would have to be spelled out again to be read, which is the
                // work this is here to leave out, so it is left undecided.
                return true;
            }
        }
        return false;
    }

    // TODO: read the characters either side of a match. For an analyzer whose tokens are delimited runs of their
    // input, a match between two boundaries is the token itself, so a value could be answered yes without being
    // analyzed, which asks this for three answers rather than two; and a match whose neighbour continues a token
    // is no match at all, which rules out a candidate before its bytes are compared.
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
     * Collects into {@code clauses} the terms {@code query} cannot be answered without, and answers whether it
     * requires any at all.
     */
    private static boolean required(Query query, String field, List<BytesRef[]> clauses) {
        if (query instanceof BoostQuery boost) {
            return required(boost.getQuery(), field, clauses);
        }
        if (query instanceof ConstantScoreQuery constant) {
            return required(constant.getQuery(), field, clauses);
        }
        if (query instanceof TermQuery term) {
            if (term.getTerm().field().equals(field) == false) {
                return false;
            }
            clauses.add(new BytesRef[] { term.getTerm().bytes() });
            return true;
        }
        if (query instanceof TermInSetQuery terms) {
            // One of the terms is spelled, as one clause of alternatives, which is what a disjunction of them is.
            // The count is checked before the terms are copied, since a terms query can name a great many of them.
            if (terms.getField().equals(field) == false || terms.getTermsCount() > MOST_TERMS_READ_FOR) {
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
            // Wherever the slop lets the terms sit, a document answering the phrase holds every one of them.
            for (Term term : phrase.getTerms()) {
                clauses.add(new BytesRef[] { term.bytes() });
            }
            return true;
        }
        if (query instanceof BooleanQuery bool) {
            return required(bool, field, clauses);
        }
        return false;
    }

    private static boolean required(BooleanQuery query, String field, List<BytesRef[]> clauses) {
        final List<Query> answeredByEvery = new ArrayList<>();
        final List<Query> answeredByOne = new ArrayList<>();
        for (BooleanClause clause : query) {
            switch (clause.occur()) {
                case MUST, FILTER -> answeredByEvery.add(clause.query());
                case SHOULD -> answeredByOne.add(clause.query());
                // A clause a document must not answer only takes documents away, so it requires nothing of them.
                case MUST_NOT -> {
                }
            }
        }
        if (answeredByEvery.isEmpty() == false) {
            // Every one of these is answered, so whatever any of them requires is required.
            boolean anyRequires = false;
            for (Query clause : answeredByEvery) {
                final List<BytesRef[]> ofOne = new ArrayList<>();
                if (required(clause, field, ofOne)) {
                    clauses.addAll(ofOne);
                    anyRequires = true;
                }
            }
            return anyRequires;
        }
        if (answeredByOne.isEmpty()) {
            return false;
        }
        // One of these is answered, and whichever it is requires a term of its own first clause, so a document
        // holds one of those terms. Which of them is not known, so they become the alternatives of one clause.
        final List<BytesRef> alternatives = new ArrayList<>();
        for (Query clause : answeredByOne) {
            final List<BytesRef[]> ofOne = new ArrayList<>();
            if (required(clause, field, ofOne) == false) {
                // This clause can be answered by a document holding none of the terms named anywhere here.
                return false;
            }
            // Answering a clause spells every term of its first requirement, so one of those is spelled here.
            assert ofOne.isEmpty() == false : "a clause requiring terms named none: " + clause;
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
