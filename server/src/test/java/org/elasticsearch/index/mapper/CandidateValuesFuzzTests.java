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
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;

/**
 * The read held to the analysis over text built to break it rather than to read like prose. A value is drawn from
 * every class of character the analyzers of the allowlist tell apart, at every edge where one of them cuts a token:
 * the bytes a folded compare takes for a letter or a digit, the bytes that join two words, runs longer than the
 * longest token, and the characters outside ASCII that carry, join or split a word. The terms are the tokens the
 * analyzer really leaves, stretches of the values that may or may not be tokens, and text of the same alphabet.
 *
 * <p>Whatever the read answers yes, the analysis answers yes; whatever it answers no, the analysis answers no. Each
 * way a caller asks is held to it, since each reaches the answer by its own walk over the values.
 */
public class CandidateValuesFuzzTests extends ESTestCase {

    private static final String FIELD = "body";

    /** Characters an analyzer of the allowlist treats alike, each class weighed the same however many it holds. */
    private static final String[][] CLASSES = {
        // Letters, in both cases, and digits.
        { "a", "b", "c", "k", "s", "x", "z" },
        { "A", "B", "K", "S", "X", "Z" },
        { "0", "1", "4", "9" },
        // What holds two runs together, and what joins two words between them.
        { "_" },
        { ".", ",", ":", ";", "'", "\"" },
        // What ends a token for every one of them.
        { " ", "-", "/", "(", ")", "!", "@", "`", "[", "{", "~", "\t", "\n" },
        // The bytes a digit or a letter differs from by the case bit, which a folded compare reads as one.
        { "\u0010", "\u0011", "\u0014", "\u0019", "\u0000", "\u001b", "\u007f" },
        // Outside ASCII: letters, the two that lower into ASCII, a final sigma, marks and formats that a word holds
        // on to, a script without spaces, a letter a quote joins, a surrogate pair, a wide digit.
        { "é", "ß", "Ü", "K", "İ", "Σ", "ς", "́", "­", "‍", "日本", "ひ", "א", "🙂", "１" } };

    public void testAnAnsweredValueIsTheAnalyzersAnswer() throws IOException {
        int yes = 0;
        int no = 0;
        for (int iteration = 0; iteration < 3000; iteration++) {
            final Analyzer analyzer = randomAnalyzer();
            final List<String> documentValues = new ArrayList<>();
            for (int i = between(1, 3); i > 0; i--) {
                documentValues.add(randomValue());
            }
            final Query query = randomQuery(analyzer, documentValues);
            final CandidateValues candidates = CandidateValues.of(query, FIELD).readingTokensOf(analyzer);
            if (candidates.reads() == false) {
                continue;
            }
            final String description = query + " over " + escaped(documentValues) + " under " + describe(analyzer);

            final List<Object> asBytes = new ArrayList<>(documentValues.size());
            for (String value : documentValues) {
                asBytes.add(new BytesRef(value));
            }
            final CandidateValues.Answer answer = candidates.held(asBytes);
            assertEquals(
                "read by index rather than as a list: " + description,
                answer,
                candidates.held(asBytes.size(), index -> (BytesRef) asBytes.get(index))
            );
            assertEquals(
                "whether it may hold, against what it holds: " + description,
                answer != CandidateValues.Answer.NO,
                candidates.mayHold(asBytes)
            );

            final boolean analyzed = matches(analyzer, documentValues, query);
            switch (answer) {
                case YES -> {
                    assertTrue("read as held but the analyzer answers no: " + description, analyzed);
                    yes++;
                }
                case NO -> {
                    assertFalse("read as holding none but the analyzer answers yes: " + description, analyzed);
                    no++;
                }
                case MAYBE -> {
                }
            }

            // A caller matching one value at a time answers the document by any of them.
            for (String value : documentValues) {
                final CandidateValues.Answer ofValue = candidates.held(new BytesRef(value));
                if (ofValue == CandidateValues.Answer.YES) {
                    assertTrue(
                        "one value read as held but the analyzer answers no: " + description,
                        matches(analyzer, List.of(value), query)
                    );
                } else if (ofValue == CandidateValues.Answer.NO) {
                    assertFalse("one value read as holding none: " + description, matches(analyzer, List.of(value), query));
                }
            }
        }
        // Text built to break the read must still reach both of its answers, or it proves nothing about them.
        assertThat("nothing was answered yes", yes, greaterThan(100));
        assertThat("nothing was answered no", no, greaterThan(100));
    }

    private Analyzer randomAnalyzer() {
        final StandardAnalyzer shortTokens = new StandardAnalyzer(randomBoolean() ? CharArraySet.EMPTY_SET : stopWords());
        shortTokens.setMaxTokenLength(between(1, 10));
        return randomFrom(
            new StandardAnalyzer(),
            new StandardAnalyzer(stopWords()),
            shortTokens,
            new SimpleAnalyzer(),
            new StopAnalyzer(stopWords()),
            new WhitespaceAnalyzer(),
            new KeywordAnalyzer()
        );
    }

    /** Stop words of this alphabet, so a value holds them often enough for a dropped token to matter. */
    private static CharArraySet stopWords() {
        final CharArraySet stop = new CharArraySet(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, false);
        stop.add("x");
        stop.add("ab");
        stop.add("k9");
        return stop;
    }

    private String randomValue() {
        final StringBuilder value = new StringBuilder();
        for (int i = between(0, 12); i > 0; i--) {
            switch (between(0, 19)) {
                // A run as long as the longest token of the analyzers whose limit is fixed, give or take.
                case 0 -> value.append(randomFrom("x", "k", "9", "aB")).append("x".repeat(between(250, 258)));
                // A word, so a run of letters stands between the edges the other classes make.
                case 1, 2, 3 -> value.append(randomFrom("cat", "Cat", "k9", "ab", "x", "42", "the", "kat"));
                default -> value.append(randomFrom(randomFrom(CLASSES)));
            }
        }
        return value.toString();
    }

    private Query randomQuery(Analyzer analyzer, List<String> documentValues) throws IOException {
        return switch (between(0, 7)) {
            case 0, 1 -> new TermQuery(term(analyzer, documentValues));
            case 2 -> new TermInSetQuery(FIELD, List.of(term(analyzer, documentValues).bytes(), term(analyzer, documentValues).bytes()));
            case 3 -> {
                final BooleanQuery.Builder any = new BooleanQuery.Builder();
                for (int i = between(1, 3); i > 0; i--) {
                    any.add(new TermQuery(term(analyzer, documentValues)), Occur.SHOULD);
                }
                if (randomBoolean()) {
                    any.setMinimumNumberShouldMatch(between(0, 2));
                }
                yield any.build();
            }
            case 4 -> new BooleanQuery.Builder().add(new TermQuery(term(analyzer, documentValues)), Occur.MUST)
                .add(new TermQuery(term(analyzer, documentValues)), randomFrom(Occur.MUST, Occur.MUST_NOT, Occur.SHOULD, Occur.FILTER))
                .build();
            case 5 -> new PhraseQuery(FIELD, term(analyzer, documentValues).bytes(), term(analyzer, documentValues).bytes());
            case 6 -> new BoostQuery(new TermQuery(term(analyzer, documentValues)), 2f);
            default -> new ConstantScoreQuery(new TermQuery(term(analyzer, documentValues)));
        };
    }

    /**
     * A term the values hold as a token, a stretch of them that may be one, or one of the alphabet's own, lowercased
     * as the analyzers that lowercase leave a token.
     */
    private Term term(Analyzer analyzer, List<String> documentValues) throws IOException {
        final String value = randomFrom(documentValues);
        switch (between(0, 2)) {
            case 0 -> {
                final List<BytesRef> tokens = TokenStreamMatching.analyzeTerms(analyzer, FIELD, value);
                if (tokens.isEmpty() == false) {
                    return new Term(FIELD, BytesRef.deepCopyOf(randomFrom(tokens)));
                }
            }
            case 1 -> {
                if (value.isEmpty() == false) {
                    final int from = between(0, value.length() - 1);
                    final int to = Math.min(value.length(), from + between(1, 6));
                    // A stretch cut through a surrogate pair is no text at all, and no analyzer leaves it.
                    if (Character.isLowSurrogate(value.charAt(from)) == false
                        && (to == value.length() || Character.isLowSurrogate(value.charAt(to)) == false)) {
                        return new Term(FIELD, value.substring(from, to).toLowerCase(Locale.ROOT));
                    }
                }
            }
            default -> {
            }
        }
        return new Term(
            FIELD,
            randomFrom(randomFrom(CLASSES)).toLowerCase(Locale.ROOT) + (randomBoolean() ? "" : randomFrom("at", "9", ""))
        );
    }

    /** The authority: the values analyzed and the query run over them. */
    private static boolean matches(Analyzer analyzer, List<String> documentValues, Query query) {
        final MemoryIndex index = new MemoryIndex(true, false);
        for (String value : documentValues) {
            index.addField(FIELD, value, analyzer);
        }
        return index.search(query) > 0;
    }

    private static String describe(Analyzer analyzer) {
        return analyzer instanceof StandardAnalyzer standard
            ? "StandardAnalyzer(maxTokenLength=" + standard.getMaxTokenLength() + ")"
            : analyzer.getClass().getSimpleName();
    }

    /** The values with every character outside printable ASCII spelled as its code point, so a failure reads. */
    private static String escaped(List<String> documentValues) {
        final List<String> escaped = new ArrayList<>(documentValues.size());
        for (String value : documentValues) {
            final StringBuilder spelled = new StringBuilder("[");
            value.codePoints().forEach(c -> {
                if (c >= 0x20 && c < 0x7f) {
                    spelled.appendCodePoint(c);
                } else {
                    spelled.append(String.format(Locale.ROOT, "\\u%04x", c));
                }
            });
            escaped.add(spelled.append(']').toString());
        }
        return escaped.toString();
    }
}
