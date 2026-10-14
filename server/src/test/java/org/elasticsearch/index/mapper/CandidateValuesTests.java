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
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Reading a document's values for the terms a query asks about, before any of them is analyzed. The answer may be
 * wider than the question; what it may never be is narrower, so that is what most of this asserts, against a
 * {@link MemoryIndex} of the same values under the same analyzer.
 */
public class CandidateValuesTests extends ESTestCase {

    private static final String FIELD = "body";

    public void testTheAnalyzersAValueIsReadFor() {
        assertTrue(CandidateValues.valuesSpellTheirTokens(new StandardAnalyzer()));
        assertTrue(CandidateValues.valuesSpellTheirTokens(new WhitespaceAnalyzer()));
        assertTrue(CandidateValues.valuesSpellTheirTokens(new SimpleAnalyzer()));
        assertTrue(CandidateValues.valuesSpellTheirTokens(new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)));
        assertTrue(CandidateValues.valuesSpellTheirTokens(new KeywordAnalyzer()));
    }

    public void testHowTheAnalyzerIsConfiguredDoesNotMatter() {
        // Stop words are dropped and a long token is cut short: neither leaves a token the value does not spell.
        assertTrue(CandidateValues.valuesSpellTheirTokens(new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)));
        final StandardAnalyzer shortTokens = new StandardAnalyzer();
        shortTokens.setMaxTokenLength(4);
        assertTrue(CandidateValues.valuesSpellTheirTokens(shortTokens));
    }

    public void testANamedAnalyzerIsUnwrapped() {
        assertTrue(CandidateValues.valuesSpellTheirTokens(new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())));
        assertTrue(CandidateValues.valuesSpellTheirTokens(Lucene.STANDARD_ANALYZER));
        assertFalse(CandidateValues.valuesSpellTheirTokens(new NamedAnalyzer("english", AnalyzerScope.INDEX, new EnglishAnalyzer())));
    }

    public void testTheAnalyzersAValueIsNotReadFor() {
        assertFalse("no analyzer leaves nothing to go by", CandidateValues.valuesSpellTheirTokens(null));
        assertFalse("a stemmer rewrites a token", CandidateValues.valuesSpellTheirTokens(new EnglishAnalyzer()));
        assertFalse("a chain of the same shape is still not recognised", CandidateValues.valuesSpellTheirTokens(new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final StandardTokenizer tokenizer = new StandardTokenizer();
                return new TokenStreamComponents(tokenizer, new LowerCaseFilter(tokenizer));
            }
        }));
    }

    /** Why the allowlist is needed at all: a stemmer leaves a token its value does not spell. */
    public void testAStemmedTokenIsNotSpelledByItsValue() {
        // The English analyzer stems "happy" to "happi", which the value spells nowhere.
        final Query query = new TermQuery(new Term(FIELD, "happi"));
        assertTrue("the English analyzer answers this query for this value", matches(new EnglishAnalyzer(), List.of("happy"), query));
        assertFalse(
            "and reading the value's bytes would wrongly rule it out, which is why that analyzer is not read",
            CandidateValues.of(query, FIELD).mayHold(values("happy"))
        );
    }

    public void testATermIsRequired() {
        final CandidateValues candidates = CandidateValues.of(new TermQuery(new Term(FIELD, "fox")), FIELD);
        assertTrue(candidates.mayHold(values("the quick brown fox")));
        assertTrue("case is answered for the ASCII letters", candidates.mayHold(values("The Quick Brown FOX")));
        assertFalse(candidates.mayHold(values("the quick brown bear")));
        assertTrue("one of the values holding it is enough", candidates.mayHold(values("a bear", "a fox")));
    }

    public void testEveryTermOfAPhraseIsRequired() {
        final CandidateValues candidates = CandidateValues.of(
            new PhraseQuery(FIELD, new BytesRef("quick"), new BytesRef("brown"), new BytesRef("fox")),
            FIELD
        );
        assertTrue(candidates.mayHold(values("the quick brown fox")));
        assertFalse("a value missing one of them cannot hold the phrase", candidates.mayHold(values("the quick brown bear")));
    }

    public void testOneTermOfADisjunctionIsRequired() {
        final CandidateValues candidates = CandidateValues.of(
            new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.SHOULD)
                .add(new TermQuery(new Term(FIELD, "bear")), Occur.SHOULD)
                .build(),
            FIELD
        );
        assertTrue(candidates.mayHold(values("a fox")));
        assertTrue(candidates.mayHold(values("a bear")));
        assertFalse(candidates.mayHold(values("a mouse")));
    }

    public void testEveryTermOfAConjunctionIsRequired() {
        final CandidateValues candidates = CandidateValues.of(
            new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.MUST)
                .add(new TermQuery(new Term(FIELD, "quick")), Occur.MUST)
                .build(),
            FIELD
        );
        assertTrue(candidates.mayHold(values("the quick fox")));
        assertFalse(candidates.mayHold(values("the slow fox")));
    }

    public void testAClauseADocumentMustNotAnswerRequiresNothing() {
        final CandidateValues candidates = CandidateValues.of(
            new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.SHOULD)
                .add(new TermQuery(new Term(FIELD, "bear")), Occur.MUST_NOT)
                .build(),
            FIELD
        );
        assertTrue(candidates.mayHold(values("a fox")));
        assertFalse("only the clause it must answer is read for", candidates.mayHold(values("a mouse")));
        assertTrue(
            "a document holding the term it must not is still a candidate, and is answered by analyzing it",
            candidates.mayHold(values("a fox and a bear"))
        );
    }

    public void testAQueryNamingItsTermsByARangeOfThemReadsNothing() {
        assertFalse(reads(new PrefixQuery(new Term(FIELD, "fo"))));
        assertFalse(
            "a disjunction holding one is answered by a document holding none of the terms it does name",
            reads(
                new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "quick")), Occur.SHOULD)
                    .add(new PrefixQuery(new Term(FIELD, "fo")), Occur.SHOULD)
                    .build()
            )
        );
        assertTrue(
            "while a conjunction holding one still requires the terms of its other clauses",
            reads(
                new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "quick")), Occur.MUST)
                    .add(new PrefixQuery(new Term(FIELD, "fo")), Occur.MUST)
                    .build()
            )
        );
    }

    public void testABoostAndAConstantScoreAreUnwrapped() {
        final Query term = new TermQuery(new Term(FIELD, "fox"));
        assertFalse(CandidateValues.of(new BoostQuery(term, 2f), FIELD).mayHold(values("a bear")));
        assertFalse(CandidateValues.of(new ConstantScoreQuery(term), FIELD).mayHold(values("a bear")));
        assertTrue(CandidateValues.of(new BoostQuery(new ConstantScoreQuery(term), 2f), FIELD).mayHold(values("a fox")));
    }

    public void testATermOfAnotherFieldReadsNothingOfTheseValues() {
        final Query otherField = new TermQuery(new Term("title", "fox"));
        assertFalse("the values read here are not that field's", reads(otherField));
        assertFalse(
            "and a disjunction holding such a clause is answered by a document holding none of this field's terms",
            reads(new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.SHOULD).add(otherField, Occur.SHOULD).build())
        );
        assertTrue(
            "while a conjunction still requires the terms of the clauses that do name this field",
            reads(new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.MUST).add(otherField, Occur.MUST).build())
        );
        assertFalse("a phrase over another field names none of these values' terms", reads(new PhraseQuery("title", "quick", "brown")));
    }

    public void testAQueryOfOnlyClausesADocumentMustNotAnswerReadsNothing() {
        assertFalse(
            "a document answers it by holding none of the terms it names, so none of them is required",
            reads(new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.MUST_NOT).build())
        );
    }

    public void testANestedDisjunctionNamesItsTermsAsAlternatives() {
        final CandidateValues candidates = CandidateValues.of(
            new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.SHOULD)
                    .add(new TermQuery(new Term(FIELD, "bear")), Occur.SHOULD)
                    .build(),
                Occur.SHOULD
            ).add(new TermQuery(new Term(FIELD, "mouse")), Occur.SHOULD).build(),
            FIELD
        );
        assertTrue(candidates.mayHold(values("a fox")));
        assertTrue(candidates.mayHold(values("a bear")));
        assertTrue(candidates.mayHold(values("a mouse")));
        assertFalse(candidates.mayHold(values("a moose")));
    }

    public void testAClauseADocumentMustAnswerIsRequiredWhereverItSits() {
        final CandidateValues candidates = CandidateValues.of(
            new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, "fox")), Occur.FILTER)
                .add(new TermQuery(new Term(FIELD, "quick")), Occur.SHOULD)
                .build(),
            FIELD
        );
        assertTrue(candidates.mayHold(values("the quick fox")));
        assertFalse("the clause it must answer is required", candidates.mayHold(values("the quick bear")));
        assertTrue("while the one it may answer is not", candidates.mayHold(values("a slow fox")));
    }

    public void testAPhraseRepeatingATermReadsForItOnce() {
        final CandidateValues candidates = CandidateValues.allOf(List.of(new BytesRef("the"), new BytesRef("quick"), new BytesRef("the")));
        assertTrue(candidates.mayHold(new BytesRef("the quick the")));
        assertTrue("the repeat asks nothing more of the value", candidates.mayHold(new BytesRef("the quick")));
        assertFalse(candidates.mayHold(new BytesRef("the slow")));
    }

    public void testTheValuesOfADocumentSpellItsTermsBetweenThem() {
        final CandidateValues candidates = CandidateValues.allOf(List.of(new BytesRef("quick"), new BytesRef("fox")));
        assertTrue("a term spelled by any value is spelled by the document", candidates.mayHold(values("a quick one", "a fox")));
        assertFalse(candidates.mayHold(values("a quick one", "a bear")));
    }

    public void testAQueryOfNoTermsReadsNothing() {
        assertFalse(reads(new BooleanQuery.Builder().build()));
        assertFalse(reads(new PhraseQuery.Builder().build()));
        assertFalse("and nothing is read for a matcher of no terms either", CandidateValues.anyOf(List.of()).reads());
        assertFalse(CandidateValues.allOf(List.of()).reads());
        assertFalse("which is what ANY is", CandidateValues.ANY.reads());
        assertTrue("and it keeps every value", CandidateValues.ANY.mayHold(new BytesRef("anything at all")));
    }

    public void testOneTermOfATermsQueryIsRequired() {
        final CandidateValues candidates = CandidateValues.of(
            new TermInSetQuery(FIELD, List.of(new BytesRef("fox"), new BytesRef("bear"))),
            FIELD
        );
        assertTrue(candidates.mayHold(values("a fox")));
        assertTrue(candidates.mayHold(values("a BEAR")));
        assertFalse(candidates.mayHold(values("a mouse")));
    }

    /** A value is read once per term, so a query naming many of them is left unread rather than paid for. */
    public void testAQueryOfManyTermsIsNotReadFor() {
        final List<BytesRef> few = new ArrayList<>();
        for (int i = 0; i < CandidateValues.MOST_TERMS_READ_FOR; i++) {
            few.add(new BytesRef("term" + i));
        }
        assertTrue(CandidateValues.of(new TermInSetQuery(FIELD, few), FIELD).reads());

        final List<BytesRef> many = new ArrayList<>(few);
        many.add(new BytesRef("oneTooMany"));
        assertFalse(CandidateValues.of(new TermInSetQuery(FIELD, many), FIELD).reads());

        final BooleanQuery.Builder wideDisjunction = new BooleanQuery.Builder();
        for (BytesRef term : many) {
            wideDisjunction.add(new TermQuery(new Term(FIELD, term)), Occur.SHOULD);
        }
        assertFalse("a disjunction of as many is left unread too", CandidateValues.of(wideDisjunction.build(), FIELD).reads());
        assertFalse("and so is a matcher naming them", CandidateValues.anyOf(many).reads());
        assertTrue(CandidateValues.anyOf(few).reads());
        assertFalse("a phrase of as many words is read for none of them", CandidateValues.allOf(many).reads());
    }

    /** The terms a matcher names, which is how a row-by-row source reads its values. */
    public void testWhatAMatcherAsksOfAValue() {
        final List<BytesRef> terms = List.of(new BytesRef("quick"), new BytesRef("fox"));
        final CandidateValues anyTerm = new TokenStreamMatching.AnyTerm(terms).candidates();
        assertTrue(anyTerm.mayHold(new BytesRef("the quick brown")));
        assertTrue(anyTerm.mayHold(new BytesRef("a fox")));
        assertFalse(anyTerm.mayHold(new BytesRef("a bear")));

        final CandidateValues phrase = new TokenStreamMatching.Phrase(terms).candidates();
        assertTrue("a phrase needs every one of its terms", phrase.mayHold(new BytesRef("the quick fox")));
        assertFalse(phrase.mayHold(new BytesRef("the quick brown")));
        assertTrue("case is answered for the ASCII letters here too", phrase.mayHold(new BytesRef("QUICK FOX")));
    }

    public void testAMatcherLeavingItsTermsUnsaidReadsNothing() {
        final TokenStreamMatching.Matcher everything = stream -> true;
        assertFalse(everything.candidates().reads());
    }

    private static boolean reads(Query query) {
        return CandidateValues.of(query, FIELD).reads();
    }

    public void testAValueNotReadAsBytesIsLeftUndecided() {
        final CandidateValues candidates = CandidateValues.of(new TermQuery(new Term(FIELD, "fox")), FIELD);
        assertTrue("a value spelled out as a string is not read", candidates.mayHold(List.<Object>of("a bear")));
    }

    /** The property that matters: whatever is ruled out here is answered no by analyzing it anyway. */
    public void testRulingOutADocumentNeverRulesOutAMatch() {
        int ruledOut = 0;
        int matched = 0;
        for (int iteration = 0; iteration < 2000; iteration++) {
            final Analyzer analyzer = randomAnalyzer();
            final List<String> documentValues = randomValues();
            final Query query = randomQuery(documentValues);
            final CandidateValues candidates = CandidateValues.of(query, FIELD);
            if (candidates == null) {
                continue;
            }
            final boolean mayHold = candidates.mayHold(values(documentValues.toArray(String[]::new)));
            final boolean matches = matches(analyzer, documentValues, query);
            if (mayHold == false) {
                assertFalse(
                    "values " + documentValues + " were ruled out for [" + query + "] under " + analyzer + " but answer it",
                    matches
                );
                ruledOut++;
            }
            if (matches) {
                matched++;
            }
        }
        assertThat("the read never ruled anything out, so it proved nothing", ruledOut, greaterThan(0));
        assertThat("no query ever matched, so the comparison proved nothing", matched, greaterThan(0));
    }

    private Analyzer randomAnalyzer() {
        return randomFrom(
            new StandardAnalyzer(),
            new WhitespaceAnalyzer(),
            new SimpleAnalyzer(),
            new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET),
            new KeywordAnalyzer()
        );
    }

    private static final String[] WORDS = { "quick", "brown", "fox", "the", "a", "QUICK", "Fox", "café", "naïve", "x" };

    private List<String> randomValues() {
        final List<String> values = new ArrayList<>();
        for (int i = between(1, 3); i > 0; i--) {
            final StringBuilder value = new StringBuilder();
            for (int w = between(0, 6); w > 0; w--) {
                value.append(value.isEmpty() ? "" : randomFrom(" ", ", ", " - ")).append(randomFrom(WORDS));
            }
            values.add(value.toString());
        }
        return values;
    }

    /** A query over the terms the values could hold, lowercased as the analyzers of the allowlist leave them. */
    private Query randomQuery(List<String> documentValues) {
        return switch (between(0, 4)) {
            case 0 -> new TermQuery(new Term(FIELD, randomTerm(documentValues)));
            case 1 -> new PhraseQuery(FIELD, randomTerm(documentValues), randomTerm(documentValues));
            case 2 -> {
                final BooleanQuery.Builder any = new BooleanQuery.Builder();
                for (int i = between(1, 3); i > 0; i--) {
                    any.add(new TermQuery(new Term(FIELD, randomTerm(documentValues))), Occur.SHOULD);
                }
                yield any.build();
            }
            case 3 -> new BooleanQuery.Builder().add(new TermQuery(new Term(FIELD, randomTerm(documentValues))), Occur.MUST)
                .add(new TermQuery(new Term(FIELD, randomTerm(documentValues))), Occur.MUST_NOT)
                .build();
            default -> new BoostQuery(new TermQuery(new Term(FIELD, randomTerm(documentValues))), 2f);
        };
    }

    private String randomTerm(List<String> documentValues) {
        if (randomBoolean()) {
            return randomFrom(WORDS).toLowerCase(Locale.ROOT);
        }
        // A word of the document itself, so the query is answered often enough for the comparison to mean something.
        final String[] words = String.join(" ", documentValues).split("[ ,-]+");
        return words.length == 0 ? "x" : randomFrom(words).toLowerCase(Locale.ROOT);
    }

    /** The authority: the values analyzed and the query run over them, which is what this read stands in front of. */
    private static boolean matches(Analyzer analyzer, List<String> documentValues, Query query) {
        final MemoryIndex index = new MemoryIndex(true, false);
        for (String value : documentValues) {
            index.addField(FIELD, value, analyzer);
        }
        return index.search(query) > 0;
    }

    private static List<Object> values(String... documentValues) {
        final List<Object> asBytes = new ArrayList<>(documentValues.length);
        for (String value : documentValues) {
            asBytes.add(new BytesRef(value));
        }
        return asBytes;
    }
}
