/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.Result;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.CharArrayReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class PhraseSuggester extends Suggester<PhraseSuggestionContext> {
    private final BytesRef SEPARATOR = new BytesRef(" ");
    private static final String SUGGESTION_TEMPLATE_VAR_NAME = "suggestion";

    public static final PhraseSuggester INSTANCE = new PhraseSuggester();

    private PhraseSuggester() {}

    /*
     * More Ideas:
     *   - add ability to find whitespace problems -> we can build a poor mans decompounder with our index based on a automaton?
     *   - add ability to build different error models maybe based on a confusion matrix?
     *   - try to combine a token with its subsequent token to find / detect word splits (optional)
     *      - for this to work we need some way to defined the position length of a candidate
     *   - phonetic filters could be interesting here too for candidate selection
     */
    @Override
    public Suggestion<? extends Entry<? extends Option>> innerExecute(
        String name,
        PhraseSuggestionContext suggestion,
        IndexSearcher searcher,
        CharsRefBuilder spare
    ) throws IOException {
        double realWordErrorLikelihood = suggestion.realworldErrorLikelihood();
        final PhraseSuggestion response = new PhraseSuggestion(name, suggestion.getSize());
        final IndexReader indexReader = searcher.getIndexReader();
        List<PhraseSuggestionContext.DirectCandidateGenerator> generators = suggestion.generators();
        final int numGenerators = generators.size();
        final List<CandidateGenerator> gens = new ArrayList<>(generators.size());
        for (int i = 0; i < numGenerators; i++) {
            PhraseSuggestionContext.DirectCandidateGenerator generator = generators.get(i);
            DirectSpellChecker directSpellChecker = generator.createDirectSpellChecker();
            Terms terms = MultiTerms.getTerms(indexReader, generator.field());
            if (terms != null) {
                gens.add(
                    new DirectCandidateGenerator(
                        directSpellChecker,
                        generator.field(),
                        generator.suggestMode(),
                        indexReader,
                        realWordErrorLikelihood,
                        generator.size(),
                        generator.preFilter(),
                        generator.postFilter(),
                        terms
                    )
                );
            }
        }
        final String suggestField = suggestion.getField();
        final Terms suggestTerms = MultiTerms.getTerms(indexReader, suggestField);
        if (gens.size() > 0 && suggestTerms != null) {
            final NoisyChannelSpellChecker checker = new NoisyChannelSpellChecker(
                realWordErrorLikelihood,
                suggestion.getRequireUnigram(),
                suggestion.getTokenLimit()
            );
            final BytesRef separator = suggestion.separator();
            WordScorer wordScorer = suggestion.model()
                .newScorer(indexReader, suggestTerms, suggestField, realWordErrorLikelihood, separator);
            Result checkerResult;
            try (TokenStream stream = tokenStream(suggestion.getAnalyzer(), suggestion.getText(), spare, suggestion.getField())) {
                checkerResult = checker.getCorrections(
                    stream,
                    new MultiCandidateGeneratorWrapper(suggestion.getShardSize(), gens.toArray(new CandidateGenerator[gens.size()])),
                    suggestion.maxErrors(),
                    suggestion.getShardSize(),
                    wordScorer,
                    suggestion.confidence(),
                    suggestion.gramSize()
                );
            }

            PhraseSuggestion.Entry resultEntry = buildResultEntry(suggestion, spare, checkerResult.cutoffScore);
            response.addTerm(resultEntry);

            final BytesRefBuilder byteSpare = new BytesRefBuilder();
            final TemplateScript.Factory scriptFactory = suggestion.getCollateQueryScript();
            final boolean collatePrune = (scriptFactory != null) && suggestion.collatePrune();
            for (int i = 0; i < checkerResult.corrections.length; i++) {
                Correction correction = checkerResult.corrections[i];
                spare.copyUTF8Bytes(correction.join(SEPARATOR, byteSpare, null, null));
                boolean collateMatch = true;
                if (scriptFactory != null) {
                    // Checks if the template query collateScript yields any documents
                    // from the index for a correction, collateMatch is updated
                    final Map<String, Object> vars = suggestion.getCollateScriptParams();
                    vars.put(SUGGESTION_TEMPLATE_VAR_NAME, spare.toString());
                    SearchExecutionContext searchExecutionContext = suggestion.getSearchExecutionContext();
                    final String querySource = scriptFactory.newInstance(vars).execute();
                    try (
                        XContentParser parser = XContentFactory.xContent(querySource)
                            .createParser(searchExecutionContext.getParserConfig(), querySource)
                    ) {
                        QueryBuilder innerQueryBuilder = AbstractQueryBuilder.parseTopLevelQuery(parser);
                        final ParsedQuery parsedQuery = searchExecutionContext.toQuery(innerQueryBuilder);
                        collateMatch = Lucene.exists(searcher, parsedQuery.query());
                    }
                }
                if (collateMatch == false && collatePrune == false) {
                    continue;
                }
                Text phrase = new Text(spare.toString());
                Text highlighted = null;
                if (suggestion.getPreTag() != null) {
                    spare.copyUTF8Bytes(correction.join(SEPARATOR, byteSpare, suggestion.getPreTag(), suggestion.getPostTag()));
                    highlighted = new Text(spare.toString());
                }
                if (collatePrune) {
                    resultEntry.addOption(new PhraseSuggestion.Entry.Option(phrase, highlighted, (float) (correction.score), collateMatch));
                } else {
                    resultEntry.addOption(new PhraseSuggestion.Entry.Option(phrase, highlighted, (float) (correction.score)));
                }
            }
        } else {
            response.addTerm(buildResultEntry(suggestion, spare, Double.MIN_VALUE));
        }
        return response;
    }

    private static TokenStream tokenStream(Analyzer analyzer, BytesRef query, CharsRefBuilder spare, String field) {
        spare.copyUTF8Bytes(query);
        return analyzer.tokenStream(field, new CharArrayReader(spare.chars(), 0, spare.length()));
    }

    private static PhraseSuggestion.Entry buildResultEntry(SuggestionContext suggestion, CharsRefBuilder spare, double cutoffScore) {
        spare.copyUTF8Bytes(suggestion.getText());
        return new PhraseSuggestion.Entry(new Text(spare.toString()), 0, spare.length(), cutoffScore);
    }

    @Override
    protected Suggestion<? extends Entry<? extends Option>> emptySuggestion(
        String name,
        PhraseSuggestionContext suggestion,
        CharsRefBuilder spare
    ) {
        PhraseSuggestion phraseSuggestion = new PhraseSuggestion(name, suggestion.getSize());
        spare.copyUTF8Bytes(suggestion.getText());
        phraseSuggestion.addTerm(new PhraseSuggestion.Entry(new Text(spare.toString()), 0, spare.length()));
        return phraseSuggestion;
    }
}
