/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.suggest.phrase;


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.phrase.NoisyChannelSpellChecker.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public Suggestion<? extends Entry<? extends Option>> innerExecute(String name, PhraseSuggestionContext suggestion,
            IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        double realWordErrorLikelihood = suggestion.realworldErrorLikelyhood();
        final PhraseSuggestion response = new PhraseSuggestion(name, suggestion.getSize());
        final IndexReader indexReader = searcher.getIndexReader();
        List<PhraseSuggestionContext.DirectCandidateGenerator>  generators = suggestion.generators();
        final int numGenerators = generators.size();
        final List<CandidateGenerator> gens = new ArrayList<>(generators.size());
        for (int i = 0; i < numGenerators; i++) {
            PhraseSuggestionContext.DirectCandidateGenerator generator = generators.get(i);
            DirectSpellChecker directSpellChecker = SuggestUtils.getDirectSpellChecker(generator);
            Terms terms = MultiFields.getTerms(indexReader, generator.field());
            if (terms !=  null) {
                gens.add(new DirectCandidateGenerator(directSpellChecker, generator.field(), generator.suggestMode(),
                        indexReader, realWordErrorLikelihood, generator.size(), generator.preFilter(), generator.postFilter(), terms));
            }
        }
        final String suggestField = suggestion.getField();
        final Terms suggestTerms = MultiFields.getTerms(indexReader, suggestField);
        if (gens.size() > 0 && suggestTerms != null) {
            final NoisyChannelSpellChecker checker = new NoisyChannelSpellChecker(realWordErrorLikelihood, suggestion.getRequireUnigram(),
                    suggestion.getTokenLimit());
            final BytesRef separator = suggestion.separator();
            WordScorer wordScorer = suggestion.model().newScorer(indexReader, suggestTerms, suggestField, realWordErrorLikelihood,
                    separator);
            Result checkerResult;
            try (TokenStream stream = checker.tokenStream(suggestion.getAnalyzer(), suggestion.getText(), spare, suggestion.getField())) {
                checkerResult = checker.getCorrections(stream,
                        new MultiCandidateGeneratorWrapper(suggestion.getShardSize(), gens.toArray(new CandidateGenerator[gens.size()])),
                        suggestion.maxErrors(), suggestion.getShardSize(), wordScorer, suggestion.confidence(), suggestion.gramSize());
                }

            PhraseSuggestion.Entry resultEntry = buildResultEntry(suggestion, spare, checkerResult.cutoffScore);
            response.addTerm(resultEntry);

            final BytesRefBuilder byteSpare = new BytesRefBuilder();
            final CompiledScript collateScript = suggestion.getCollateQueryScript();
            final boolean collatePrune = (collateScript != null) && suggestion.collatePrune();
            for (int i = 0; i < checkerResult.corrections.length; i++) {
                Correction correction = checkerResult.corrections[i];
                spare.copyUTF8Bytes(correction.join(SEPARATOR, byteSpare, null, null));
                boolean collateMatch = true;
                if (collateScript != null) {
                    // Checks if the template query collateScript yields any documents
                    // from the index for a correction, collateMatch is updated
                    final Map<String, Object> vars = suggestion.getCollateScriptParams();
                    vars.put(SUGGESTION_TEMPLATE_VAR_NAME, spare.toString());
                    QueryShardContext shardContext = suggestion.getShardContext();
                    ScriptService scriptService = shardContext.getScriptService();
                    final ExecutableScript executable = scriptService.executable(collateScript, vars);
                    final BytesReference querySource = (BytesReference) executable.run();
                    try (XContentParser parser = XContentFactory.xContent(querySource).createParser(querySource)) {
                        Optional<QueryBuilder> innerQueryBuilder = shardContext.newParseContext(parser).parseInnerQueryBuilder();
                        final ParsedQuery parsedQuery = shardContext.toQuery(innerQueryBuilder.orElse(new MatchNoneQueryBuilder()));
                        collateMatch = Lucene.exists(searcher, parsedQuery.query());
                    }
                }
                if (!collateMatch && !collatePrune) {
                    continue;
                }
                Text phrase = new Text(spare.toString());
                Text highlighted = null;
                if (suggestion.getPreTag() != null) {
                    spare.copyUTF8Bytes(correction.join(SEPARATOR, byteSpare, suggestion.getPreTag(), suggestion.getPostTag()));
                    highlighted = new Text(spare.toString());
                }
                if (collatePrune) {
                    resultEntry.addOption(new Suggestion.Entry.Option(phrase, highlighted, (float) (correction.score), collateMatch));
                } else {
                    resultEntry.addOption(new Suggestion.Entry.Option(phrase, highlighted, (float) (correction.score)));
                }
            }
        } else {
            response.addTerm(buildResultEntry(suggestion, spare, Double.MIN_VALUE));
        }
        return response;
    }

    private static PhraseSuggestion.Entry buildResultEntry(SuggestionContext suggestion, CharsRefBuilder spare, double cutoffScore) {
        spare.copyUTF8Bytes(suggestion.getText());
        return new PhraseSuggestion.Entry(new Text(spare.toString()), 0, spare.length(), cutoffScore);
    }

    @Override
    public SuggestionBuilder<?> innerFromXContent(QueryParseContext context) throws IOException {
        return PhraseSuggestionBuilder.innerFromXContent(context);
    }

    @Override
    public SuggestionBuilder<?> read(StreamInput in) throws IOException {
        return new PhraseSuggestionBuilder(in);
    }
}
