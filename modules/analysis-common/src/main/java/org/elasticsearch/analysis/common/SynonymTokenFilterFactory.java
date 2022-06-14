/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.function.Function;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(SynonymTokenFilterFactory.class);

    private final String format;
    private final boolean expand;
    private final boolean lenient;
    protected final Settings settings;
    protected final Environment environment;
    protected final AnalysisMode analysisMode;

    SynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.settings = settings;

        if (settings.get("ignore_case") != null) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.ANALYSIS,
                "synonym_ignore_case_option",
                "The ignore_case option on the synonym_graph filter is deprecated. "
                    + "Instead, insert a lowercase filter in the filter chain before the synonym_graph filter."
            );
        }

        this.expand = settings.getAsBoolean("expand", true);
        this.lenient = settings.getAsBoolean("lenient", false);
        this.format = settings.get("format", "");
        boolean updateable = settings.getAsBoolean("updateable", false);
        this.analysisMode = updateable ? AnalysisMode.SEARCH_TIME : AnalysisMode.ALL;
        this.environment = env;
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException("Call createPerAnalyzerSynonymFactory to specialize this factory for an analysis chain first");
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> previousTokenFilters,
        Function<String, TokenFilterFactory> allFilters
    ) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters);
        final SynonymMap synonyms = buildSynonyms(analyzer, getRulesFromSettings(environment));
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return synonyms.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonyms, false);
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }
        };
    }

    static Analyzer buildSynonymAnalyzer(
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> tokenFilters
    ) {
        return new CustomAnalyzer(
            tokenizer,
            charFilters.toArray(new CharFilterFactory[0]),
            tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new)
        );
    }

    SynonymMap buildSynonyms(Analyzer analyzer, Reader rules) {
        try {
            SynonymMap.Builder parser;
            if ("wordnet".equalsIgnoreCase(format)) {
                parser = new ESWordnetSynonymParser(true, expand, lenient, analyzer);
                ((ESWordnetSynonymParser) parser).parse(rules);
            } else {
                parser = new ESSolrSynonymParser(true, expand, lenient, analyzer);
                ((ESSolrSynonymParser) parser).parse(rules);
            }
            return parser.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to build synonyms", e);
        }
    }

    Reader getRulesFromSettings(Environment env) {
        Reader rulesReader;
        if (settings.getAsList("synonyms", null) != null) {
            List<String> rulesList = Analysis.getWordList(env, settings, "synonyms");
            StringBuilder sb = new StringBuilder();
            for (String line : rulesList) {
                sb.append(line).append(System.lineSeparator());
            }
            rulesReader = new StringReader(sb.toString());
        } else if (settings.get("synonyms_path") != null) {
            rulesReader = Analysis.getReaderFromFile(env, settings, "synonyms_path");
        } else {
            throw new IllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }
        return rulesReader;
    }

}
