/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.elasticsearch.index.analysis.PluginIteratorStream;
import org.elasticsearch.plugins.lucene.DelegatingTokenStream;
import org.elasticsearch.plugins.lucene.StableLuceneFilterIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Set;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@State(Scope.Thread)
@Fork(value = 1)
public class AnalysisGlueCodeBenchmark {

    @Param(
        value = {
            "(“Elastic”), the company behind Elasticsearch and the Elastic Stack, announces new updates and capabilities across the Elastic Enterprise Search Solution in its 7.16 release, giving customers more power and flexibility to create, tune, and manage their search experiences.\n"
                + "\n"
                + "The beta release of curations powered by adaptive relevance in Elastic App Search provides customers with automated recommendations for better tuning results. Based on users’ interactions with search results, adaptive relevance provides users with actionable insights by enabling them to harness the power of collected analytics and recommendations to tune search results for optimized search experiences.\n"
                + "\n"
                + "Elastic App Search and Workplace Search features are also now generally available and accessible in Kibana from a single management interface. Users can leverage cross-platform navigation to monitor and visualize search data while ensuring a seamless, unified search experience.\n"
                + "\n"
                + "Additionally, Elastic App Search now includes support for Google Firebase via the App Search extension, enabling users to more easily build premium search experiences into their applications via the seamless indexing of their data to Elastic Cloud. With this extension, users can focus on building out core components of their products by offloading premium search experiences to App Search." }
    )
    private String benchmarkText;

    @Param(
        value = {
            "(«Elastic»), la société à l'origine d'Elasticsearch et de la Suite Elastic, annonce de nouvelles mises à jour et fonctionnalités pour la solution Elastic Enterprise Search dans sa version 7.16, offrant aux clients plus de puissance et de flexibilité pour créer, ajuster et gérer leurs expériences de recherche.\n"
                + "La version bêta des curations alimentées par la pertinence adaptative dans Elastic App Search fournit aux clients des recommandations automatisées pour de meilleurs résultats de réglage. Basée sur les interactions des utilisateurs avec les résultats de recherche, la pertinence adaptative fournit aux utilisateurs des informations exploitables en leur permettant d'exploiter la puissance des analyses et des recommandations collectées pour ajuster les résultats de recherche afin d'optimiser les expériences de recherche.\n"
                + "\n"
                + "Les fonctionnalités Elastic App Search et Workplace Search sont désormais généralement disponibles et accessibles dans Kibana à partir d'une interface de gestion unique. Les utilisateurs peuvent tirer parti de la navigation multiplateforme pour surveiller et visualiser les données de recherche tout en garantissant une expérience de recherche transparente et unifiée.\n"
                + "\n"
                + "De plus, Elastic App Search inclut désormais la prise en charge de Google Firebase via l'extension App Search, permettant aux utilisateurs de créer plus facilement des expériences de recherche premium dans leurs applications via l'indexation transparente de leurs données sur Elastic Cloud. Avec cette extension, les utilisateurs peuvent se concentrer sur la création des composants de base de leurs produits en déchargeant les expériences de recherche premium sur App Search." }
    )
    private String frenchText;

    private Analyzer baseAnalyzer;
    private Analyzer wrappedAnalyzer;
    private Analyzer baseFrenchAnalyzer;
    private Analyzer wrappedFrenchAnalyzer;

    private class ElasticWordOnlyTokenFilter extends FilteringTokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        ElasticWordOnlyTokenFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() {
            return termAtt.toString().equalsIgnoreCase("elastic");
        }
    }

    @Setup
    public void init() {
        baseAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream tokenStream = tokenizer;
                tokenStream = new LowerCaseFilter(tokenStream);
                tokenStream = new ElasticWordOnlyTokenFilter(tokenStream);
                tokenStream = new UpperCaseFilter(tokenStream);
                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };

        wrappedAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream tokenStream = tokenizer;
                tokenStream = new LowerCaseFilter(tokenStream);
                tokenStream = new PluginIteratorStream(
                    new StableLuceneFilterIterator(
                        new ElasticWordOnlyTokenFilter(new DelegatingTokenStream(new ESTokenStream(tokenStream)))
                    )
                );
                tokenStream = new UpperCaseFilter(tokenStream);
                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };

        baseFrenchAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream tokenStream = tokenizer;
                tokenStream = new LowerCaseFilter(tokenStream);
                tokenStream = new ElisionFilter(tokenStream, new CharArraySet(Set.of("l", "m", "t", "qu", "n", "s", "d"), true));
                tokenStream = new StopFilter(tokenStream, FrenchAnalyzer.getDefaultStopSet());
                tokenStream = new ElasticWordOnlyTokenFilter(tokenStream);
                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };

        wrappedFrenchAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream tokenStream = tokenizer;
                tokenStream = new LowerCaseFilter(tokenStream);
                tokenStream = new ElisionFilter(tokenStream, new CharArraySet(Set.of("l", "m", "t", "qu", "n", "s", "d"), true));
                tokenStream = new StopFilter(tokenStream, FrenchAnalyzer.getDefaultStopSet());
                tokenStream = new PluginIteratorStream(
                    new StableLuceneFilterIterator(
                        new ElasticWordOnlyTokenFilter(new DelegatingTokenStream(new ESTokenStream(tokenStream)))
                    )
                );
                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };
    }

    @Benchmark
    public int processTextBase() throws IOException {
        return processText(baseAnalyzer, benchmarkText);
    }

    @Benchmark
    public int processTextWrapped() throws IOException {
        return processText(wrappedAnalyzer, benchmarkText);
    }

    @Benchmark
    public int processFrenchTextBase() throws IOException {
        return processText(baseFrenchAnalyzer, frenchText);
    }

    @Benchmark
    public int processFrenchTextWrapped() throws IOException {
        return processText(wrappedFrenchAnalyzer, frenchText);
    }

    private int processText(Analyzer analyzer, String text) throws IOException {
        int counter = 0;
        try (TokenStream stream = analyzer.tokenStream("some_field", text)) {
            stream.reset();
            stream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
            stream.addAttribute(OffsetAttribute.class);
            stream.addAttribute(TypeAttribute.class);
            stream.addAttribute(PositionLengthAttribute.class);

            while (stream.incrementToken()) {
                counter += posIncr.getPositionIncrement();
            }
            stream.end();
        }

        return counter;
    }

}
