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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Set;

public class AnalyzeWrappersTests extends ESTestCase {
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

    public void testSimpleAnalyzer() throws IOException {
        Analyzer baseAnalyzer = new Analyzer() {
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

        try (TokenStream stream = baseAnalyzer.tokenStream("some_field", "I love using Elastic products")) {
            stream.reset();
            CharTermAttribute charTerm = stream.addAttribute(CharTermAttribute.class);
            stream.addAttribute(PositionIncrementAttribute.class);
            stream.addAttribute(OffsetAttribute.class);
            stream.addAttribute(TypeAttribute.class);
            stream.addAttribute(PositionLengthAttribute.class);

            while (stream.incrementToken()) {
                assertEquals("elastic".toUpperCase(Locale.ROOT), charTerm.toString());
            }
            stream.end();
        }
    }

    public void testWrappedAnalyzer() throws IOException {
        Analyzer baseAnalyzer = new Analyzer() {
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

        try (TokenStream stream = baseAnalyzer.tokenStream("some_field", "I love using Elastic products")) {
            stream.reset();
            CharTermAttribute charTerm = stream.addAttribute(CharTermAttribute.class);
            stream.addAttribute(PositionIncrementAttribute.class);
            stream.addAttribute(OffsetAttribute.class);
            stream.addAttribute(TypeAttribute.class);
            stream.addAttribute(PositionLengthAttribute.class);

            while (stream.incrementToken()) {
                assertEquals("elastic".toUpperCase(Locale.ROOT), charTerm.toString());
            }
            stream.end();
        }
    }

    public void testWrappedAnalyzerDifferentLoaders() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                TestPluginClassLoader pluginLoader = new TestPluginClassLoader(this.getClass().getClassLoader());
                @SuppressWarnings("unchecked")
                Class<AnalysisIteratorFactory> factoryClass = (Class<AnalysisIteratorFactory>) pluginLoader.loadClass(
                    DemoFilterIteratorFactory.class.getName()
                );

                AnalysisIteratorFactory factory = factoryClass.getConstructor().newInstance();

                Analyzer baseAnalyzer = new Analyzer() {
                    @Override
                    protected TokenStreamComponents createComponents(String fieldName) {
                        Tokenizer tokenizer = new StandardTokenizer();
                        TokenStream tokenStream = tokenizer;
                        tokenStream = new LowerCaseFilter(tokenStream);
                        tokenStream = new PluginIteratorStream(factory.newInstance(new ESTokenStream(tokenStream)));
                        tokenStream = new UpperCaseFilter(tokenStream);
                        return new TokenStreamComponents(tokenizer, tokenStream);
                    }
                };

                try (TokenStream stream = baseAnalyzer.tokenStream("some_field", "I love using Elastic products")) {
                    stream.reset();
                    CharTermAttribute charTerm = stream.addAttribute(CharTermAttribute.class);
                    stream.addAttribute(PositionIncrementAttribute.class);
                    stream.addAttribute(OffsetAttribute.class);
                    stream.addAttribute(TypeAttribute.class);
                    stream.addAttribute(PositionLengthAttribute.class);

                    while (stream.incrementToken()) {
                        assertEquals("elastic".toUpperCase(Locale.ROOT), charTerm.toString());
                    }
                    stream.end();
                }
            } catch (Exception x) {
                fail("Shouldn't reach here " + x.getMessage());
            }
            return null;
        });
    }

    public void testWrappedFrenchAnalyzer() throws IOException {
        Analyzer baseAnalyzer = new Analyzer() {
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

        try (
            TokenStream stream = baseAnalyzer.tokenStream(
                "some_field",
                "Plop, juste pour voir l'embrouille avec les plug-ins d'Elastic. M'enfin."
            )
        ) {
            stream.reset();
            CharTermAttribute charTerm = stream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
            OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
            stream.addAttribute(TypeAttribute.class);
            stream.addAttribute(PositionLengthAttribute.class);

            while (stream.incrementToken()) {
                assertEquals("elastic", charTerm.toString());
                assertEquals(posIncr.getPositionIncrement(), 10);
                assertEquals(53, offset.startOffset());
                assertEquals(62, offset.endOffset());
            }
            stream.end();
        }
    }

}
