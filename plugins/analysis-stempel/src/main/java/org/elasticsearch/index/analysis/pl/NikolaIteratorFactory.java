/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis.pl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.AbstractAnalysisIteratorFactory;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.SimpleAnalyzeIterator;

import java.io.IOException;

public class NikolaIteratorFactory extends AbstractAnalysisIteratorFactory {

    private final Analyzer analyzer;

    public NikolaIteratorFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream tokenStream = new NikolaWordOnlyTokenFilter(tokenizer);

                return new TokenStreamComponents(tokenizer, tokenStream);
            }
        };
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public SimpleAnalyzeIterator newInstance(String text) {
        return new NikolaAnalyzeIterator(analyzer, text);
    }

    public class NikolaAnalyzeIterator implements SimpleAnalyzeIterator {
        private final TokenStream stream;
        private final CharTermAttribute term;
        private final PositionIncrementAttribute posIncr;
        private final OffsetAttribute offset;
        private final TypeAttribute type;
        private final PositionLengthAttribute posLen;

        int lastPosition;
        int lastOffset;

        public NikolaAnalyzeIterator(Analyzer analyzer, String text) {
            stream = analyzer.tokenStream(null, text);
            term = stream.addAttribute(CharTermAttribute.class);
            posIncr = stream.addAttribute(PositionIncrementAttribute.class);
            offset = stream.addAttribute(OffsetAttribute.class);
            type = stream.addAttribute(TypeAttribute.class);
            posLen = stream.addAttribute(PositionLengthAttribute.class);
        }

        @Override
        public void start() {
            lastPosition = -1;
            lastOffset = 0;
            try {
                stream.reset();
            } catch (IOException x) {
                throw new IllegalArgumentException("Unsupported token stream operation", x);
            }
        }

        @Override
        public AnalyzeToken next() {
            try {
                if (stream.incrementToken()) {
                    int increment = posIncr.getPositionIncrement();
                    if (increment > 0) {
                        lastPosition = lastPosition + increment;
                    }
                    return new AnalyzeToken(
                        term.toString(),
                        lastPosition,
                        lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(),
                        posLen.getPositionLength(),
                        type.type()
                    );
                }
                return null;
            } catch (IOException x) {
                throw new IllegalArgumentException("Unsupported token stream operation", x);
            }
        }

        @Override
        public void close() {
            try {
                stream.close();
            } catch (IOException x) {
                throw new IllegalArgumentException("Unsupported token stream operation", x);
            }
        }
    }

    public class NikolaWordOnlyTokenFilter extends FilteringTokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        public NikolaWordOnlyTokenFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() {
            return termAtt.toString().equalsIgnoreCase("elastic");
        }
    }

}
