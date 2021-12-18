/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.AbstractAnalysisIteratorFactory;
import org.elasticsearch.plugins.analysis.AnalyzeSettings;
import org.elasticsearch.plugins.analysis.AnalyzeState;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.ESTokenStream;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;
import org.elasticsearch.plugins.analysis.StableLuceneFilterIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DemoIteratorFactoryAdvanced extends AbstractAnalysisIteratorFactory {

    public DemoIteratorFactoryAdvanced(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
    }

    @Override
    public PortableAnalyzeIterator newInstance(ESTokenStream esTokenStream) {
        return new StableLuceneFilterIterator(
            new ElasticWordOnlyTokenFilter(new AnalyzeTokenStream(esTokenStream)),
            new AnalyzeState(-1, 0),
            new AnalyzeSettings(100, 1));
    }

    private class AnalyzeTokenStream extends Tokenizer {
        private final ESTokenStream tokenStream;

        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
        private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
        private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
        private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);

        public AnalyzeTokenStream(ESTokenStream tokenStream) {
            this.tokenStream = tokenStream;
        }

        @Override
        public final boolean incrementToken() throws IOException {
            clearAttributes();

            AnalyzeToken currentToken = tokenStream.incrementToken();

            if (currentToken == null) {
                return false;
            }

            posIncrAtt.setPositionIncrement(currentToken.getPosition());
            offsetAtt.setOffset(currentToken.getStartOffset(), currentToken.getEndOffset());
            typeAtt.setType(currentToken.getType());
            posLenAtt.setPositionLength(currentToken.getPositionLength());
            termAtt.setEmpty().append(currentToken.getTerm());

            return true;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            tokenStream.reset();
        }

        @Override
        public void end() throws IOException {
            super.end();
            tokenStream.end();
        }

        @Override
        public void close() throws IOException {
            super.close();
            tokenStream.close();
        }
    }

    private class ElasticWordOnlyTokenFilter extends FilteringTokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        public ElasticWordOnlyTokenFilter(TokenStream in) {
            super(in);
        }

        @Override
        protected boolean accept() {
            return termAtt.toString().equalsIgnoreCase("elastic");
        }
    }
}
