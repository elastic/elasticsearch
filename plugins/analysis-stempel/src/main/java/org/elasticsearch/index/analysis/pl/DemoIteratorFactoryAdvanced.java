/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis.pl;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.analysis.AbstractAnalysisIteratorFactory;
import org.elasticsearch.plugins.analysis.AnalyzeSettings;
import org.elasticsearch.plugins.analysis.AnalyzeState;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;
import org.elasticsearch.plugins.analysis.StableLuceneAnalyzeIterator;

import java.util.Iterator;
import java.util.List;

public class DemoIteratorFactoryAdvanced extends AbstractAnalysisIteratorFactory {

    public DemoIteratorFactoryAdvanced(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
    }

    @Override
    public PortableAnalyzeIterator newInstance(List<AnalyzeToken> tokens, AnalyzeState prevState) {
        return new StableLuceneAnalyzeIterator(
            new ElasticWordOnlyTokenFilter(new AnalyzeTokenStream(tokens)),
            prevState,
            new AnalyzeSettings(100, 1));
    }

    private class AnalyzeTokenStream extends Tokenizer {
        private final List<AnalyzeToken> tokens;
        private Iterator<AnalyzeToken> tokenIterator;

        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
        private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
        private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
        private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);

        public AnalyzeTokenStream(List<AnalyzeToken> tokens) {
            this.tokens = tokens;
            this.tokenIterator = tokens.listIterator();
        }

        @Override
        public final boolean incrementToken() {
            clearAttributes();
            if (tokenIterator.hasNext() == false) {
                return false;
            }

            AnalyzeToken currentToken = tokenIterator.next();

            posIncrAtt.setPositionIncrement(1);
            offsetAtt.setOffset(currentToken.getStartOffset(), currentToken.getEndOffset());
            typeAtt.setType(currentToken.getType());
            posLenAtt.setPositionLength(currentToken.getPositionLength());
            termAtt.setEmpty().append(currentToken.getTerm());

            return true;
        }

        @Override
        public void reset() {
            tokenIterator = tokens.listIterator();
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
