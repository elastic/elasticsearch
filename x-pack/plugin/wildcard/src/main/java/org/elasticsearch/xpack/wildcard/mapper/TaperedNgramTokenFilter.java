/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

/**
 * Variation on Lucene's NGramTokenFilter that uses smaller (1 character) ngrams for the final few characters in a string. Helps improve
 * performance of short suffix queries e.g. "*.exe"
 */
public final class TaperedNgramTokenFilter extends TokenFilter {

    private final int maxGram;

    private char[] curTermBuffer;
    private int curTermLength;
    private int curTermCodePointCount;
    private int curGramSize;
    private int curPos;
    private int curPosIncr;
    private State state;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    /**
     * Creates a TaperedNgramTokenFilter that, for a given input term, produces all contained n-grams with length = maxGram. Will generate
     * small ngrams from maxGram down to 1 for the end of the input token.
     * 
     * Note: Care must be taken when choosing maxGram; depending on the input token size, this filter potentially produces a huge number of
     * unique terms in the index.
     * 
     * @param input
     *            {@link TokenStream} holding the input to be tokenized
     * @param maxGram
     *            the maximum length of the generated n-grams (apart from those at tail)
     */
    public TaperedNgramTokenFilter(TokenStream input, int maxGram) {
        super(input);
        if (maxGram < 1) {
            throw new IllegalArgumentException("maxGram must be greater than zero");
        }
        this.maxGram = maxGram;
    }

    @Override
    public boolean incrementToken() throws IOException {
        while (true) {
            if (curTermBuffer == null) {
                if (!input.incrementToken()) {
                    return false;
                }
                state = captureState();

                curTermLength = termAtt.length();
                curTermCodePointCount = Character.codePointCount(termAtt, 0, termAtt.length());
                curPosIncr += posIncrAtt.getPositionIncrement();
                curPos = -1;

                curTermBuffer = termAtt.buffer().clone();
                curGramSize = Math.min(curTermCodePointCount, maxGram);
            }
            curPos++;
            if ( (curPos + curGramSize) > curTermCodePointCount) {
                // Reached near the end of the string. Start tapering token size down to 1
                curGramSize = curTermCodePointCount - curPos;
            }
            if (curGramSize > 0) {
                restoreState(state);
                final int start = Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curPos);
                final int end = Character.offsetByCodePoints(curTermBuffer, 0, curTermLength, start, curGramSize);
                termAtt.copyBuffer(curTermBuffer, start, end - start);
                
                posIncrAtt.setPositionIncrement(curPosIncr);
                return true;
            }

            // Done with this input token, get next token on next iteration.
            curTermBuffer = null;
        }
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        curTermBuffer = null;
        curPosIncr = 0;
    }

    @Override
    public void end() throws IOException {
        super.end();
        posIncrAtt.setPositionIncrement(curPosIncr);
    }
}
