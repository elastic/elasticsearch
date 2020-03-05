/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;

public class TaperedNgramTokenizer extends Tokenizer {

    
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
    
    /** Default read buffer size */ 
    public static final int DEFAULT_BUFFER_SIZE = 256;    

    public TaperedNgramTokenizer(int maxGram) {
        if (maxGram < 1) {
            throw new IllegalArgumentException("maxGram must be greater than zero");
        }
        this.maxGram = maxGram;
        termAtt.resizeBuffer(DEFAULT_BUFFER_SIZE);
    }    
    
    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();        
        while (true) {
            if (curTermBuffer == null) {                
                loadBufferFromReader();
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
            return false;
        }
    }
    
    void loadBufferFromReader() throws IOException {
        int upto = 0;
        curTermBuffer = termAtt.buffer();
        while (true) {
          final int length = input.read(curTermBuffer, upto, curTermBuffer.length-upto);
          if (length == -1) break;
          upto += length;
          if (upto > IndexWriter.MAX_TERM_LENGTH) {
              throw new IllegalArgumentException("Provided value longer than Lucene maximum term length of " 
                      + IndexWriter.MAX_TERM_LENGTH );
          }
          if (upto == curTermBuffer.length)
              curTermBuffer = termAtt.resizeBuffer(1+curTermBuffer.length);
        }
        termAtt.setLength(upto);
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
