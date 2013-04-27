package org.apache.lucene.analysis.ngram;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

/**
 * Tokenizes the input into n-grams of the given size(s).
 * <a name="version"/>
 * <p>You must specify the required {@link Version} compatibility when
 * creating a {@link NGramTokenFilter}. As of Lucene 4.4, this token filters:<ul>
 * <li>emits all n-grams for the same token at the same position,</li>
 * <li>does not modify offsets,</li>
 * <li>sorts n-grams by their offset in the original token first, then
 * increasing length (meaning that "abc" will give "a", "ab", "abc", "b", "bc",
 * "c").</li></ul>
 * <p>You can make this filter use the old behavior by providing a version &lt;
 * {@link Version#LUCENE_44} in the constructor but this is not recommended as
 * it will lead to broken {@link TokenStream}s that will cause highlighting
 * bugs.
 */
public final class XNGramTokenFilter extends TokenFilter {
    
    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1476563
        assert Lucene.VERSION.ordinal() < Version.LUCENE_42.ordinal()+2  : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this should can be removed"; 
    }
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;

  private final int minGram, maxGram;

  private char[] curTermBuffer;
  private int curTermLength;
  private int curGramSize;
  private int curPos;
  private int curPosInc, curPosLen;
  private int tokStart;
  private int tokEnd;
  private boolean hasIllegalOffsets; // only if the length changed before this filter

  private final Version version;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt;
  private final PositionLengthAttribute posLenAtt;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates NGramTokenFilter with given min and max n-grams.
   * @param version Lucene version to enable correct position increments.
   *                See <a href="#version">above</a> for details.
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XNGramTokenFilter(Version version, TokenStream input, int minGram, int maxGram) {
    super(new LengthFilter(true, input, minGram, Integer.MAX_VALUE));
    this.version = version;
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    if (version.onOrAfter(Version.LUCENE_42)) {
      posIncAtt = addAttribute(PositionIncrementAttribute.class);
      posLenAtt = addAttribute(PositionLengthAttribute.class);
    } else {
      posIncAtt = new PositionIncrementAttribute() {
        @Override
        public void setPositionIncrement(int positionIncrement) {}
        @Override
        public int getPositionIncrement() {
          return 0;
        }
      };
      posLenAtt = new PositionLengthAttribute() {
        @Override
        public void setPositionLength(int positionLength) {}        
        @Override
        public int getPositionLength() {
          return 0;
        }
      };
    }
  }

  /**
   * Creates NGramTokenFilter with default min and max n-grams.
   * @param version Lucene version to enable correct position increments.
   *                See <a href="#version">above</a> for details.
   * @param input {@link TokenStream} holding the input to be tokenized
   */
  public XNGramTokenFilter(Version version, TokenStream input) {
    this(version, input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE);
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (curTermBuffer == null) {
        if (!input.incrementToken()) {
          return false;
        } else {
          curTermBuffer = termAtt.buffer().clone();
          curTermLength = termAtt.length();
          curGramSize = minGram;
          curPos = 0;
          curPosInc = posIncAtt.getPositionIncrement();
          curPosLen = posLenAtt.getPositionLength();
          tokStart = offsetAtt.startOffset();
          tokEnd = offsetAtt.endOffset();
          // if length by start + end offsets doesn't match the term text then assume
          // this is a synonym and don't adjust the offsets.
          hasIllegalOffsets = (tokStart + curTermLength) != tokEnd;
        }
      }
      if (version.onOrAfter(Version.LUCENE_42)) {
        if (curGramSize > maxGram || curPos + curGramSize > curTermLength) {
          ++curPos;
          curGramSize = minGram;
        }
        if (curPos + curGramSize <= curTermLength) {
          clearAttributes();
          termAtt.copyBuffer(curTermBuffer, curPos, curGramSize);
          posIncAtt.setPositionIncrement(curPosInc);
          curPosInc = 0;
          posLenAtt.setPositionLength(curPosLen);
          offsetAtt.setOffset(tokStart, tokEnd);
          curGramSize++;
          return true;
        }
      } else {
        while (curGramSize <= maxGram) {
          while (curPos+curGramSize <= curTermLength) {     // while there is input
            clearAttributes();
            termAtt.copyBuffer(curTermBuffer, curPos, curGramSize);
            if (hasIllegalOffsets) {
              offsetAtt.setOffset(tokStart, tokEnd);
            } else {
              offsetAtt.setOffset(tokStart + curPos, tokStart + curPos + curGramSize);
            }
            curPos++;
            return true;
          }
          curGramSize++;                         // increase n-gram size
          curPos = 0;
        }
      }
      curTermBuffer = null;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    curTermBuffer = null;
  }
}
