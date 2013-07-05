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

import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.util.XCharacterUtils;
import org.apache.lucene.util.Version;

/**
 * Tokenizes the input into n-grams of the given size(s).
 * <a name="version"/>
 * <p>You must specify the required {@link Version} compatibility when
 * creating a {@link XNGramTokenFilter}. As of Lucene 4.4, this token filters:<ul>
 * <li>handles supplementary characters correctly,</li>
 * <li>emits all n-grams for the same token at the same position,</li>
 * <li>does not modify offsets,</li>
 * <li>sorts n-grams by their offset in the original token first, then
 * increasing length (meaning that "abc" will give "a", "ab", "abc", "b", "bc",
 * "c").</li></ul>
 * <p>You can make this filter use the old behavior by providing a version &lt;
 * {@link Version#LUCENE_44} in the constructor but this is not recommended as
 * it will lead to broken {@link TokenStream}s that will cause highlighting
 * bugs.
 * <p>If you were using this {@link TokenFilter} to perform partial highlighting,
 * this won't work anymore since this filter doesn't update offsets. You should
 * modify your analysis chain to use {@link NGramTokenizer}, and potentially
 * override {@link NGramTokenizer#isTokenChar(int)} to perform pre-tokenization.
 */
public final class XNGramTokenFilter extends TokenFilter {

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1492640.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;

  private final int minGram, maxGram;

  private char[] curTermBuffer;
  private int curTermLength;
  private int curCodePointCount;
  private int curGramSize;
  private int curPos;
  private int curPosInc, curPosLen;
  private int tokStart;
  private int tokEnd;

  private final XCharacterUtils charUtils;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt;
  private final PositionLengthAttribute posLenAtt;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates XNGramTokenFilter with given min and max n-grams.
   * @param version Lucene version to enable correct position increments.
   *                See <a href="#version">above</a> for details.
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XNGramTokenFilter(Version version, TokenStream input, int minGram, int maxGram) {
    super(new LengthFilter(true, input, minGram, Integer.MAX_VALUE));
    this.charUtils = XCharacterUtils.getInstance(version);
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    posIncAtt = addAttribute(PositionIncrementAttribute.class);
    posLenAtt = addAttribute(PositionLengthAttribute.class);
  }

  /**
   * Creates XNGramTokenFilter with default min and max n-grams.
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
          curCodePointCount = charUtils.codePointCount(termAtt);
          curGramSize = minGram;
          curPos = 0;
          curPosInc = posIncAtt.getPositionIncrement();
          curPosLen = posLenAtt.getPositionLength();
          tokStart = offsetAtt.startOffset();
          tokEnd = offsetAtt.endOffset();
        }
      }
        if (curGramSize > maxGram || (curPos + curGramSize) > curCodePointCount) {
          ++curPos;
          curGramSize = minGram;
        }
        if ((curPos + curGramSize) <= curCodePointCount) {
          clearAttributes();
          final int start = charUtils.offsetByCodePoints(curTermBuffer, 0, curTermLength, 0, curPos);
          final int end = charUtils.offsetByCodePoints(curTermBuffer, 0, curTermLength, start, curGramSize);
          termAtt.copyBuffer(curTermBuffer, start, end - start);
          posIncAtt.setPositionIncrement(curPosInc);
          curPosInc = 0;
          posLenAtt.setPositionLength(curPosLen);
          offsetAtt.setOffset(tokStart, tokEnd);
          curGramSize++;
          return true;
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
