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
import java.io.Reader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Version;

/**
 * Old version of {@link EdgeNGramTokenizer} which doesn't handle correctly
 * supplementary characters.
 */
@Deprecated
public final class XLucene43EdgeNGramTokenizer extends Tokenizer {

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1492640.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

  public static final Side DEFAULT_SIDE = Side.FRONT;
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  /** Specifies which side of the input the n-gram should be generated from */
  public static enum Side {

    /** Get the n-gram from the front of the input */
    FRONT {
      @Override
      public String getLabel() { return "front"; }
    },

    /** Get the n-gram from the end of the input */
    BACK  {
      @Override
      public String getLabel() { return "back"; }
    };

    public abstract String getLabel();

    // Get the appropriate Side from a string
    public static Side getSide(String sideName) {
      if (FRONT.getLabel().equals(sideName)) {
        return FRONT;
      }
      if (BACK.getLabel().equals(sideName)) {
        return BACK;
      }
      return null;
    }
  }

  private int minGram;
  private int maxGram;
  private int gramSize;
  private Side side;
  private boolean started;
  private int inLen; // length of the input AFTER trim()
  private int charsRead; // length of the input
  private String inStr;


  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param input {@link Reader} holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  @Deprecated
  public XLucene43EdgeNGramTokenizer(Version version, Reader input, Side side, int minGram, int maxGram) {
    super(input);
    init(version, side, minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  @Deprecated
  public XLucene43EdgeNGramTokenizer(Version version, AttributeFactory factory, Reader input, Side side, int minGram, int maxGram) {
    super(factory, input);
    init(version, side, minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param input {@link Reader} holding the input to be tokenized
   * @param sideLabel the name of the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  @Deprecated
  public XLucene43EdgeNGramTokenizer(Version version, Reader input, String sideLabel, int minGram, int maxGram) {
    this(version, input, Side.getSide(sideLabel), minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param sideLabel the name of the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  @Deprecated
  public XLucene43EdgeNGramTokenizer(Version version, AttributeFactory factory, Reader input, String sideLabel, int minGram, int maxGram) {
    this(version, factory, input, Side.getSide(sideLabel), minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XLucene43EdgeNGramTokenizer(Version version, Reader input, int minGram, int maxGram) {
    this(version, input, Side.FRONT, minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XLucene43EdgeNGramTokenizer(Version version, AttributeFactory factory, Reader input, int minGram, int maxGram) {
    this(version, factory, input, Side.FRONT, minGram, maxGram);
  }

  private void init(Version version, Side side, int minGram, int maxGram) {
    if (version == null) {
      throw new IllegalArgumentException("version must not be null");
    }

    if (side == null) {
      throw new IllegalArgumentException("sideLabel must be either front or back");
    }

    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }

    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }

    maxGram = Math.min(maxGram, 1024);

    this.minGram = minGram;
    this.maxGram = maxGram;
    this.side = side;
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    // if we are just starting, read the whole input
    if (!started) {
      started = true;
      gramSize = minGram;
      final int limit = side == Side.FRONT ? maxGram : 1024;
      char[] chars = new char[Math.min(1024, limit)];
      charsRead = 0;
      // TODO: refactor to a shared readFully somewhere:
      boolean exhausted = false;
      while (charsRead < limit) {
        final int inc = input.read(chars, charsRead, chars.length-charsRead);
        if (inc == -1) {
          exhausted = true;
          break;
        }
        charsRead += inc;
        if (charsRead == chars.length && charsRead < limit) {
          chars = ArrayUtil.grow(chars);
        }
      }

      inStr = new String(chars, 0, charsRead);
      inStr = inStr.trim();

      if (!exhausted) {
        // Read extra throwaway chars so that on end() we
        // report the correct offset:
        char[] throwaway = new char[1024];
        while(true) {
          final int inc = input.read(throwaway, 0, throwaway.length);
          if (inc == -1) {
            break;
          }
          charsRead += inc;
        }
      }

      inLen = inStr.length();
      if (inLen == 0) {
        return false;
      }
      posIncrAtt.setPositionIncrement(1);
    } else {
      posIncrAtt.setPositionIncrement(0);
    }

    // if the remaining input is too short, we can't generate any n-grams
    if (gramSize > inLen) {
      return false;
    }

    // if we have hit the end of our n-gram size range, quit
    if (gramSize > maxGram || gramSize > inLen) {
      return false;
    }

    // grab gramSize chars from front or back
    int start = side == Side.FRONT ? 0 : inLen - gramSize;
    int end = start + gramSize;
    termAtt.setEmpty().append(inStr, start, end);
    offsetAtt.setOffset(correctOffset(start), correctOffset(end));
    gramSize++;
    return true;
  }

  @Override
  public void end() {
    // set final offset
    final int finalOffset = correctOffset(charsRead);
    this.offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    started = false;
  }
}
