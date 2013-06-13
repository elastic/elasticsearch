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

import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * Old broken version of {@link NGramTokenizer}.
 */
@Deprecated
public final class XLucene43NGramTokenizer extends Tokenizer {

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1492640.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;

  private int minGram, maxGram;
  private int gramSize;
  private int pos;
  private int inLen; // length of the input AFTER trim()
  private int charsRead; // length of the input
  private String inStr;
  private boolean started;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XLucene43NGramTokenizer(Reader input, int minGram, int maxGram) {
    super(input);
    init(minGram, maxGram);
  }

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XLucene43NGramTokenizer(AttributeFactory factory, Reader input, int minGram, int maxGram) {
    super(factory, input);
    init(minGram, maxGram);
  }

  /**
   * Creates NGramTokenizer with default min and max n-grams.
   * @param input {@link Reader} holding the input to be tokenized
   */
  public XLucene43NGramTokenizer(Reader input) {
    this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE);
  }

  private void init(int minGram, int maxGram) {
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    if (!started) {
      started = true;
      gramSize = minGram;
      char[] chars = new char[1024];
      charsRead = 0;
      // TODO: refactor to a shared readFully somewhere:
      while (charsRead < chars.length) {
        int inc = input.read(chars, charsRead, chars.length-charsRead);
        if (inc == -1) {
          break;
        }
        charsRead += inc;
      }
      inStr = new String(chars, 0, charsRead).trim();  // remove any trailing empty strings

      if (charsRead == chars.length) {
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
    }

    if (pos+gramSize > inLen) {            // if we hit the end of the string
      pos = 0;                           // reset to beginning of string
      gramSize++;                        // increase n-gram size
      if (gramSize > maxGram)            // we are done
        return false;
      if (pos+gramSize > inLen)
        return false;
    }

    int oldPos = pos;
    pos++;
    termAtt.setEmpty().append(inStr, oldPos, oldPos+gramSize);
    offsetAtt.setOffset(correctOffset(oldPos), correctOffset(oldPos+gramSize));
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
    pos = 0;
  }
}
