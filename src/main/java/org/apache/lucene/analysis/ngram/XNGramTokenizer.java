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
import java.io.Reader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

/**
 * Tokenizes the input into n-grams of the given size(s).
 * <p>On the contrary to {@link NGramTokenFilter}, this class sets offsets so
 * that characters between startOffset and endOffset in the original stream are
 * the same as the term chars.
 * <p>For example, "abcde" would be tokenized as (minGram=2, maxGram=3):
 * <table>
 * <tr><th>Term</th><td>ab</td><td>abc</td><td>bc</td><td>bcd</td><td>cd</td><td>cde</td><td>de</td></tr>
 * <tr><th>Position increment</th><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td></tr>
 * <tr><th>Position length</th><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td><td>1</td></tr>
 * <tr><th>Offsets</th><td>[0,2[</td><td>[0,3[</td><td>[1,3[</td><td>[1,4[</td><td>[2,4[</td><td>[2,5[</td><td>[3,5[</td></tr>
 * </table>
 * <a name="version"/>
 * <p>Before Lucene 4.4, this class had a different behavior:<ul>
 * <li>It didn't support more than 1024 chars of input, the rest was trashed.</li>
 * <li>The last whitespaces of the 1024 chars block were trimmed.</li>
 * <li>Tokens were emitted in a different order (by increasing lengths).</li></ul>
 * <p>Although highly discouraged, it is still possible to use the old behavior
 * through {@link Lucene43NGramTokenizer}.
 */
public final class XNGramTokenizer extends Tokenizer {
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;
  
  static {
      // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1476563
      assert Lucene.VERSION.ordinal() < Version.LUCENE_42.ordinal()+2  : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this should can be removed"; 
  }

  private char[] buffer;
  private int bufferStart, bufferEnd; // remaining slice of the buffer
  private int offset;
  private int gramSize;
  private int minGram, maxGram;
  private boolean exhausted;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param version the lucene compatibility <a href="#version">version</a>
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XNGramTokenizer(Version version, Reader input, int minGram, int maxGram) {
    super(input);
    init(version, minGram, maxGram);
  }

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param version the lucene compatibility <a href="#version">version</a>
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XNGramTokenizer(Version version, AttributeFactory factory, Reader input, int minGram, int maxGram) {
    super(factory, input);
    init(version, minGram, maxGram);
  }

  /**
   * Creates NGramTokenizer with default min and max n-grams.
   * @param version the lucene compatibility <a href="#version">version</a>
   * @param input {@link Reader} holding the input to be tokenized
   */
  public XNGramTokenizer(Version version, Reader input) {
    this(version, input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE);
  }

  private void init(Version version, int minGram, int maxGram) {
    if (!version.onOrAfter(Version.LUCENE_42)) {
      throw new IllegalArgumentException("This class only works with Lucene 4.4+. To emulate the old (broken) behavior of NGramTokenizer, use Lucene43NGramTokenizer");
    }
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
    buffer = new char[maxGram + 1024];
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();

    // compact
    if (bufferStart >= buffer.length - maxGram) {
      System.arraycopy(buffer, bufferStart, buffer, 0, bufferEnd - bufferStart);
      bufferEnd -= bufferStart;
      bufferStart = 0;

      // fill in remaining space
      if (!exhausted) {
        // TODO: refactor to a shared readFully
        while (bufferEnd < buffer.length) {
          final int read = input.read(buffer, bufferEnd, buffer.length - bufferEnd);
          if (read == -1) {
            exhausted = true;
            break;
          }
          bufferEnd += read;
        }
      }
    }

    // should we go to the next offset?
    if (gramSize > maxGram || bufferStart + gramSize > bufferEnd) {
      bufferStart++;
      offset++;
      gramSize = minGram;
    }

    // are there enough chars remaining?
    if (bufferStart + gramSize > bufferEnd) {
      return false;
    }

    termAtt.copyBuffer(buffer, bufferStart, gramSize);
    posIncAtt.setPositionIncrement(1);
    posLenAtt.setPositionLength(1);
    offsetAtt.setOffset(correctOffset(offset), correctOffset(offset + gramSize));
    ++gramSize;
    return true;
  }

  @Override
  public void end() {
    final int endOffset = correctOffset(offset + bufferEnd - bufferStart);
    offsetAtt.setOffset(endOffset, endOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    bufferStart = bufferEnd = buffer.length;
    offset = 0;
    gramSize = minGram;
    exhausted = false;
  }
}
