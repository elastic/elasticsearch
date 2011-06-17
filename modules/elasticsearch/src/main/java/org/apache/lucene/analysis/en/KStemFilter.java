package org.apache.lucene.analysis.en;

/**
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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/** A high-performance kstem filter for english.
 * <p/>
 * See <a href="http://ciir.cs.umass.edu/pubfiles/ir-35.pdf">
 * "Viewing Morphology as an Inference Process"</a>
 * (Krovetz, R., Proceedings of the Sixteenth Annual International ACM SIGIR
 * Conference on Research and Development in Information Retrieval, 191-203, 1993).
 * <p/>
 * All terms must already be lowercased for this filter to work correctly.
 */
// LUCENE MONITOR: Remove as of Lucene 3.3
public final class KStemFilter extends TokenFilter {
  private final KStemmer stemmer = new KStemmer();
  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);

  public KStemFilter(TokenStream in) {
    super(in);
  }

  /** Returns the next, stemmed, input Token.
   *  @return The stemed form of a token.
   *  @throws IOException
   */
  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken())
      return false;

    char[] term = termAttribute.buffer();
    int len = termAttribute.length();
    if ((!keywordAtt.isKeyword()) && stemmer.stem(term, len)) {
      termAttribute.setEmpty().append(stemmer.asCharSequence());
    }

    return true;
  }
}