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

import java.io.Reader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

/**
 * Tokenizes the input from an edge into n-grams of given size(s).
 * <p>
 * This {@link Tokenizer} create n-grams from the beginning edge or ending edge of a input token.
 * <p><a name="version" /> As of Lucene 4.4, this tokenizer<ul>
 * <li>can handle <code>maxGram</code> larger than 1024 chars, but beware that this will result in increased memory usage
 * <li>doesn't trim the input,
 * <li>sets position increments equal to 1 instead of 1 for the first token and 0 for all other ones
 * <li>doesn't support backward n-grams anymore.
 * <li>supports {@link #isTokenChar(int) pre-tokenization},
 * <li>correctly handles supplementary characters.
 * </ul>
 * <p>Although <b style="color:red">highly</b> discouraged, it is still possible
 * to use the old behavior through {@link Lucene43XEdgeXNGramTokenizer}.
 */
public class XEdgeNGramTokenizer extends XNGramTokenizer {

    static {
        // LUCENE MONITOR: this should be in Lucene 4.4 copied from Revision: 1492640.
        assert Lucene.VERSION == Version.LUCENE_43 : "Elasticsearch has upgraded to Lucene Version: [" + Lucene.VERSION + "] this class should be removed";
    }

  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  /**
   * Creates XEdgeXNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XEdgeNGramTokenizer(Version version, Reader input, int minGram, int maxGram) {
    super(version, input, minGram, maxGram, true);
  }

  /**
   * Creates XEdgeXNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the <a href="#version">Lucene match version</a>
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public XEdgeNGramTokenizer(Version version, AttributeFactory factory, Reader input, int minGram, int maxGram) {
    super(version, factory, input, minGram, maxGram, true);
  }

}
