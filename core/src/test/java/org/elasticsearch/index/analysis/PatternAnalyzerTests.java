package org.elasticsearch.index.analysis;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Verifies the behavior of PatternAnalyzer.
 */
public class PatternAnalyzerTests extends ESTokenStreamTestCase {

  /**
   * Test PatternAnalyzer when it is configured with a non-word pattern.
   */
  public void testNonWordPattern() throws IOException {
    // Split on non-letter pattern, do not lowercase, no stopwords
    PatternAnalyzer a = new PatternAnalyzer(Pattern.compile("\\W+"), false, null);
    assertAnalyzesTo(a, "The quick brown Fox,the abcd1234 (56.78) dc.",
                        new String[] { "The", "quick", "brown", "Fox", "the", "abcd1234", "56", "78", "dc" });

    // split on non-letter pattern, lowercase, english stopwords
    PatternAnalyzer b = new PatternAnalyzer(Pattern.compile("\\W+"), true,
                                            StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    assertAnalyzesTo(b, "The quick brown Fox,the abcd1234 (56.78) dc.",
                         new String[] { "quick", "brown", "fox", "abcd1234", "56", "78", "dc" });
  }

  /**
   * Test PatternAnalyzer when it is configured with a whitespace pattern.
   * Behavior can be similar to WhitespaceAnalyzer (depending upon options)
   */
  public void testWhitespacePattern() throws IOException {
    // Split on whitespace patterns, do not lowercase, no stopwords
    PatternAnalyzer a = new PatternAnalyzer(Pattern.compile("\\s+"), false, null);
    assertAnalyzesTo(a, "The quick brown Fox,the abcd1234 (56.78) dc.",
                        new String[] { "The", "quick", "brown", "Fox,the", "abcd1234", "(56.78)", "dc." });

    // Split on whitespace patterns, lowercase, english stopwords
    PatternAnalyzer b = new PatternAnalyzer(Pattern.compile("\\s+"), true,
                                            StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    assertAnalyzesTo(b, "The quick brown Fox,the abcd1234 (56.78) dc.",
                         new String[] { "quick", "brown", "fox,the", "abcd1234", "(56.78)", "dc." });
  }

  /**
   * Test PatternAnalyzer when it is configured with a custom pattern. In this
   * case, text is tokenized on the comma ","
   */
  public void testCustomPattern() throws IOException {
    // Split on comma, do not lowercase, no stopwords
    PatternAnalyzer a = new PatternAnalyzer(Pattern.compile(","), false, null);
    assertAnalyzesTo(a, "Here,Are,some,Comma,separated,words,",
                         new String[] { "Here", "Are", "some", "Comma", "separated", "words" });

    // split on comma, lowercase, english stopwords
    PatternAnalyzer b = new PatternAnalyzer(Pattern.compile(","), true,
                                             StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    assertAnalyzesTo(b, "Here,Are,some,Comma,separated,words,",
                         new String[] { "here", "some", "comma", "separated", "words" });
  }

  /**
   * Test PatternAnalyzer against a large document.
   */
  public void testHugeDocument() throws IOException {
    StringBuilder document = new StringBuilder();
    // 5000 a's
    char largeWord[] = new char[5000];
    Arrays.fill(largeWord, 'a');
    document.append(largeWord);

    // a space
    document.append(' ');

    // 2000 b's
    char largeWord2[] = new char[2000];
    Arrays.fill(largeWord2, 'b');
    document.append(largeWord2);

    // Split on whitespace patterns, do not lowercase, no stopwords
    PatternAnalyzer a = new PatternAnalyzer(Pattern.compile("\\s+"), false, null);
    assertAnalyzesTo(a, document.toString(),
                         new String[] { new String(largeWord), new String(largeWord2) });
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new PatternAnalyzer(Pattern.compile(","), true, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    checkRandomData(random(), a, 10000*RANDOM_MULTIPLIER);
  }

  public void testNormalize() {
      PatternAnalyzer a = new PatternAnalyzer(Pattern.compile("\\s+"), false, null);
      assertEquals(new BytesRef("FooBar"), a.normalize("dummy", "FooBar"));
      a = new PatternAnalyzer(Pattern.compile("\\s+"), true, null);
      assertEquals(new BytesRef("foobar"), a.normalize("dummy", "FooBar"));
  }
}
