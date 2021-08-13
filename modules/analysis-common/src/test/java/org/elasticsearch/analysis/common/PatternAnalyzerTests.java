package org.elasticsearch.analysis.common;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
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
            EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
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
                                            EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
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
            EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
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
    Analyzer a = new PatternAnalyzer(Pattern.compile(","), true, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
    checkRandomData(random(), a, 10000*RANDOM_MULTIPLIER);
  }

  public void testNormalize() {
      PatternAnalyzer a = new PatternAnalyzer(Pattern.compile("\\s+"), false, null);
      assertEquals(new BytesRef("FooBar"), a.normalize("dummy", "FooBar"));
      a = new PatternAnalyzer(Pattern.compile("\\s+"), true, null);
      assertEquals(new BytesRef("foobar"), a.normalize("dummy", "FooBar"));
  }
}
