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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.test.ESTokenStreamTestCase;

public class SnowballAnalyzerTests extends ESTokenStreamTestCase {

  public void testEnglish() throws Exception {
    Analyzer a = new SnowballAnalyzer("English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
  }
  
  public void testStopwords() throws Exception {
    Analyzer a = new SnowballAnalyzer("English",
        StandardAnalyzer.STOP_WORDS_SET);
    assertAnalyzesTo(a, "the quick brown fox jumped",
        new String[]{"quick", "brown", "fox", "jump"});
  }
  
  /**
   * Test turkish lowercasing
   */
  public void testTurkish() throws Exception {
    Analyzer a = new SnowballAnalyzer("Turkish");

    assertAnalyzesTo(a, "ağacı", new String[] { "ağaç" });
    assertAnalyzesTo(a, "AĞACI", new String[] { "ağaç" });
  }

  
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new SnowballAnalyzer("English");
    assertAnalyzesTo(a, "he abhorred accents",
        new String[]{"he", "abhor", "accent"});
    assertAnalyzesTo(a, "she abhorred him",
        new String[]{"she", "abhor", "him"});
  }
}