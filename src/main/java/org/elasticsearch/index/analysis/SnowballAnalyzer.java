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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.std40.StandardTokenizer40;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/** Filters {@link StandardTokenizer} with {@link StandardFilter}, {@link
 * LowerCaseFilter}, {@link StopFilter} and {@link SnowballFilter}.
 *
 * Available stemmers are listed in org.tartarus.snowball.ext.  The name of a
 * stemmer is the part of the class name before "Stemmer", e.g., the stemmer in
 * {@link org.tartarus.snowball.ext.EnglishStemmer} is named "English".

 * @deprecated (3.1) Use the language-specific analyzer in modules/analysis instead. 
 * This analyzer WAS removed in Lucene 5.0
 */
@Deprecated
public final class SnowballAnalyzer extends Analyzer {
  private String name;
  private CharArraySet stopSet;

  /** Builds the named analyzer with no stop words. */
  public SnowballAnalyzer(String name) {
    this.name = name;
  }

  /** Builds the named analyzer with the given stop words. */
  public SnowballAnalyzer(String name, CharArraySet stopWords) {
    this(name);
    stopSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stopWords));
  }

  /** Constructs a {@link StandardTokenizer} filtered by a {@link
      StandardFilter}, a {@link LowerCaseFilter}, a {@link StopFilter},
      and a {@link SnowballFilter} */
  @Override
  public TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer tokenizer;
    if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
      tokenizer = new StandardTokenizer();
    } else {
      tokenizer = new StandardTokenizer40();
    }
    TokenStream result = tokenizer;
    // remove the possessive 's for english stemmers
    if (name.equals("English") || name.equals("Porter") || name.equals("Lovins"))
      result = new EnglishPossessiveFilter(result);
    // Use a special lowercase filter for turkish, the stemmer expects it.
    if (name.equals("Turkish"))
      result = new TurkishLowerCaseFilter(result);
    else
      result = new LowerCaseFilter(result);
    if (stopSet != null)
      result = new StopFilter(result, stopSet);
    result = new SnowballFilter(result, name);
    return new TokenStreamComponents(tokenizer, result);
  }
}