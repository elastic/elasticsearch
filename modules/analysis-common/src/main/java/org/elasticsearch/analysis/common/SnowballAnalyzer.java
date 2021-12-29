/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;

/** Filters {@link StandardTokenizer} with {@link
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
    SnowballAnalyzer(String name) {
        this.name = name;
    }

    /** Builds the named analyzer with the given stop words. */
    SnowballAnalyzer(String name, CharArraySet stopWords) {
        this(name);
        stopSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stopWords));
    }

    /** Constructs a {@link StandardTokenizer} filtered by a {@link LowerCaseFilter}, a {@link StopFilter},
      and a {@link SnowballFilter} */
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer tokenizer = new StandardTokenizer();
        TokenStream result = tokenizer;
        // remove the possessive 's for english stemmers
        if (name.equals("English") || name.equals("Porter") || name.equals("Lovins")) result = new EnglishPossessiveFilter(result);
        // Use a special lowercase filter for turkish, the stemmer expects it.
        if (name.equals("Turkish")) result = new TurkishLowerCaseFilter(result);
        else result = new LowerCaseFilter(result);
        if (stopSet != null) result = new StopFilter(result, stopSet);
        result = new SnowballFilter(result, name);
        return new TokenStreamComponents(tokenizer, result);
    }
}
