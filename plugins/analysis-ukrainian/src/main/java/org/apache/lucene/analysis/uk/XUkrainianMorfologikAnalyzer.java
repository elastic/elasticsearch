/*@notice
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
package org.apache.lucene.analysis.uk;

import morfologik.stemming.Dictionary;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.morfologik.MorfologikFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

/**
 * A dictionary-based {@link Analyzer} for Ukrainian.
 *
 * Modified from lucene 8.8.0 sources to incorporate a bugfix for
 * https://issues.apache.org/jira/browse/LUCENE-9930
 */
public final class XUkrainianMorfologikAnalyzer extends StopwordAnalyzerBase {
    private final CharArraySet stemExclusionSet;

    /** File containing default Ukrainian stopwords. */
    public static final String DEFAULT_STOPWORD_FILE = "stopwords.txt";

    /**
     * Returns an unmodifiable instance of the default stop words set.
     * @return default stop words set.
     */
    public static CharArraySet getDefaultStopSet() {
        return DefaultSetHolder.DEFAULT_STOP_SET;
    }

    /**
     * Atomically loads the DEFAULT_STOP_SET and DICTIONARY in a lazy fashion once the outer class
     * accesses the static final set the first time.;
     */
    @SuppressForbidden(reason="Lucene uses IOUtils")
    private static class DefaultSetHolder {
        static final CharArraySet DEFAULT_STOP_SET;
        static final Dictionary DICTIONARY;

        static {
            try {
                DEFAULT_STOP_SET = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(UkrainianMorfologikAnalyzer.class,
                    DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
                DICTIONARY = Dictionary.read(
                    UkrainianMorfologikAnalyzer.class.getClassLoader().getResource("ua/net/nlp/ukrainian.dict"));
            } catch (IOException ex) {
                // default set should always be present as it is part of the
                // distribution (JAR)
                throw new RuntimeException("Unable to load resources", ex);
            }
        }
    }

    /**
     * Builds an analyzer with the default stop words: {@link #DEFAULT_STOPWORD_FILE}.
     */
    public XUkrainianMorfologikAnalyzer() {
        this(DefaultSetHolder.DEFAULT_STOP_SET);
    }

    /**
     * Builds an analyzer with the given stop words.
     *
     * @param stopwords a stopword set
     */
    public XUkrainianMorfologikAnalyzer(CharArraySet stopwords) {
        this(stopwords, CharArraySet.EMPTY_SET);
    }

    /**
     * Builds an analyzer with the given stop words. If a non-empty stem exclusion set is
     * provided this analyzer will add a {@link SetKeywordMarkerFilter} before
     * stemming.
     *
     * @param stopwords a stopword set
     * @param stemExclusionSet a set of terms not to be stemmed
     */
    public XUkrainianMorfologikAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
        super(stopwords);
        this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
        // different apostrophes
        builder.add("\u2019", "'");
        builder.add("\u2018", "'");
        builder.add("\u02BC", "'");
        builder.add("`", "'");
        builder.add("´", "'");
        // ignored characters
        builder.add("\u0301", "");
        builder.add("\u00AD", "");
        builder.add("ґ", "г");
        builder.add("Ґ", "Г");

        NormalizeCharMap normMap = builder.build();
        reader = new MappingCharFilter(normMap, reader);
        return reader;
    }

    /**
     * Creates a
     * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
     * which tokenizes all the text in the provided {@link Reader}.
     *
     * @return A
     *         {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
     *         built from an {@link StandardTokenizer} filtered with
     *         {@link LowerCaseFilter}, {@link StopFilter}
     *         , {@link SetKeywordMarkerFilter} if a stem exclusion set is
     *         provided and {@link MorfologikFilter} on the Ukrainian dictionary.
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new StandardTokenizer();
        TokenStream result = new LowerCaseFilter(source);
        result = new StopFilter(result, stopwords);

        if (stemExclusionSet.isEmpty() == false) {
            result = new SetKeywordMarkerFilter(result, stemExclusionSet);
        }

        result = new MorfologikFilter(result, DefaultSetHolder.DICTIONARY);
        return new TokenStreamComponents(source, result);
    }

}
