/*
 * @notice
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
package org.elasticsearch.plugin.analysis.ukrainian;

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
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;

/**
 * A dictionary-based {@link Analyzer} for Ukrainian.
 *
 * A copy of UkrainianMorfologikAnalyzer from lucene 9.1.0 sources
 * with an added public method getDefaultStopSet.
 */
public final class XUkrainianMorfologikAnalyzer extends StopwordAnalyzerBase {
    private final Dictionary dictionary;
    private final CharArraySet stemExclusionSet;

    private static final NormalizeCharMap NORMALIZER_MAP;

    static {
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

        NORMALIZER_MAP = builder.build();
    }

    /** Returns a lazy singleton with the default Ukrainian resources. */
    private static volatile DefaultResources defaultResources;

    @SuppressForbidden(reason = "Lucene uses IOUtils")
    private static DefaultResources getDefaultResources() {
        if (defaultResources == null) {
            synchronized (DefaultResources.class) {
                try {
                    CharArraySet wordList;
                    try (var is = UkrainianMorfologikAnalyzer.class.getResourceAsStream("stopwords.txt")) {
                        if (is == null) {
                            throw new IOException("Could not locate the required stopwords resource.");
                        }
                        wordList = WordlistLoader.getSnowballWordSet(is);
                    }

                    // First, try to look up the resource module by name.
                    Dictionary dictionary;
                    Module ourModule = DefaultResources.class.getModule();
                    if (ourModule.isNamed() && ourModule.getLayer() != null) {
                        var module = ourModule.getLayer()
                            .findModule("morfologik.ukrainian.search")
                            .orElseThrow(() -> new IOException("Can't find the resource module: morfologik.ukrainian.search"));

                        try (
                            var fsaStream = module.getResourceAsStream("ua/net/nlp/ukrainian.dict");
                            var metaStream = module.getResourceAsStream("ua/net/nlp/ukrainian.info")
                        ) {
                            dictionary = Dictionary.read(fsaStream, metaStream);
                        }
                    } else {
                        var name = "ua/net/nlp/ukrainian.dict";
                        dictionary = Dictionary.read(
                            IOUtils.requireResourceNonNull(UkrainianMorfologikAnalyzer.class.getClassLoader().getResource(name), name)
                        );
                    }
                    defaultResources = new DefaultResources(wordList, dictionary);
                } catch (IOException e) {
                    throw new UncheckedIOException("Could not load the required resources for the Ukrainian analyzer.", e);
                }
            }
        }
        return defaultResources;
    }

    private static class DefaultResources {
        final CharArraySet stopSet;
        final Dictionary dictionary;

        private DefaultResources(CharArraySet stopSet, Dictionary dictionary) {
            this.stopSet = stopSet;
            this.dictionary = dictionary;
        }
    }

    /** Builds an analyzer with the default stop words. */
    public XUkrainianMorfologikAnalyzer() {
        this(getDefaultResources().stopSet);
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
     * Builds an analyzer with the given stop words. If a non-empty stem exclusion set is provided
     * this analyzer will add a {@link SetKeywordMarkerFilter} before stemming.
     *
     * @param stopwords a stopword set
     * @param stemExclusionSet a set of terms not to be stemmed
     */
    public XUkrainianMorfologikAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
        super(stopwords);
        this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
        this.dictionary = getDefaultResources().dictionary;
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        return new MappingCharFilter(NORMALIZER_MAP, reader);
    }

    /**
     * Creates a {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} which tokenizes all
     * the text in the provided {@link Reader}.
     *
     * @return A {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} built from an
     *     {@link StandardTokenizer} filtered with {@link LowerCaseFilter}, {@link StopFilter} ,
     *     {@link SetKeywordMarkerFilter} if a stem exclusion set is provided and {@link
     *     MorfologikFilter} on the Ukrainian dictionary.
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new StandardTokenizer();
        TokenStream result = new LowerCaseFilter(source);
        result = new StopFilter(result, stopwords);

        if (stemExclusionSet.isEmpty() == false) {
            result = new SetKeywordMarkerFilter(result, stemExclusionSet);
        }

        result = new MorfologikFilter(result, dictionary);
        return new TokenStreamComponents(source, result);
    }

    public static CharArraySet getDefaultStopSet() {
        return getDefaultResources().stopSet;
    }
}
