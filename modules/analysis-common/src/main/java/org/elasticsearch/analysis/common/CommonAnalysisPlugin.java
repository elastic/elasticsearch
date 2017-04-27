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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter;
import org.apache.lucene.analysis.miscellaneous.UniqueTokenFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.HtmlStripCharFilterFactory;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.plugins.AnalysisPlugin.requriesAnalysisSettings;

public class CommonAnalysisPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> filters = new TreeMap<>();
        filters.put("asciifolding", ASCIIFoldingTokenFilterFactory::new);
        filters.put("word_delimiter", WordDelimiterTokenFilterFactory::new);
        filters.put("word_delimiter_graph", WordDelimiterGraphTokenFilterFactory::new);
        return filters;
    }

    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        Map<String, AnalysisProvider<CharFilterFactory>> filters = new TreeMap<>();
        filters.put("html_strip", HtmlStripCharFilterFactory::new);
        filters.put("pattern_replace", requriesAnalysisSettings(PatternReplaceCharFilterFactory::new));
        filters.put("mapping", requriesAnalysisSettings(MappingCharFilterFactory::new));
        return filters;
    }

    @Override
    public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        // TODO we should revisit the caching strategies.
        List<PreConfiguredTokenFilter> filters = new ArrayList<>();
        filters.add(new PreConfiguredTokenFilter("asciifolding", true, CachingStrategy.ONE, input -> new ASCIIFoldingFilter(input)));
        filters.add(new PreConfiguredTokenFilter("classic", false, CachingStrategy.ONE, ClassicFilter::new));
        filters.add(new PreConfiguredTokenFilter("common_grams", false, CachingStrategy.LUCENE, input ->
                new CommonGramsFilter(input, CharArraySet.EMPTY_SET)));
        filters.add(new PreConfiguredTokenFilter("edge_ngram", false, CachingStrategy.LUCENE, input ->
                new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE)));
        // TODO deprecate edgeNGram
        filters.add(new PreConfiguredTokenFilter("edgeNGram", false, CachingStrategy.LUCENE, input ->
                new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE)));
        filters.add(new PreConfiguredTokenFilter("kstem", false, CachingStrategy.ONE, KStemFilter::new));
        filters.add(new PreConfiguredTokenFilter("length", false, CachingStrategy.LUCENE, input ->
                new LengthFilter(input, 0, Integer.MAX_VALUE)));  // TODO this one seems useless
        filters.add(new PreConfiguredTokenFilter("ngram", false, CachingStrategy.LUCENE, NGramTokenFilter::new));
        // TODO deprecate nGram
        filters.add(new PreConfiguredTokenFilter("nGram", false, CachingStrategy.LUCENE, NGramTokenFilter::new));
        filters.add(new PreConfiguredTokenFilter("porter_stem", false, CachingStrategy.ONE, PorterStemFilter::new));
        filters.add(new PreConfiguredTokenFilter("reverse", false, CachingStrategy.LUCENE, input -> new ReverseStringFilter(input)));
        // The stop filter is in lucene-core but the English stop words set is in lucene-analyzers-common
        filters.add(new PreConfiguredTokenFilter("stop", false, CachingStrategy.LUCENE, input ->
                new StopFilter(input, StopAnalyzer.ENGLISH_STOP_WORDS_SET)));
        filters.add(new PreConfiguredTokenFilter("trim", false, CachingStrategy.LUCENE, TrimFilter::new));
        filters.add(new PreConfiguredTokenFilter("truncate", false, CachingStrategy.ONE, input ->
                new TruncateTokenFilter(input, 10)));
        filters.add(new PreConfiguredTokenFilter("unique", false, CachingStrategy.ONE, input -> new UniqueTokenFilter(input)));
        filters.add(new PreConfiguredTokenFilter("uppercase", true, CachingStrategy.LUCENE, UpperCaseFilter::new));
        filters.add(new PreConfiguredTokenFilter("word_delimiter", false, CachingStrategy.ONE, input ->
                new WordDelimiterFilter(input,
                        WordDelimiterFilter.GENERATE_WORD_PARTS
                      | WordDelimiterFilter.GENERATE_NUMBER_PARTS
                      | WordDelimiterFilter.SPLIT_ON_CASE_CHANGE
                      | WordDelimiterFilter.SPLIT_ON_NUMERICS
                      | WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE, null)));
        filters.add(new PreConfiguredTokenFilter("word_delimiter_graph", false, CachingStrategy.ONE, input ->
                new WordDelimiterGraphFilter(input,
                        WordDelimiterGraphFilter.GENERATE_WORD_PARTS
                      | WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS
                      | WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE
                      | WordDelimiterGraphFilter.SPLIT_ON_NUMERICS
                      | WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE, null)));
        return filters;
    }
}
