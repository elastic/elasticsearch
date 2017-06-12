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

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.en.PorterStemFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilterFactory;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory;
import org.apache.lucene.analysis.reverse.ReverseStringFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.elasticsearch.index.analysis.HtmlStripCharFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class CommonAnalysisFactoryTests extends AnalysisFactoryTestCase {
    public CommonAnalysisFactoryTests() {
        super(new CommonAnalysisPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new TreeMap<>(super.getTokenizers());
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getTokenFilters());
        filters.put("asciifolding",          ASCIIFoldingTokenFilterFactory.class);
        filters.put("keywordmarker",         KeywordMarkerTokenFilterFactory.class);
        filters.put("porterstem",            PorterStemTokenFilterFactory.class);
        filters.put("snowballporter",        SnowballTokenFilterFactory.class);
        filters.put("trim",                  TrimTokenFilterFactory.class);
        filters.put("worddelimiter",         WordDelimiterTokenFilterFactory.class);
        filters.put("worddelimitergraph",    WordDelimiterGraphTokenFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getCharFilters());
        filters.put("htmlstrip",      HtmlStripCharFilterFactory.class);
        filters.put("mapping",        MappingCharFilterFactory.class);
        filters.put("patternreplace", PatternReplaceCharFilterFactory.class);

        // TODO: these charfilters are not yet exposed: useful?
        // handling of zwnj for persian
        filters.put("persian",        Void.class);
        return filters;
    }

    @Override
    public Map<String, Class<?>> getPreConfiguredCharFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getPreConfiguredCharFilters());
        filters.put("html_strip", HTMLStripCharFilterFactory.class);
        filters.put("htmlStrip", HTMLStripCharFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getPreConfiguredTokenFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getPreConfiguredTokenFilters());
        filters.put("apostrophe", null);
        filters.put("arabic_normalization", null);
        filters.put("arabic_stem", null);
        filters.put("asciifolding", null);
        filters.put("brazilian_stem", null);
        filters.put("cjk_bigram", null);
        filters.put("cjk_width", null);
        filters.put("classic", null);
        filters.put("common_grams", null);
        filters.put("czech_stem", null);
        filters.put("decimal_digit", null);
        filters.put("delimited_payload_filter", DelimitedPayloadTokenFilterFactory.class);
        filters.put("dutch_stem", SnowballPorterFilterFactory.class);
        filters.put("edge_ngram", null);
        filters.put("edgeNGram", null);
        filters.put("elision", null);
        filters.put("french_stem", SnowballPorterFilterFactory.class);
        filters.put("german_stem", null);
        filters.put("german_normalization", null);
        filters.put("hindi_normalization", null);
        filters.put("indic_normalization", null);
        filters.put("keyword_repeat", null);
        filters.put("kstem", null);
        filters.put("length", null);
        filters.put("limit", LimitTokenCountFilterFactory.class);
        filters.put("ngram", null);
        filters.put("nGram", null);
        filters.put("persian_normalization", null);
        filters.put("porter_stem", null);
        filters.put("reverse", ReverseStringFilterFactory.class);
        filters.put("russian_stem", SnowballPorterFilterFactory.class);
        filters.put("scandinavian_normalization", null);
        filters.put("scandinavian_folding", null);
        filters.put("shingle", null);
        filters.put("snowball", SnowballPorterFilterFactory.class);
        filters.put("sorani_normalization", null);
        filters.put("stemmer", PorterStemFilterFactory.class);
        filters.put("stop", null);
        filters.put("trim", null);
        filters.put("truncate", null);
        filters.put("type_as_payload", null);
        filters.put("unique", Void.class);
        filters.put("uppercase", null);
        filters.put("word_delimiter", null);
        filters.put("word_delimiter_graph", null);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getPreConfiguredTokenizers() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getPreConfiguredTokenizers());
        filters.put("lowercase", null);
        return filters;
    }

    /**
     * Fails if a tokenizer is marked in the superclass with {@link MovedToAnalysisCommon} but
     * hasn't been marked in this class with its proper factory.
     */
    public void testAllTokenizersMarked() {
        markedTestCase("char filter", getTokenizers());
    }

    /**
     * Fails if a char filter is marked in the superclass with {@link MovedToAnalysisCommon} but
     * hasn't been marked in this class with its proper factory.
     */
    public void testAllCharFiltersMarked() {
        markedTestCase("char filter", getCharFilters());
    }

    /**
     * Fails if a char filter is marked in the superclass with {@link MovedToAnalysisCommon} but
     * hasn't been marked in this class with its proper factory.
     */
    public void testAllTokenFiltersMarked() {
        markedTestCase("token filter", getTokenFilters());
    }

    private void markedTestCase(String name, Map<String, Class<?>> map) {
        List<String> unmarked = map.entrySet().stream()
                .filter(e -> e.getValue() == MovedToAnalysisCommon.class)
                .map(Map.Entry::getKey)
                .sorted()
                .collect(toList());
        assertEquals(name + " marked in AnalysisFactoryTestCase as moved to analysis-common "
                + "but not mapped here", emptyList(), unmarked);
    }
}
