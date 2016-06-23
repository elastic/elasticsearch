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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.ar.ArabicNormalizationFilterFactory;
import org.apache.lucene.analysis.ar.ArabicStemFilterFactory;
import org.apache.lucene.analysis.br.BrazilianStemFilterFactory;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.cjk.CJKWidthFilterFactory;
import org.apache.lucene.analysis.ckb.SoraniNormalizationFilterFactory;
import org.apache.lucene.analysis.commongrams.CommonGramsFilterFactory;
import org.apache.lucene.analysis.core.DecimalDigitFilterFactory;
import org.apache.lucene.analysis.core.LetterTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.UpperCaseFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.cz.CzechStemFilterFactory;
import org.apache.lucene.analysis.de.GermanNormalizationFilterFactory;
import org.apache.lucene.analysis.en.KStemFilterFactory;
import org.apache.lucene.analysis.en.PorterStemFilterFactory;
import org.apache.lucene.analysis.fa.PersianNormalizationFilterFactory;
import org.apache.lucene.analysis.hi.HindiNormalizationFilterFactory;
import org.apache.lucene.analysis.in.IndicNormalizationFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LengthFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ScandinavianFoldingFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilterFactory;
import org.apache.lucene.analysis.miscellaneous.TrimFilterFactory;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilterFactory;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.ngram.EdgeNGramFilterFactory;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizerFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;
import org.apache.lucene.analysis.path.PathHierarchyTokenizerFactory;
import org.apache.lucene.analysis.pattern.PatternTokenizerFactory;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilterFactory;
import org.apache.lucene.analysis.reverse.ReverseStringFilterFactory;
import org.apache.lucene.analysis.shingle.ShingleFilterFactory;
import org.apache.lucene.analysis.standard.ClassicFilterFactory;
import org.apache.lucene.analysis.standard.ClassicTokenizerFactory;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizerFactory;
import org.apache.lucene.analysis.th.ThaiTokenizerFactory;
import org.apache.lucene.analysis.tr.ApostropheFilterFactory;
import org.apache.lucene.analysis.util.ElisionFilterFactory;
import org.elasticsearch.AnalysisFactoryTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.indices.analysis.PreBuiltCharFilters;
import org.elasticsearch.indices.analysis.PreBuiltTokenFilters;
import org.elasticsearch.indices.analysis.PreBuiltTokenizers;

import java.util.Map;

public class AnalysisFactoryTests extends AnalysisFactoryTestCase {

    static final Map<PreBuiltTokenizers,Class<?>> KNOWN_TOKENIZERS
        = new MapBuilder<PreBuiltTokenizers,Class<?>>()
            .put(PreBuiltTokenizers.CLASSIC, ClassicTokenizerFactory.class)
            .put(PreBuiltTokenizers.EDGE_NGRAM, EdgeNGramTokenizerFactory.class)
            .put(PreBuiltTokenizers.KEYWORD, org.apache.lucene.analysis.core.KeywordTokenizerFactory.class)
            .put(PreBuiltTokenizers.LETTER, LetterTokenizerFactory.class)
            .put(PreBuiltTokenizers.LOWERCASE, Void.class)
            .put(PreBuiltTokenizers.NGRAM, NGramTokenizerFactory.class)
            .put(PreBuiltTokenizers.PATH_HIERARCHY, PathHierarchyTokenizerFactory.class)
            .put(PreBuiltTokenizers.PATTERN, PatternTokenizerFactory.class)
            .put(PreBuiltTokenizers.STANDARD, StandardTokenizerFactory.class)
            .put(PreBuiltTokenizers.THAI, ThaiTokenizerFactory.class)
            .put(PreBuiltTokenizers.UAX_URL_EMAIL, UAX29URLEmailTokenizerFactory.class)
            .put(PreBuiltTokenizers.WHITESPACE, WhitespaceTokenizerFactory.class)
            .immutableMap();

    static final Map<PreBuiltCharFilters,Class<?>> KNOWN_CHAR_FILTERS
        = new MapBuilder<PreBuiltCharFilters,Class<?>>()
            .put(PreBuiltCharFilters.HTML_STRIP, HTMLStripCharFilterFactory.class)
            .immutableMap();

    static final Map<PreBuiltTokenFilters,Class<?>> KNOWN_TOKEN_FILTERS
        = new MapBuilder<PreBuiltTokenFilters,Class<?>>()
            .put(PreBuiltTokenFilters.APOSTROPHE, ApostropheFilterFactory.class)
            .put(PreBuiltTokenFilters.ARABIC_NORMALIZATION, ArabicNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.ARABIC_STEM, ArabicStemFilterFactory.class)
            .put(PreBuiltTokenFilters.ASCIIFOLDING, ASCIIFoldingFilterFactory.class)
            .put(PreBuiltTokenFilters.BRAZILIAN_STEM, BrazilianStemFilterFactory.class)
            .put(PreBuiltTokenFilters.CJK_BIGRAM, org.apache.lucene.analysis.cjk.CJKBigramFilterFactory.class)
            .put(PreBuiltTokenFilters.CJK_WIDTH, CJKWidthFilterFactory.class)
            .put(PreBuiltTokenFilters.CLASSIC, ClassicFilterFactory.class)
            .put(PreBuiltTokenFilters.COMMON_GRAMS, CommonGramsFilterFactory.class)
            .put(PreBuiltTokenFilters.CZECH_STEM, CzechStemFilterFactory.class)
            .put(PreBuiltTokenFilters.DECIMAL_DIGIT, DecimalDigitFilterFactory.class)
            .put(PreBuiltTokenFilters.DELIMITED_PAYLOAD_FILTER, DelimitedPayloadTokenFilterFactory.class)
            .put(PreBuiltTokenFilters.DUTCH_STEM, Void.class) // no Lucene factory
            .put(PreBuiltTokenFilters.EDGE_NGRAM, EdgeNGramFilterFactory.class)
            .put(PreBuiltTokenFilters.ELISION, ElisionFilterFactory.class)
            .put(PreBuiltTokenFilters.FRENCH_STEM, Void.class) // no Lucene factory
            .put(PreBuiltTokenFilters.GERMAN_NORMALIZATION, GermanNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.GERMAN_STEM, Void.class) // no Lucene factory
            .put(PreBuiltTokenFilters.HINDI_NORMALIZATION, HindiNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.INDIC_NORMALIZATION, IndicNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.KEYWORD_REPEAT, KeywordRepeatFilterFactory.class)
            .put(PreBuiltTokenFilters.KSTEM, KStemFilterFactory.class)
            .put(PreBuiltTokenFilters.LENGTH, LengthFilterFactory.class)
            .put(PreBuiltTokenFilters.LIMIT, LimitTokenCountFilterFactory.class)
            .put(PreBuiltTokenFilters.LOWERCASE, LowerCaseFilterFactory.class)
            .put(PreBuiltTokenFilters.NGRAM, NGramFilterFactory.class)
            .put(PreBuiltTokenFilters.PERSIAN_NORMALIZATION, PersianNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.PORTER_STEM, PorterStemFilterFactory.class)
            .put(PreBuiltTokenFilters.REVERSE, ReverseStringFilterFactory.class)
            .put(PreBuiltTokenFilters.RUSSIAN_STEM, Void.class)
            .put(PreBuiltTokenFilters.SCANDINAVIAN_FOLDING, ScandinavianFoldingFilterFactory.class)
            .put(PreBuiltTokenFilters.SCANDINAVIAN_NORMALIZATION, ScandinavianNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.SHINGLE, ShingleFilterFactory.class)
            .put(PreBuiltTokenFilters.SNOWBALL, Void.class) // no Lucene factory
            .put(PreBuiltTokenFilters.SORANI_NORMALIZATION, SoraniNormalizationFilterFactory.class)
            .put(PreBuiltTokenFilters.STANDARD, StandardFilterFactory.class)
            .put(PreBuiltTokenFilters.STEMMER, PorterStemFilterFactory.class)
            .put(PreBuiltTokenFilters.STOP, StopFilterFactory.class)
            .put(PreBuiltTokenFilters.TRIM, TrimFilterFactory.class)
            .put(PreBuiltTokenFilters.TRUNCATE, TruncateTokenFilterFactory.class)
            .put(PreBuiltTokenFilters.TYPE_AS_PAYLOAD, TypeAsPayloadTokenFilterFactory.class)
            .put(PreBuiltTokenFilters.UNIQUE, Void.class) // no Lucene factory
            .put(PreBuiltTokenFilters.UPPERCASE, UpperCaseFilterFactory.class)
            .put(PreBuiltTokenFilters.WORD_DELIMITER, WordDelimiterFilterFactory.class)
            .immutableMap();

    public void testPrebuiltTokenizers() {
        for (PreBuiltTokenizers tokenizer : PreBuiltTokenizers.values()) {
            Class<?> luceneFactory = KNOWN_TOKENIZERS.get(tokenizer);
            assertNotNull("Add " + tokenizer + " to KNOWN_TOKENIZERS", luceneFactory);
            if (Void.class.equals(luceneFactory)) {
                continue;
            }
            assertTrue(
                    "Not a Lucene factory for " + tokenizer,
                    org.apache.lucene.analysis.util.TokenizerFactory.class.isAssignableFrom(luceneFactory));
            TokenizerFactory factory = tokenizer.getTokenizerFactory(Version.CURRENT);
            assertEquals("Wrong multi-term behaviour for " + tokenizer,
                    org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(luceneFactory),
                    factory instanceof MultiTermAwareComponent);
        }
    }

    public void testPrebuiltCharFilters() {
        for (PreBuiltCharFilters charFilter : PreBuiltCharFilters.values()) {
            Class<?> luceneFactory = KNOWN_CHAR_FILTERS.get(charFilter);
            assertNotNull("Add " + charFilter + " to KNOWN_CHAR_FILTERS", luceneFactory);
            if (Void.class.equals(luceneFactory)) {
                continue;
            }
            assertTrue(
                    "Not a Lucene factory for " + charFilter,
                    org.apache.lucene.analysis.util.CharFilterFactory.class.isAssignableFrom(luceneFactory));
            CharFilterFactory factory = charFilter.getCharFilterFactory(Version.CURRENT);
            assertEquals("Wrong multi-term behaviour for " + charFilter,
                    org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(luceneFactory),
                    factory instanceof MultiTermAwareComponent);
        }
    }

    public void testPrebuiltTokenFilterFactories() {
        for (PreBuiltTokenFilters tokenFilter : PreBuiltTokenFilters.values()) {
            Class<?> luceneFactory = KNOWN_TOKEN_FILTERS.get(tokenFilter);
            assertNotNull("Add " + tokenFilter + " to KNOWN_TOKEN_FILTERS", luceneFactory);
            if (Void.class.equals(luceneFactory)) {
                continue;
            }
            assertTrue(
                    "Not a Lucene factory for " + tokenFilter,
                    org.apache.lucene.analysis.util.TokenFilterFactory.class.isAssignableFrom(luceneFactory));
            TokenFilterFactory factory = tokenFilter.getTokenFilterFactory(Version.CURRENT);
            assertEquals("Wrong multi-term behaviour for " + tokenFilter,
                    org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(luceneFactory),
                    factory instanceof MultiTermAwareComponent);
        }
    }

}
