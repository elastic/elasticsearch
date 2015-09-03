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

import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** 
 * Alerts us if new analyzers are added to lucene, so we don't miss them.
 * <p>
 * If we don't want to expose one for a specific reason, just map it to Void
 */
public class AnalysisFactoryTests extends ESTestCase {
    
    static final Map<String,Class<?>> KNOWN_TOKENIZERS = new HashMap<String,Class<?>>() {{
        // deprecated ones, we dont care about these
        put("arabicletter",  Deprecated.class);
        put("chinese",       Deprecated.class);
        put("cjk",           Deprecated.class);
        put("russianletter", Deprecated.class);
        
        // exposed in ES
        put("classic",       ClassicTokenizerFactory.class);
        put("edgengram",     EdgeNGramTokenizerFactory.class);
        put("keyword",       KeywordTokenizerFactory.class);
        put("letter",        LetterTokenizerFactory.class);
        put("lowercase",     LowerCaseTokenizerFactory.class);
        put("ngram",         NGramTokenizerFactory.class);
        put("pathhierarchy", PathHierarchyTokenizerFactory.class);
        put("pattern",       PatternTokenizerFactory.class);
        put("standard",      StandardTokenizerFactory.class);
        put("thai",          ThaiTokenizerFactory.class);
        put("uax29urlemail", UAX29URLEmailTokenizerFactory.class);
        put("whitespace",    WhitespaceTokenizerFactory.class);
        
        // this one "seems to mess up offsets". probably shouldn't be a tokenizer...
        put("wikipedia",     Void.class);
    }};
    
    public void testTokenizers() {
        Set<String> missing = new TreeSet<String>(org.apache.lucene.analysis.util.TokenizerFactory.availableTokenizers());
        missing.removeAll(KNOWN_TOKENIZERS.keySet());
        assertTrue("new tokenizers found, please update KNOWN_TOKENIZERS: " + missing.toString(), missing.isEmpty());
    }
    
    static final Map<String,Class<?>> KNOWN_TOKENFILTERS = new HashMap<String,Class<?>>() {{
        // deprecated ones, we dont care about these
        put("chinese",                Deprecated.class);
        put("collationkey",           Deprecated.class);
        put("position",               Deprecated.class);
        put("thaiword",               Deprecated.class);
        
        
        // exposed in ES
        put("apostrophe",                ApostropheFilterFactory.class);
        put("arabicnormalization",       ArabicNormalizationFilterFactory.class);
        put("arabicstem",                ArabicStemTokenFilterFactory.class);
        put("asciifolding",              ASCIIFoldingTokenFilterFactory.class);
        put("brazilianstem",             BrazilianStemTokenFilterFactory.class);
        put("bulgarianstem",             StemmerTokenFilterFactory.class);
        put("cjkbigram",                 CJKBigramFilterFactory.class);
        put("cjkwidth",                  CJKWidthFilterFactory.class);
        put("classic",                   ClassicFilterFactory.class);
        put("commongrams",               CommonGramsTokenFilterFactory.class);
        put("commongramsquery",          CommonGramsTokenFilterFactory.class);
        put("czechstem",                 CzechStemTokenFilterFactory.class);
        put("decimaldigit",              DecimalDigitFilterFactory.class);
        put("delimitedpayload",          DelimitedPayloadTokenFilterFactory.class);
        put("dictionarycompoundword",    DictionaryCompoundWordTokenFilterFactory.class);
        put("edgengram",                 EdgeNGramTokenFilterFactory.class);
        put("elision",                   ElisionTokenFilterFactory.class);
        put("englishminimalstem",        StemmerTokenFilterFactory.class);
        put("englishpossessive",         StemmerTokenFilterFactory.class);
        put("finnishlightstem",          StemmerTokenFilterFactory.class);
        put("frenchlightstem",           StemmerTokenFilterFactory.class);
        put("frenchminimalstem",         StemmerTokenFilterFactory.class);
        put("galicianminimalstem",       StemmerTokenFilterFactory.class);
        put("galicianstem",              StemmerTokenFilterFactory.class);
        put("germanstem",                GermanStemTokenFilterFactory.class);
        put("germanlightstem",           StemmerTokenFilterFactory.class);
        put("germanminimalstem",         StemmerTokenFilterFactory.class);
        put("germannormalization",       GermanNormalizationFilterFactory.class);
        put("greeklowercase",            LowerCaseTokenFilterFactory.class);
        put("greekstem",                 StemmerTokenFilterFactory.class);
        put("hindinormalization",        HindiNormalizationFilterFactory.class);
        put("hindistem",                 StemmerTokenFilterFactory.class);
        put("hungarianlightstem",        StemmerTokenFilterFactory.class);
        put("hunspellstem",              HunspellTokenFilterFactory.class);
        put("hyphenationcompoundword",   HyphenationCompoundWordTokenFilterFactory.class);
        put("indicnormalization",        IndicNormalizationFilterFactory.class);
        put("irishlowercase",            LowerCaseTokenFilterFactory.class);
        put("indonesianstem",            StemmerTokenFilterFactory.class);
        put("italianlightstem",          StemmerTokenFilterFactory.class);
        put("keepword",                  KeepWordFilterFactory.class);
        put("keywordmarker",             KeywordMarkerTokenFilterFactory.class);
        put("kstem",                     KStemTokenFilterFactory.class);
        put("latvianstem",               StemmerTokenFilterFactory.class);
        put("length",                    LengthTokenFilterFactory.class);
        put("limittokencount",           LimitTokenCountFilterFactory.class);
        put("lowercase",                 LowerCaseTokenFilterFactory.class);
        put("ngram",                     NGramTokenFilterFactory.class);
        put("norwegianlightstem",        StemmerTokenFilterFactory.class);
        put("norwegianminimalstem",      StemmerTokenFilterFactory.class);
        put("patterncapturegroup",       PatternCaptureGroupTokenFilterFactory.class);
        put("patternreplace",            PatternReplaceTokenFilterFactory.class);
        put("persiannormalization",      PersianNormalizationFilterFactory.class);
        put("porterstem",                PorterStemTokenFilterFactory.class);
        put("portuguesestem",            StemmerTokenFilterFactory.class);
        put("portugueselightstem",       StemmerTokenFilterFactory.class);
        put("portugueseminimalstem",     StemmerTokenFilterFactory.class);
        put("reversestring",             ReverseTokenFilterFactory.class);
        put("russianlightstem",          StemmerTokenFilterFactory.class);
        put("scandinavianfolding",       ScandinavianFoldingFilterFactory.class);
        put("scandinaviannormalization", ScandinavianNormalizationFilterFactory.class);
        put("serbiannormalization",      SerbianNormalizationFilterFactory.class);
        put("shingle",                   ShingleTokenFilterFactory.class);
        put("snowballporter",            SnowballTokenFilterFactory.class);
        put("soraninormalization",       SoraniNormalizationFilterFactory.class);
        put("soranistem",                StemmerTokenFilterFactory.class);
        put("spanishlightstem",          StemmerTokenFilterFactory.class);
        put("standard",                  StandardTokenFilterFactory.class);
        put("stemmeroverride",           StemmerOverrideTokenFilterFactory.class);
        put("stop",                      StopTokenFilterFactory.class);
        put("swedishlightstem",          StemmerTokenFilterFactory.class);
        put("synonym",                   SynonymTokenFilterFactory.class);
        put("trim",                      TrimTokenFilterFactory.class);
        put("truncate",                  TruncateTokenFilterFactory.class);
        put("turkishlowercase",          LowerCaseTokenFilterFactory.class);
        put("type",                      KeepTypesFilterFactory.class);
        put("uppercase",                 UpperCaseTokenFilterFactory.class);
        put("worddelimiter",             WordDelimiterTokenFilterFactory.class);
                
        // TODO: these tokenfilters are not yet exposed: useful?

        // suggest stop
        put("suggeststop",               Void.class);
        // capitalizes tokens
        put("capitalization",            Void.class);
        // like length filter (but codepoints)
        put("codepointcount",            Void.class);
        // puts hyphenated words back together
        put("hyphenatedwords",           Void.class);
        // repeats anything marked as keyword
        put("keywordrepeat",             Void.class);
        // like limittokencount, but by offset
        put("limittokenoffset",          Void.class);
        // like limittokencount, but by position
        put("limittokenposition",        Void.class);
        // ???
        put("numericpayload",            Void.class);
        // removes duplicates at the same position (this should be used by the existing factory)
        put("removeduplicates",          Void.class);
        // ???
        put("tokenoffsetpayload",        Void.class);
        // puts the type into the payload
        put("typeaspayload",             Void.class);
        // fingerprint
        put("fingerprint",               Void.class);
    }};
    
    public void testTokenFilters() {
        Set<String> missing = new TreeSet<String>(org.apache.lucene.analysis.util.TokenFilterFactory.availableTokenFilters());
        missing.removeAll(KNOWN_TOKENFILTERS.keySet());
        assertTrue("new tokenfilters found, please update KNOWN_TOKENFILTERS: " + missing.toString(), missing.isEmpty());
    }
    
    static final Map<String,Class<?>> KNOWN_CHARFILTERS = new HashMap<String,Class<?>>() {{        
        // exposed in ES
        put("htmlstrip",      HtmlStripCharFilterFactory.class);
        put("mapping",        MappingCharFilterFactory.class);
        put("patternreplace", PatternReplaceCharFilterFactory.class);
                
        // TODO: these charfilters are not yet exposed: useful?
        // handling of zwnj for persian
        put("persian",        Void.class);
    }};
    
    public void testCharFilters() {
        Set<String> missing = new TreeSet<String>(org.apache.lucene.analysis.util.CharFilterFactory.availableCharFilters());
        missing.removeAll(KNOWN_CHARFILTERS.keySet());
        assertTrue("new charfilters found, please update KNOWN_CHARFILTERS: " + missing.toString(), missing.isEmpty());
    }
   
    
}
