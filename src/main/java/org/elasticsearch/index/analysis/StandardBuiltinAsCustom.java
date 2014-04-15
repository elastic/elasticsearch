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

import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Spits out custom analyzer configuration required to reproduce builtin analyzers.
 */
public class StandardBuiltinAsCustom implements BuiltinAsCustom {
    @Override
    public boolean build(XContentBuilder builder, String name) throws IOException {
        if ("standard".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "standard", "standard", "standard", "lowercase");
            return true;
        }
        if ("simple".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "simple", "lowercase");
            return true;
        }
        if ("keyword".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "keyword", "keyword");
            return true;
        }
        if ("whitespace".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "whitespace", "whitespace");
            return true;
        }
        if ("stop".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "stop", "lowercase", "stop");
            builder.startObject("filter");
            buildStop(builder, "english", EnglishAnalyzer.getDefaultStopSet());
            builder.endObject();
            return true;
        }
        if ("snowball".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "snowball", "standard", "standard", "possessive_english_stemmer", "lowercase", "english_stop",
                    "english_snowball");
            builder.startObject("filter");
            buildStemmer(builder, "possessive_english");
            buildStop(builder, "english", EnglishAnalyzer.getDefaultStopSet());
            builder.startObject("english_snowball");
            builder.field("type", "snowball");
            builder.field("language", "English");
            builder.endObject();
            builder.endObject();
            return true;
        }
        if ("arabic".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "arabic", "standard", "standard", "lowercase", "arabic_stop", "arabic_normalization", "arabic_stem");
            builder.startObject("filter");
            buildStop(builder, "arabic", ArabicAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "arabic");
            builder.endObject();
            return true;
        }
        if ("armenian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "armenian", ArmenianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("basque".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "basque", BasqueAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("brazilian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "brazilian", BrazilianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("bulgarian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "bulgarian", BulgarianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("catalan".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "catalan", "standard", "standard", "catalan_elision", "lowercase", "catalan_stop", "catalan_stemmer");
            builder.startObject("filter");
            // The default articles are private.....
            buildElision(builder, "catalan", "d", "l", "m", "n", "s", "t");
            buildStop(builder, "catalan", CatalanAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "catalan");
            builder.endObject();
            return true;
        }
        if ("chinese".equalsIgnoreCase(name)) {
            throw new IllegalArgumentException(
                    "The chinese analyzer is deprecated.  Use the Standard analyzer or the smart Chinese plugin instead.");
        }
        if ("cjk".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "cjk", "standard", "cjk_width", "lowercase", "cjk_bigram", "cjk_stop");
            builder.startObject("filter");
            buildStop(builder, "cjk", CJKAnalyzer.getDefaultStopSet());
            builder.endObject();
            return true;
        }
        if ("czech".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "czech", CzechAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("danish".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "danish", DanishAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("dutch".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "dutch", "standard", "standard", "lowercase", "dutch_stop", "dutch_stemmer_override", "dutch_stemmer");
            builder.startObject("filter");
            buildStop(builder, "dutch", DutchAnalyzer.getDefaultStopSet());
            builder.startObject("dutch_stemmer_override");
            builder.field("type", "stemmer_override");
            builder.startArray("rules");
            // Have to manually specify the rules because they are private in lucene....
            builder.value("fiets => fiets");
            builder.value("bromfiets => bromfiets");
            builder.value("ei => eier");
            builder.value("kind => kinder");
            builder.endArray();
            builder.endObject();
            buildStemmer(builder, "dutch");
            builder.endObject();
            return true;
        }
        if ("english".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "english", "standard", "standard", "possessive_english_stemmer", "lowercase", "english_stop", "porter_stem");
            builder.startObject("filter");
            buildStemmer(builder, "possessive_english");
            buildStop(builder, "english", EnglishAnalyzer.getDefaultStopSet());
            builder.endObject();
            return true;
        }
        if ("finnish".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "finnish", FinnishAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("french".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "french", "standard", "standard", "elision", "lowercase", "french_stop", "light_french_stemmer");
            builder.startObject("filter");
            buildStop(builder, "french", FrenchAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "light_french");
            builder.endObject();
            return true;
        }
        if ("galician".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "galician", GalicianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("german".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "german", "standard", "standard", "lowercase", "german_stop", "german_normalization", "light_german_stemmer");
            builder.startObject("filter");
            buildStop(builder, "german", GermanAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "light_german");
            builder.endObject();
            return true;
        }
        if ("greek".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "greek", "standard", "greek_lowercase", "standard", "greek_stop", "greek_stemmer");
            builder.startObject("filter");
            buildLowercase(builder, "greek");
            buildStop(builder, "greek", GreekAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "greek");
            builder.endObject();
            return true;
        }
        if ("hindi".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "hindi", "standard", "lowercase", "indic_normalization", "hindi_normalization", "hindi_stop", "hindi_stemmer");
            builder.startObject("filter");
            buildStop(builder, "hindi", HindiAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "hindi");
            builder.endObject();
            return true;
        }
        if ("hungarian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "hungarian", HungarianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("indonesian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "indonesian", IndonesianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("italian".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "italian", "standard", "standard", "italian_elision", "lowercase", "italian_stop", "light_italian_stemmer");
            builder.startObject("filter");
            // The default articles are private.....
            buildElision(builder, "italian", "c", "l", "all", "dall", "dell", "nell", "sull", "coll", "pell", "gl", "agl", "dagl", "degl",
                    "negl", "sugl", "un", "m", "t", "s", "v", "d");
            buildStop(builder, "italian", ItalianAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "light_italian");
            builder.endObject();
            return true;
        }
        if ("norwegian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "norwegian", NorwegianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("persian".equalsIgnoreCase(name)) {
            // Since Persian uses a char_filter it can't use buildAnalyzer
            builder.startObject("analyzer");
            builder.startObject("custom_persian");
            builder.field("type", "custom");
            builder.field("tokenizer", "standard");
            builder.array("filter", "lowercase", "arabic_normalization", "persian_normalization", "persian_stop");
            builder.array("char_filter", "persian");
            builder.endObject();
            builder.endObject();
            builder.startObject("filter");
            buildStop(builder, "persian", PersianAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "persian");
            builder.endObject();
            builder.startObject("char_filter");
            builder.startObject("persian");
            builder.field("type", "persian");
            builder.endObject();
            builder.endObject();
            return true;
        }
        if ("portuguese".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "portuguese", PortugueseAnalyzer.getDefaultStopSet(), "light_");
            return true;
        }
        if ("romanian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "romanian", RomanianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("russian".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "russian", RussianAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("spanish".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "spanish", SpanishAnalyzer.getDefaultStopSet(), "light_");
            return true;
        }
        if ("swedish".equalsIgnoreCase(name)) {
            buildStandardLanguage(builder, "swedish", SwedishAnalyzer.getDefaultStopSet());
            return true;
        }
        if ("turkish".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "turkish", "standard", "turkish_lowercase", "standard", "turkish_stop", "turkish_stemmer");
            builder.startObject("filter");
            buildLowercase(builder, "turkish");
            buildStop(builder, "turkish", TurkishAnalyzer.getDefaultStopSet());
            buildStemmer(builder, "turkish");
            builder.endObject();
            return true;
        }
        if ("thai".equalsIgnoreCase(name)) {
            buildAnalyzer(builder, "thai", "standard", "standard", "lowercase", "thai_word", "thai_stop");
            builder.startObject("filter");
            builder.startObject("thai_word");
            builder.field("type", "thai_word");
            builder.endObject();
            buildStop(builder, "thai", ThaiAnalyzer.getDefaultStopSet());
            builder.endObject();
            return true;
        }
        return false;
    }

    private void buildAnalyzer(XContentBuilder builder, String name, String tokenizer, String... filters) throws IOException {
        builder.startObject("analyzer");
        builder.startObject("custom_" + name);
        builder.field("type", "custom");
        builder.field("tokenizer", tokenizer);
        builder.array("filter", filters);
        builder.endObject();
        builder.endObject();
    }

    private void buildStandardLanguage(XContentBuilder builder, String name, CharArraySet stopSet) throws IOException {
        buildStandardLanguage(builder, name, stopSet, "");
    }

    /**
     * Build a language with the "standard" set of filters.
     */
    private void buildStandardLanguage(XContentBuilder builder, String name, CharArraySet stopSet, String stemmerNamePrefix) throws IOException {
        buildAnalyzer(builder, name, "standard", "standard", "lowercase", name + "_stop", stemmerNamePrefix + name + "_stemmer");
        builder.startObject("filter");
        buildStop(builder, name, stopSet);
        buildStemmer(builder, stemmerNamePrefix + name);
        builder.endObject();
    }

    private void buildStop(XContentBuilder builder, String name, CharArraySet set) throws IOException {
        builder.startObject(name + "_stop");
        builder.field("type", "stop");
        builder.startArray("stopwords");
        for (Object o : set) {
            builder.value(new String((char[]) o));
        }
        builder.endArray();
        builder.endObject();
    }
    
    private void buildStemmer(XContentBuilder builder, String language) throws IOException {
        builder.startObject(language + "_stemmer");
        builder.field("type", "stemmer");
        builder.field("language", language);
        builder.endObject();
    }

    private void buildElision(XContentBuilder builder, String language, String... articles) throws IOException {
        builder.startObject(language + "_elision");
        builder.field("type", "elision");
        builder.field("articles", articles);
        builder.field("articles_case", true);
        builder.endObject();
    }

    private void buildLowercase(XContentBuilder builder, String language) throws IOException {
        builder.startObject(language + "_lowercase");
        builder.field("type", "lowercase");
        builder.field("language", language);
        builder.endObject();
    }
}
