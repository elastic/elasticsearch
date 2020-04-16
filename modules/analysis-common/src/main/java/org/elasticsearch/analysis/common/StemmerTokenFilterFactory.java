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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.bg.BulgarianStemFilter;
import org.apache.lucene.analysis.bn.BengaliStemFilter;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.ckb.SoraniStemFilter;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanMinimalStemFilter;
import org.apache.lucene.analysis.el.GreekStemFilter;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.es.SpanishLightStemFilter;
import org.apache.lucene.analysis.fi.FinnishLightStemFilter;
import org.apache.lucene.analysis.fr.FrenchLightStemFilter;
import org.apache.lucene.analysis.fr.FrenchMinimalStemFilter;
import org.apache.lucene.analysis.gl.GalicianMinimalStemFilter;
import org.apache.lucene.analysis.gl.GalicianStemFilter;
import org.apache.lucene.analysis.hi.HindiStemFilter;
import org.apache.lucene.analysis.hu.HungarianLightStemFilter;
import org.apache.lucene.analysis.id.IndonesianStemFilter;
import org.apache.lucene.analysis.it.ItalianLightStemFilter;
import org.apache.lucene.analysis.lv.LatvianStemFilter;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.analysis.no.NorwegianLightStemFilter;
import org.apache.lucene.analysis.no.NorwegianLightStemmer;
import org.apache.lucene.analysis.no.NorwegianMinimalStemFilter;
import org.apache.lucene.analysis.pt.PortugueseLightStemFilter;
import org.apache.lucene.analysis.pt.PortugueseMinimalStemFilter;
import org.apache.lucene.analysis.pt.PortugueseStemFilter;
import org.apache.lucene.analysis.ru.RussianLightStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.sv.SwedishLightStemFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.tartarus.snowball.ext.ArmenianStemmer;
import org.tartarus.snowball.ext.BasqueStemmer;
import org.tartarus.snowball.ext.CatalanStemmer;
import org.tartarus.snowball.ext.DanishStemmer;
import org.tartarus.snowball.ext.DutchStemmer;
import org.tartarus.snowball.ext.EnglishStemmer;
import org.tartarus.snowball.ext.EstonianStemmer;
import org.tartarus.snowball.ext.FinnishStemmer;
import org.tartarus.snowball.ext.FrenchStemmer;
import org.tartarus.snowball.ext.German2Stemmer;
import org.tartarus.snowball.ext.GermanStemmer;
import org.tartarus.snowball.ext.HungarianStemmer;
import org.tartarus.snowball.ext.IrishStemmer;
import org.tartarus.snowball.ext.ItalianStemmer;
import org.tartarus.snowball.ext.KpStemmer;
import org.tartarus.snowball.ext.LithuanianStemmer;
import org.tartarus.snowball.ext.LovinsStemmer;
import org.tartarus.snowball.ext.NorwegianStemmer;
import org.tartarus.snowball.ext.PortugueseStemmer;
import org.tartarus.snowball.ext.RomanianStemmer;
import org.tartarus.snowball.ext.RussianStemmer;
import org.tartarus.snowball.ext.SpanishStemmer;
import org.tartarus.snowball.ext.SwedishStemmer;
import org.tartarus.snowball.ext.TurkishStemmer;

import java.io.IOException;

public class StemmerTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final TokenStream EMPTY_TOKEN_STREAM = new EmptyTokenStream();

    private String language;

    StemmerTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);
        this.language = Strings.capitalize(settings.get("language", settings.get("name", "porter")));
        // check that we have a valid language by trying to create a TokenStream
        create(EMPTY_TOKEN_STREAM).close();
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if ("arabic".equalsIgnoreCase(language)) {
            return new ArabicStemFilter(tokenStream);
        } else if ("armenian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new ArmenianStemmer());
        } else if ("basque".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new BasqueStemmer());
        } else if ("bengali".equalsIgnoreCase(language)) {
            return new BengaliStemFilter(tokenStream);
        } else if ("brazilian".equalsIgnoreCase(language)) {
            return new BrazilianStemFilter(tokenStream);
        } else if ("bulgarian".equalsIgnoreCase(language)) {
            return new BulgarianStemFilter(tokenStream);
        } else if ("catalan".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new CatalanStemmer());
        } else if ("czech".equalsIgnoreCase(language)) {
            return new CzechStemFilter(tokenStream);
        } else if ("danish".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new DanishStemmer());

            // Dutch stemmers
        } else if ("dutch".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new DutchStemmer());
        } else if ("dutch_kp".equalsIgnoreCase(language) || "dutchKp".equalsIgnoreCase(language) || "kp".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new KpStemmer());

            // English stemmers
        } else if ("english".equalsIgnoreCase(language)) {
            return new PorterStemFilter(tokenStream);
        } else if ("light_english".equalsIgnoreCase(language) || "lightEnglish".equalsIgnoreCase(language)
                || "kstem".equalsIgnoreCase(language)) {
            return new KStemFilter(tokenStream);
        } else if ("lovins".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new LovinsStemmer());
        } else if ("porter".equalsIgnoreCase(language)) {
            return new PorterStemFilter(tokenStream);
        } else if ("porter2".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new EnglishStemmer());
        } else if ("minimal_english".equalsIgnoreCase(language) || "minimalEnglish".equalsIgnoreCase(language)) {
            return new EnglishMinimalStemFilter(tokenStream);
        } else if ("possessive_english".equalsIgnoreCase(language) || "possessiveEnglish".equalsIgnoreCase(language)) {
            return new EnglishPossessiveFilter(tokenStream);

        } else if ("estonian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new EstonianStemmer());

            // Finnish stemmers
        } else if ("finnish".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new FinnishStemmer());
        } else if ("light_finish".equalsIgnoreCase(language) || "lightFinish".equalsIgnoreCase(language)) {
            // leaving this for backward compatibility
            return new FinnishLightStemFilter(tokenStream);
        } else if ("light_finnish".equalsIgnoreCase(language) || "lightFinnish".equalsIgnoreCase(language)) {
            return new FinnishLightStemFilter(tokenStream);

            // French stemmers
        } else if ("french".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new FrenchStemmer());
        } else if ("light_french".equalsIgnoreCase(language) || "lightFrench".equalsIgnoreCase(language)) {
            return new FrenchLightStemFilter(tokenStream);
        } else if ("minimal_french".equalsIgnoreCase(language) || "minimalFrench".equalsIgnoreCase(language)) {
            return new FrenchMinimalStemFilter(tokenStream);

            // Galician stemmers
        } else if ("galician".equalsIgnoreCase(language)) {
            return new GalicianStemFilter(tokenStream);
        } else if ("minimal_galician".equalsIgnoreCase(language)) {
            return new GalicianMinimalStemFilter(tokenStream);

            // German stemmers
        } else if ("german".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new GermanStemmer());
        } else if ("german2".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new German2Stemmer());
        } else if ("light_german".equalsIgnoreCase(language) || "lightGerman".equalsIgnoreCase(language)) {
            return new GermanLightStemFilter(tokenStream);
        } else if ("minimal_german".equalsIgnoreCase(language) || "minimalGerman".equalsIgnoreCase(language)) {
            return new GermanMinimalStemFilter(tokenStream);

        } else if ("greek".equalsIgnoreCase(language)) {
            return new GreekStemFilter(tokenStream);
        } else if ("hindi".equalsIgnoreCase(language)) {
            return new HindiStemFilter(tokenStream);

            // Hungarian stemmers
        } else if ("hungarian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new HungarianStemmer());
        } else if ("light_hungarian".equalsIgnoreCase(language) || "lightHungarian".equalsIgnoreCase(language)) {
            return new HungarianLightStemFilter(tokenStream);

        } else if ("indonesian".equalsIgnoreCase(language)) {
            return new IndonesianStemFilter(tokenStream);

            // Irish stemmer
        } else if ("irish".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new IrishStemmer());

            // Italian stemmers
        } else if ("italian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new ItalianStemmer());
        } else if ("light_italian".equalsIgnoreCase(language) || "lightItalian".equalsIgnoreCase(language)) {
            return new ItalianLightStemFilter(tokenStream);

        } else if ("latvian".equalsIgnoreCase(language)) {
            return new LatvianStemFilter(tokenStream);

        } else if ("lithuanian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new LithuanianStemmer());

            // Norwegian (Bokmål) stemmers
        } else if ("norwegian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new NorwegianStemmer());
        } else if ("light_norwegian".equalsIgnoreCase(language) || "lightNorwegian".equalsIgnoreCase(language)) {
            return new NorwegianLightStemFilter(tokenStream);
        } else if ("minimal_norwegian".equalsIgnoreCase(language) || "minimalNorwegian".equals(language)) {
            return new NorwegianMinimalStemFilter(tokenStream);

            // Norwegian (Nynorsk) stemmers
        } else if ("light_nynorsk".equalsIgnoreCase(language) || "lightNynorsk".equalsIgnoreCase(language)) {
            return new NorwegianLightStemFilter(tokenStream, NorwegianLightStemmer.NYNORSK);
        } else if ("minimal_nynorsk".equalsIgnoreCase(language) || "minimalNynorsk".equalsIgnoreCase(language)) {
            return new NorwegianMinimalStemFilter(tokenStream, NorwegianLightStemmer.NYNORSK);

            // Portuguese stemmers
        } else if ("portuguese".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new PortugueseStemmer());
        } else if ("light_portuguese".equalsIgnoreCase(language) || "lightPortuguese".equalsIgnoreCase(language)) {
            return new PortugueseLightStemFilter(tokenStream);
        } else if ("minimal_portuguese".equalsIgnoreCase(language) || "minimalPortuguese".equalsIgnoreCase(language)) {
            return new PortugueseMinimalStemFilter(tokenStream);
        } else if ("portuguese_rslp".equalsIgnoreCase(language)) {
            return new PortugueseStemFilter(tokenStream);

        } else if ("romanian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new RomanianStemmer());

            // Russian stemmers
        } else if ("russian".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new RussianStemmer());
        } else if ("light_russian".equalsIgnoreCase(language) || "lightRussian".equalsIgnoreCase(language)) {
            return new RussianLightStemFilter(tokenStream);

            // Spanish stemmers
        } else if ("spanish".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new SpanishStemmer());
        } else if ("light_spanish".equalsIgnoreCase(language) || "lightSpanish".equalsIgnoreCase(language)) {
            return new SpanishLightStemFilter(tokenStream);

            // Sorani Kurdish stemmer
        } else if ("sorani".equalsIgnoreCase(language)) {
            return new SoraniStemFilter(tokenStream);

            // Swedish stemmers
        } else if ("swedish".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new SwedishStemmer());
        } else if ("light_swedish".equalsIgnoreCase(language) || "lightSwedish".equalsIgnoreCase(language)) {
            return new SwedishLightStemFilter(tokenStream);

        } else if ("turkish".equalsIgnoreCase(language)) {
            return new SnowballFilter(tokenStream, new TurkishStemmer());
        }

        return new SnowballFilter(tokenStream, language);
    }

}
