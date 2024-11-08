/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenFilter;
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
import org.apache.lucene.analysis.es.SpanishPluralStemFilter;
import org.apache.lucene.analysis.fa.PersianStemFilter;
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
import org.apache.lucene.analysis.no.NorwegianLightStemFilterFactory;
import org.apache.lucene.analysis.no.NorwegianMinimalStemFilter;
import org.apache.lucene.analysis.no.NorwegianMinimalStemFilterFactory;
import org.apache.lucene.analysis.pt.PortugueseLightStemFilter;
import org.apache.lucene.analysis.pt.PortugueseMinimalStemFilter;
import org.apache.lucene.analysis.pt.PortugueseStemFilter;
import org.apache.lucene.analysis.ru.RussianLightStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.sv.SwedishLightStemFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
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
import org.tartarus.snowball.ext.GermanStemmer;
import org.tartarus.snowball.ext.HungarianStemmer;
import org.tartarus.snowball.ext.IrishStemmer;
import org.tartarus.snowball.ext.ItalianStemmer;
import org.tartarus.snowball.ext.LithuanianStemmer;
import org.tartarus.snowball.ext.NorwegianStemmer;
import org.tartarus.snowball.ext.PortugueseStemmer;
import org.tartarus.snowball.ext.RomanianStemmer;
import org.tartarus.snowball.ext.RussianStemmer;
import org.tartarus.snowball.ext.SerbianStemmer;
import org.tartarus.snowball.ext.SpanishStemmer;
import org.tartarus.snowball.ext.SwedishStemmer;
import org.tartarus.snowball.ext.TurkishStemmer;

import java.io.IOException;
import java.util.Collections;

public class StemmerTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(StemmerTokenFilterFactory.class);

    private static final TokenStream EMPTY_TOKEN_STREAM = new EmptyTokenStream();

    private final String language;

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(StemmerTokenFilterFactory.class);

    StemmerTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        super(name, settings);
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
            deprecationLogger.critical(
                DeprecationCategory.ANALYSIS,
                "dutch_kp_deprecation",
                "The [dutch_kp] stemmer is deprecated and will be removed in a future version."
            );
            return new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            };
            // English stemmers
        } else if ("english".equalsIgnoreCase(language)) {
            return new PorterStemFilter(tokenStream);
        } else if ("light_english".equalsIgnoreCase(language)
            || "lightEnglish".equalsIgnoreCase(language)
            || "kstem".equalsIgnoreCase(language)) {
                return new KStemFilter(tokenStream);
            } else if ("lovins".equalsIgnoreCase(language)) {
                deprecationLogger.critical(
                    DeprecationCategory.ANALYSIS,
                    "lovins_deprecation",
                    "The [lovins] stemmer is deprecated and will be removed in a future version."
                );
                return new TokenFilter(tokenStream) {
                    @Override
                    public boolean incrementToken() {
                        return false;
                    }
                };
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
                DEPRECATION_LOGGER.critical(
                    DeprecationCategory.ANALYSIS,
                    "german2_stemmer_deprecation",
                    "The 'german2' stemmer has been deprecated and folded into the 'german' Stemmer. "
                        + "Replace all usages of 'german2' with 'german'."
                );
                return new SnowballFilter(tokenStream, new GermanStemmer());
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
                NorwegianLightStemFilterFactory factory = new NorwegianLightStemFilterFactory(Collections.singletonMap("variant", "nn"));
                return factory.create(tokenStream);
            } else if ("minimal_nynorsk".equalsIgnoreCase(language) || "minimalNynorsk".equalsIgnoreCase(language)) {
                NorwegianMinimalStemFilterFactory factory = new NorwegianMinimalStemFilterFactory(
                    Collections.singletonMap("variant", "nn")
                );
                return factory.create(tokenStream);
                // Persian stemmers
            } else if ("persian".equalsIgnoreCase(language)) {
                return new PersianStemFilter(tokenStream);

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

            } else if ("serbian".equalsIgnoreCase(language)) {
                return new SnowballFilter(tokenStream, new SerbianStemmer());

                // Spanish stemmers
            } else if ("spanish".equalsIgnoreCase(language)) {
                return new SnowballFilter(tokenStream, new SpanishStemmer());
            } else if ("light_spanish".equalsIgnoreCase(language) || "lightSpanish".equalsIgnoreCase(language)) {
                return new SpanishLightStemFilter(tokenStream);
            } else if ("spanish_plural".equalsIgnoreCase(language)) {
                return new SpanishPluralStemFilter(tokenStream);

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
