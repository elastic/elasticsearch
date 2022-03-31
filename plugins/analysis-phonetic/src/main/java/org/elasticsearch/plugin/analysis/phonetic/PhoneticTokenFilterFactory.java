/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.apache.commons.codec.Encoder;
import org.apache.commons.codec.language.Caverphone1;
import org.apache.commons.codec.language.Caverphone2;
import org.apache.commons.codec.language.ColognePhonetic;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.Nysiis;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.codec.language.bm.Languages.LanguageSet;
import org.apache.commons.codec.language.bm.NameType;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.commons.codec.language.bm.RuleType;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.phonetic.BeiderMorseFilter;
import org.apache.lucene.analysis.phonetic.DaitchMokotoffSoundexFilter;
import org.apache.lucene.analysis.phonetic.DoubleMetaphoneFilter;
import org.apache.lucene.analysis.phonetic.PhoneticFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.util.HashSet;
import java.util.List;

public class PhoneticTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Encoder encoder;
    private final boolean replace;
    private int maxcodelength;
    private List<String> languageset;
    private NameType nametype;
    private RuleType ruletype;
    private boolean isDaitchMokotoff;

    public PhoneticTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.languageset = null;
        this.nametype = null;
        this.ruletype = null;
        this.maxcodelength = 0;
        this.isDaitchMokotoff = false;
        this.replace = settings.getAsBoolean("replace", true);
        // weird, encoder is null at last step in SimplePhoneticAnalysisTests, so we set it to metaphone as default
        String encodername = settings.get("encoder", "metaphone");
        if ("metaphone".equalsIgnoreCase(encodername)) {
            this.encoder = new Metaphone();
        } else if ("soundex".equalsIgnoreCase(encodername)) {
            this.encoder = new Soundex();
        } else if ("caverphone1".equalsIgnoreCase(encodername)) {
            this.encoder = new Caverphone1();
        } else if ("caverphone2".equalsIgnoreCase(encodername)) {
            this.encoder = new Caverphone2();
        } else if ("caverphone".equalsIgnoreCase(encodername)) {
            this.encoder = new Caverphone2();
        } else if ("refined_soundex".equalsIgnoreCase(encodername) || "refinedSoundex".equalsIgnoreCase(encodername)) {
            this.encoder = new RefinedSoundex();
        } else if ("cologne".equalsIgnoreCase(encodername)) {
            this.encoder = new ColognePhonetic();
        } else if ("double_metaphone".equalsIgnoreCase(encodername) || "doubleMetaphone".equalsIgnoreCase(encodername)) {
            this.encoder = null;
            this.maxcodelength = settings.getAsInt("max_code_len", 4);
        } else if ("bm".equalsIgnoreCase(encodername)
            || "beider_morse".equalsIgnoreCase(encodername)
            || "beidermorse".equalsIgnoreCase(encodername)) {
                this.encoder = null;
                this.languageset = settings.getAsList("languageset");
                String ruleType = settings.get("rule_type", "approx");
                if ("approx".equalsIgnoreCase(ruleType)) {
                    ruletype = RuleType.APPROX;
                } else if ("exact".equalsIgnoreCase(ruleType)) {
                    ruletype = RuleType.EXACT;
                } else {
                    throw new IllegalArgumentException("No matching rule type [" + ruleType + "] for beider morse encoder");
                }
                String nameType = settings.get("name_type", "generic");
                if ("GENERIC".equalsIgnoreCase(nameType)) {
                    nametype = NameType.GENERIC;
                } else if ("ASHKENAZI".equalsIgnoreCase(nameType)) {
                    nametype = NameType.ASHKENAZI;
                } else if ("SEPHARDIC".equalsIgnoreCase(nameType)) {
                    nametype = NameType.SEPHARDIC;
                }
            } else if ("koelnerphonetik".equalsIgnoreCase(encodername)) {
                this.encoder = new KoelnerPhonetik();
            } else if ("haasephonetik".equalsIgnoreCase(encodername)) {
                this.encoder = new HaasePhonetik();
            } else if ("nysiis".equalsIgnoreCase(encodername)) {
                this.encoder = new Nysiis();
            } else if ("daitch_mokotoff".equalsIgnoreCase(encodername)) {
                this.encoder = null;
                this.isDaitchMokotoff = true;
            } else {
                throw new IllegalArgumentException("unknown encoder [" + encodername + "] for phonetic token filter");
            }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        if (encoder == null) {
            if (isDaitchMokotoff) {
                return new DaitchMokotoffSoundexFilter(tokenStream, replace == false);
            }
            if (ruletype != null && nametype != null) {
                LanguageSet langset = null;
                if (languageset != null && languageset.size() > 0) {
                    langset = LanguageSet.from(new HashSet<>(languageset));
                }
                return new BeiderMorseFilter(tokenStream, new PhoneticEngine(nametype, ruletype, true), langset);
            }
            if (maxcodelength > 0) {
                return new DoubleMetaphoneFilter(tokenStream, maxcodelength, replace == false);
            }
        } else {
            return new PhoneticFilter(tokenStream, encoder, replace == false);
        }
        throw new IllegalArgumentException("encoder error");
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }
}
