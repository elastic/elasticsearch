/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.hunspell.HunspellStemFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.HunspellService;

import java.util.Locale;

public class HunspellTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Dictionary dictionary;
    private final boolean dedup;
    private final boolean longestOnly;

    public HunspellTokenFilterFactory(IndexSettings indexSettings, String name, Settings settings, HunspellService hunspellService) {
        super(indexSettings, name, settings);

        String locale = settings.get("locale", settings.get("language", settings.get("lang", null)));
        if (locale == null) {
            throw new IllegalArgumentException("missing [locale | language | lang] configuration for hunspell token filter");
        }

        dictionary = hunspellService.getDictionary(locale);
        if (dictionary == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Unknown hunspell dictionary for locale [%s]", locale));
        }

        dedup = settings.getAsBoolean("dedup", true);
        longestOnly = settings.getAsBoolean("longest_only", false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HunspellStemFilter(tokenStream, dictionary, dedup, longestOnly);
    }

    public boolean dedup() {
        return dedup;
    }

    public boolean longestOnly() {
        return longestOnly;
    }

}
