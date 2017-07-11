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

        dedup = settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "dedup", true, deprecationLogger);
        longestOnly =
            settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "longest_only", false, deprecationLogger);
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
