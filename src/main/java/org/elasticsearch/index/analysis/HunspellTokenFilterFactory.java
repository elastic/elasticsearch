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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.analysis.HunspellService;

import java.util.Locale;

@AnalysisSettingsRequired
public class HunspellTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Dictionary dictionary;
    private final boolean dedup;
    private final boolean longestOnly;

    @Inject
    public HunspellTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings, HunspellService hunspellService)  {
        super(index, indexSettings, name, settings);

        String locale = settings.get("locale", settings.get("language", settings.get("lang", null)));
        if (locale == null) {
            throw new ElasticsearchIllegalArgumentException("missing [locale | language | lang] configuration for hunspell token filter");
        }

        dictionary = hunspellService.getDictionary(locale);
        if (dictionary == null) {
            throw new ElasticsearchIllegalArgumentException(String.format(Locale.ROOT, "Unknown hunspell dictionary for locale [%s]", locale));
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
