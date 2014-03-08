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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.io.Reader;
import java.util.Locale;

@AnalysisSettingsRequired
public class SentenceCharFilterFactory extends AbstractCharFilterFactory {
    /**
     * Default number of characters to analyze.  Picked because the average first sentence length on English Wikipedia's
     * content articles is 286 characters with a standard deviation of 367.  1020 would get two standard deviations
     * worth of articles so we round up to 1024.
     */
    public static final int DEFAULT_ANALYZED_CHARS = 1024;
    private final Locale locale;
    private final int sentences;
    private final int analyzedChars;

    @Inject
    public SentenceCharFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);

        locale = LocaleUtils.parse(settings.get("locale", "en_us"));
        sentences = settings.getAsInt("sentences", 1);
        analyzedChars = settings.getAsInt("analyzed_chars", DEFAULT_ANALYZED_CHARS);
    }

    @Override
    public Reader create(Reader tokenStream) {
        try {
            return SentenceCharFilter.build(tokenStream, locale, sentences, analyzedChars);
        } catch (IOException e) {
            throw new ElasticsearchException("Error building sentence filter.", e);
        }
    }
}
