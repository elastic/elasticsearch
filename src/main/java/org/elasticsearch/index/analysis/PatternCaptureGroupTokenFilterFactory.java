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


import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternCaptureGroupTokenFilter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.regex.Pattern;

@AnalysisSettingsRequired
public class PatternCaptureGroupTokenFilterFactory extends AbstractTokenFilterFactory {
    private final Pattern[] patterns;
    private final boolean preserveOriginal;
    private static final String PATTERNS_KEY = "patterns";
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    @Inject
    public PatternCaptureGroupTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name,
            @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        String[] regexes = settings.getAsArray(PATTERNS_KEY, null, false);
        if (regexes == null) {
            throw new ElasticsearchIllegalArgumentException("required setting '" + PATTERNS_KEY + "' is missing for token filter [" + name + "]");
        }
        patterns = new Pattern[regexes.length];
        for (int i = 0; i < regexes.length; i++) {
            patterns[i] = Pattern.compile(regexes[i]);
        }

        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, true);
    }

    @Override
    public TokenFilter create(TokenStream tokenStream) {
        return new PatternCaptureGroupTokenFilter(tokenStream, preserveOriginal, patterns);
    }
}
