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


import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternCaptureGroupTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

import java.util.List;
import java.util.regex.Pattern;

public class PatternCaptureGroupTokenFilterFactory extends AbstractTokenFilterFactory {
    private final Pattern[] patterns;
    private final boolean preserveOriginal;
    private static final String PATTERNS_KEY = "patterns";
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    PatternCaptureGroupTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        List<String> regexes = settings.getAsList(PATTERNS_KEY, null, false);
        if (regexes == null) {
            throw new IllegalArgumentException("required setting '" + PATTERNS_KEY + "' is missing for token filter [" + name + "]");
        }
        patterns = new Pattern[regexes.size()];
        for (int i = 0; i < regexes.size(); i++) {
            patterns[i] = Pattern.compile(regexes.get(i));
        }

        preserveOriginal = settings.getAsBooleanLenientForPreEs6Indices(
            indexSettings.getIndexVersionCreated(), PRESERVE_ORIG_KEY, true, deprecationLogger);
    }

    @Override
    public TokenFilter create(TokenStream tokenStream) {
        return new PatternCaptureGroupTokenFilter(tokenStream, preserveOriginal, patterns);
    }
}
