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
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.regex.Pattern;

@AnalysisSettingsRequired
public class PatternReplaceTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Pattern pattern;
    private final String replacement;
    private final boolean all;

    @Inject
    public PatternReplaceTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);

        String sPattern = settings.get("pattern", null);
        if (sPattern == null) {
            throw new ElasticsearchIllegalArgumentException("pattern is missing for [" + name + "] token filter of type 'pattern_replace'");
        }
        this.pattern = Regex.compile(sPattern, settings.get("flags"));
        this.replacement = settings.get("replacement", "");
        this.all = settings.getAsBoolean("all", true);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new PatternReplaceFilter(tokenStream, pattern, replacement, all);
    }
}