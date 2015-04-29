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

import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.Reader;
import java.util.regex.Pattern;

@AnalysisSettingsRequired
public class PatternReplaceCharFilterFactory extends AbstractCharFilterFactory {

    private final Pattern pattern;
    private final String replacement;

    @Inject
    public PatternReplaceCharFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);

        if (!Strings.hasLength(settings.get("pattern"))) {
            throw new IllegalArgumentException("pattern is missing for [" + name + "] char filter of type 'pattern_replace'");
        }
        pattern = Pattern.compile(settings.get("pattern"));
        replacement = settings.get("replacement", ""); // when not set or set to "", use "".
    }

    public Pattern getPattern() {
        return pattern;
    }

    public String getReplacement() {
        return replacement;
    }

    @Override
    public Reader create(Reader tokenStream) {
        return new PatternReplaceCharFilter(pattern, replacement, tokenStream);
    }
}
