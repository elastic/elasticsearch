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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.Reader;
import java.util.regex.Pattern;

public class PatternReplaceCharFilterFactory extends AbstractCharFilterFactory {

    private final Pattern pattern;
    private final String replacement;

    public PatternReplaceCharFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name);

        String sPattern = settings.get("pattern");
        if (!Strings.hasLength(sPattern)) {
            throw new IllegalArgumentException("pattern is missing for [" + name + "] char filter of type 'pattern_replace'");
        }
        pattern = Regex.compile(sPattern, settings.get("flags"));
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
