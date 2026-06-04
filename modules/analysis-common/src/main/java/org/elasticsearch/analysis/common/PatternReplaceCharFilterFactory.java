/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractCharFilterFactory;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;

import java.io.Reader;
import java.util.regex.Pattern;

public class PatternReplaceCharFilterFactory extends AbstractCharFilterFactory implements NormalizingCharFilterFactory {

    private final Pattern pattern;
    private final String replacement;
    private final Object sharingKey;

    PatternReplaceCharFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        String sPattern = settings.get("pattern");
        if (Strings.hasLength(sPattern) == false) {
            throw new IllegalArgumentException("pattern is missing for [" + name + "] char filter of type 'pattern_replace'");
        }
        pattern = Regex.compile(sPattern, settings.get("flags"));
        replacement = settings.get("replacement", ""); // when not set or set to "", use "".
        // Pattern uses identity-equality, so capture (pattern, flags) source strings for the key.
        this.sharingKey = new Key(pattern.pattern(), pattern.flags(), replacement);
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

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(String pattern, int flags, String replacement) {}
}
