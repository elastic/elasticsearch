/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.TypeTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A {@link TokenFilterFactory} for {@link TypeTokenFilter}. This filter only
 * keep tokens that are contained in the set configured via
 * {@value #KEEP_TYPES_MODE_KEY} setting.
 * <p>
 * Configuration options:
 * <ul>
 * <li>{@value #KEEP_TYPES_KEY} the array of words / tokens.</li>
 * <li>{@value #KEEP_TYPES_MODE_KEY} whether to keep ("include") or discard
 * ("exclude") the specified token types.</li>
 * </ul>
 */
public class KeepTypesFilterFactory extends AbstractTokenFilterFactory {
    private final Set<String> keepTypes;
    private final KeepTypesMode includeMode;
    static final String KEEP_TYPES_KEY = "types";
    static final String KEEP_TYPES_MODE_KEY = "mode";

    enum KeepTypesMode {
        INCLUDE,
        EXCLUDE;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }

        private static KeepTypesMode fromString(String modeString) {
            String lc = modeString.toLowerCase(Locale.ROOT);
            if (lc.equals("include")) {
                return INCLUDE;
            } else if (lc.equals("exclude")) {
                return EXCLUDE;
            } else {
                throw new IllegalArgumentException(
                    "`keep_types` tokenfilter mode can only be ["
                        + KeepTypesMode.INCLUDE
                        + "] or ["
                        + KeepTypesMode.EXCLUDE
                        + "] but was ["
                        + modeString
                        + "]."
                );
            }
        }
    }

    KeepTypesFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name, settings);

        final List<String> arrayKeepTypes = settings.getAsList(KEEP_TYPES_KEY, null);
        if ((arrayKeepTypes == null)) {
            throw new IllegalArgumentException("keep_types requires `" + KEEP_TYPES_KEY + "` to be configured");
        }
        this.includeMode = KeepTypesMode.fromString(settings.get(KEEP_TYPES_MODE_KEY, "include"));
        this.keepTypes = new HashSet<>(arrayKeepTypes);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new TypeTokenFilter(tokenStream, keepTypes, includeMode == KeepTypesMode.INCLUDE);
    }
}
