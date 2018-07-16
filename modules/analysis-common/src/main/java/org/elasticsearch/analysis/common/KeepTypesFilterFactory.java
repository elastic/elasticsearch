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
 * {@value #KEEP_TYPES_KEY} setting.
 * <p>
 * Configuration options:
 * <ul>
 * <li>{@value #KEEP_TYPES_KEY} the array of words / tokens to keep.</li>
 * </ul>
 */
public class KeepTypesFilterFactory extends AbstractTokenFilterFactory {
    private final Set<String> keepTypes;
    private final boolean includeMode;
    static final String KEEP_TYPES_KEY = "types";
    static final String KEEP_TYPES_MODE = "mode";
    static final String KEEP_TYPES_MODE_INCLUDE = "include";
    static final String KEEP_TYPES_MODE_EXCLUDE = "exclude";


    KeepTypesFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        final List<String> arrayKeepTypes = settings.getAsList(KEEP_TYPES_KEY, null);
        if ((arrayKeepTypes == null)) {
            throw new IllegalArgumentException("keep_types requires `" + KEEP_TYPES_KEY + "` to be configured");
        }
        final String modeParameter = settings.get(KEEP_TYPES_MODE, KEEP_TYPES_MODE_INCLUDE).toLowerCase(Locale.ROOT);
        if (modeParameter.equals(KEEP_TYPES_MODE_INCLUDE) == false && modeParameter.equals(KEEP_TYPES_MODE_EXCLUDE) == false) {
            throw new IllegalArgumentException("`keep_types` tokenfilter mode can only be [" + KEEP_TYPES_MODE_INCLUDE + "] or ["
                    + KEEP_TYPES_MODE_EXCLUDE + "] but was [" + modeParameter + "].");
        }

        this.keepTypes = new HashSet<>(arrayKeepTypes);
        this.includeMode = modeParameter.equals(KEEP_TYPES_MODE_INCLUDE);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new TypeTokenFilter(tokenStream, keepTypes, includeMode);
    }
}
