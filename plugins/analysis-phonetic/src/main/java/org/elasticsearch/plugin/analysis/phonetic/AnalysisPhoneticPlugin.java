/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Elasticsearch plugin that provides phonetic analysis components.
 * This plugin enables phonetic matching by converting text to phonetic representations
 * using various algorithms such as Metaphone, Soundex, Caverphone, and others.
 */
public class AnalysisPhoneticPlugin extends Plugin implements AnalysisPlugin {

    /**
     * Provides phonetic token filters for converting text to phonetic representations.
     * Supports multiple phonetic encoding algorithms for fuzzy matching based on pronunciation.
     *
     * @return a map containing the "phonetic" token filter factory
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_phonetic": {
     *     "type": "phonetic",
     *     "encoder": "metaphone",
     *     "replace": false
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        return singletonMap("phonetic", PhoneticTokenFilterFactory::new);
    }
}
