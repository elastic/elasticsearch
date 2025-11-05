/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.FilteredNormalizer2;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.UnicodeSet;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUNormalizer2Filter} to normalize tokens.
 * <p>The {@code name} can be used to provide the type of normalization to perform.</p>
 * <p>The {@code unicodeSetFilter} attribute can be used to provide the UniCodeSet for filtering.</p>
 */
public class IcuNormalizerTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    private final Normalizer2 normalizer;

    /**
     * Constructs an ICU normalizer token filter factory for Unicode normalization of tokens.
     *
     * @param indexSettings the index settings
     * @param environment the environment
     * @param name the filter name
     * @param settings the filter settings containing:
     *        <ul>
     *        <li>name: normalization method (default: "nfkc_cf") - e.g., "nfc", "nfkc", "nfkc_cf"</li>
     *        <li>unicode_set_filter: optional Unicode set pattern to filter which characters to normalize</li>
     *        </ul>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_icu_normalizer": {
     *     "type": "icu_normalizer",
     *     "name": "nfc"
     *   }
     * }
     * }</pre>
     */
    public IcuNormalizerTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        String method = settings.get("name", "nfkc_cf");
        Normalizer2 normalizerInstance = Normalizer2.getInstance(null, method, Normalizer2.Mode.COMPOSE);
        this.normalizer = wrapWithUnicodeSetFilter(normalizerInstance, settings);
    }

    /**
     * Creates an ICU normalizer filter that applies Unicode normalization to tokens.
     *
     * @param tokenStream the input token stream to be normalized
     * @return a new {@link org.apache.lucene.analysis.icu.ICUNormalizer2Filter} that applies normalization
     */
    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new org.apache.lucene.analysis.icu.ICUNormalizer2Filter(tokenStream, normalizer);
    }

    /**
     * Wraps a normalizer with a Unicode set filter if specified in settings.
     * This allows selective normalization of only certain characters.
     *
     * @param normalizer the base normalizer to wrap
     * @param settings the settings containing an optional unicode_set_filter parameter
     * @return the original normalizer if no filter is specified, or a {@link FilteredNormalizer2} if a filter is provided
     */
    static Normalizer2 wrapWithUnicodeSetFilter(final Normalizer2 normalizer, final Settings settings) {
        String unicodeSetFilter = settings.get("unicode_set_filter");
        if (unicodeSetFilter != null) {
            UnicodeSet unicodeSet = new UnicodeSet(unicodeSetFilter);

            unicodeSet.freeze();
            return new FilteredNormalizer2(normalizer, unicodeSet);
        }
        return normalizer;
    }
}
