/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.Normalizer2;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUFoldingFilter}.
 * Applies foldings from UTR#30 Character Foldings.
 * <p>
 * Can be filtered to handle certain characters in a specified way
 * (see http://icu-project.org/apiref/icu4j/com/ibm/icu/text/UnicodeSet.html)
 * E.g national chars that should be retained (filter : "[^åäöÅÄÖ]").
 *
 * <p>The {@code unicodeSetFilter} attribute can be used to provide the
 * UniCodeSet for filtering.
 *
 * @author kimchy (shay.banon)
 */
public class IcuFoldingTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {
    /** Store here the same Normalizer used by the lucene ICUFoldingFilter */
    private static final Normalizer2 ICU_FOLDING_NORMALIZER = ICUFoldingFilter.NORMALIZER;

    private final Normalizer2 normalizer;

    /**
     * Constructs an ICU folding token filter factory that applies case and diacritic folding.
     * Folding converts text to a normalized form for case-insensitive and accent-insensitive matching,
     * following the rules defined in Unicode Technical Report #30.
     *
     * @param indexSettings the index settings
     * @param environment the environment
     * @param name the filter name
     * @param settings the filter settings containing:
     *        <ul>
     *        <li>unicodeSetFilter: optional Unicode set pattern to specify which characters to fold
     *            (e.g., "[^åäöÅÄÖ]" to preserve Scandinavian characters)</li>
     *        </ul>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_icu_folding": {
     *     "type": "icu_folding"
     *   }
     * }
     *
     * // With character filtering
     * "filter": {
     *   "swedish_folding": {
     *     "type": "icu_folding",
     *     "unicodeSetFilter": "[^åäöÅÄÖ]"
     *   }
     * }
     * }</pre>
     */
    public IcuFoldingTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(ICU_FOLDING_NORMALIZER, settings);
    }

    /**
     * Creates an ICU folding token filter that applies Unicode folding to tokens.
     *
     * @param tokenStream the input token stream to be filtered
     * @return a new {@link org.apache.lucene.analysis.icu.ICUNormalizer2Filter} that applies folding
     */
    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new org.apache.lucene.analysis.icu.ICUNormalizer2Filter(tokenStream, normalizer);
    }

}
