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

import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractCharFilterFactory;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;

import java.io.Reader;

/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter} to normalize character.
 * <p>The {@code name} can be used to provide the type of normalization to perform.</p>
 * <p>The {@code mode} can be used to provide 'compose' or 'decompose'. Default is compose.</p>
 * <p>The {@code unicodeSetFilter} attribute can be used to provide the UniCodeSet for filtering.</p>
 */
public class IcuNormalizerCharFilterFactory extends AbstractCharFilterFactory implements NormalizingCharFilterFactory {

    private final Normalizer2 normalizer;

    /**
     * Constructs an ICU normalizer character filter factory for Unicode normalization.
     * Normalization ensures text is in a consistent form for comparison and indexing.
     *
     * @param indexSettings the index settings
     * @param environment the environment
     * @param name the filter name
     * @param settings the filter settings containing:
     *        <ul>
     *        <li>name: normalization method (default: "nfkc_cf") - e.g., "nfc", "nfkc", "nfkc_cf"</li>
     *        <li>mode: normalization mode - "compose" or "decompose" (default: "compose")</li>
     *        <li>unicodeSetFilter: optional Unicode set pattern to filter which characters to normalize</li>
     *        </ul>
     * @throws IllegalArgumentException if an invalid normalization method or mode is specified
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "char_filter": {
     *   "my_icu_normalizer": {
     *     "type": "icu_normalizer",
     *     "name": "nfc",
     *     "mode": "compose"
     *   }
     * }
     * }</pre>
     */
    public IcuNormalizerCharFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        String method = settings.get("name", "nfkc_cf");
        String mode = settings.get("mode");
        if ("compose".equals(mode) == false && "decompose".equals(mode) == false) {
            mode = "compose";
        }
        Normalizer2 normalizerInstance = Normalizer2.getInstance(
            null,
            method,
            "compose".equals(mode) ? Normalizer2.Mode.COMPOSE : Normalizer2.Mode.DECOMPOSE
        );
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(normalizerInstance, settings);
    }

    /**
     * Creates an ICU normalizer character filter that normalizes the input text stream.
     *
     * @param reader the input character stream to be normalized
     * @return a new {@link ICUNormalizer2CharFilter} that applies Unicode normalization
     */
    @Override
    public Reader create(Reader reader) {
        return new ICUNormalizer2CharFilter(reader, normalizer);
    }

}
