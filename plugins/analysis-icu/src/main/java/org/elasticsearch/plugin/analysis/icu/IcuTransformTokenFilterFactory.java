/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.text.Transliterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUTransformFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

/**
 * Factory for creating ICU transform token filters that apply text transformations
 * using ICU transliteration. Supports various text transformations including script
 * conversion, case mapping, and Unicode normalization.
 */
public class IcuTransformTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    private final String id;
    private final int dir;
    private final Transliterator transliterator;

    /**
     * Constructs an ICU transform token filter factory with the specified transliteration rules.
     *
     * @param indexSettings the index settings
     * @param environment the environment
     * @param name the filter name
     * @param settings the filter settings containing:
     *        <ul>
     *        <li>id: the transliterator ID (default: "Null") - e.g., "Latin-ASCII", "Katakana-Hiragana"</li>
     *        <li>dir: transliteration direction - "forward" or "reverse" (default: "forward")</li>
     *        </ul>
     * @throws IllegalArgumentException if the transliterator ID is invalid
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "filter": {
     *   "my_icu_transform": {
     *     "type": "icu_transform",
     *     "id": "Latin-ASCII"
     *   }
     * }
     *
     * // Katakana to Hiragana conversion
     * "filter": {
     *   "katakana_to_hiragana": {
     *     "type": "icu_transform",
     *     "id": "Katakana-Hiragana"
     *   }
     * }
     * }</pre>
     */
    public IcuTransformTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        this.id = settings.get("id", "Null");
        String s = settings.get("dir", "forward");
        this.dir = "forward".equals(s) ? Transliterator.FORWARD : Transliterator.REVERSE;
        this.transliterator = Transliterator.getInstance(id, dir);
    }

    /**
     * Creates an ICU transform filter that applies the configured transliteration to tokens.
     *
     * @param tokenStream the input token stream to be transformed
     * @return a new {@link ICUTransformFilter} that applies the transliteration
     */
    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ICUTransformFilter(tokenStream, transliterator);
    }

}
