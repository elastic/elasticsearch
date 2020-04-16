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

import com.ibm.icu.text.FilteredNormalizer2;
import com.ibm.icu.text.Normalizer2;
import com.ibm.icu.text.UnicodeSet;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;


/**
 * Uses the {@link org.apache.lucene.analysis.icu.ICUNormalizer2Filter} to normalize tokens.
 * <p>The {@code name} can be used to provide the type of normalization to perform.</p>
 * <p>The {@code unicodeSetFilter} attribute can be used to provide the UniCodeSet for filtering.</p>
 */
public class IcuNormalizerTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    private final Normalizer2 normalizer;

    public IcuNormalizerTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        String method = settings.get("name", "nfkc_cf");
        Normalizer2 normalizer = Normalizer2.getInstance(null, method, Normalizer2.Mode.COMPOSE);
        this.normalizer = wrapWithUnicodeSetFilter(indexSettings, normalizer, settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new org.apache.lucene.analysis.icu.ICUNormalizer2Filter(tokenStream, normalizer);
    }

    static Normalizer2 wrapWithUnicodeSetFilter(final IndexSettings indexSettings,
                                                final Normalizer2 normalizer,
                                                final Settings settings) {
        String unicodeSetFilter = settings.get("unicode_set_filter");
        if (unicodeSetFilter != null) {
            UnicodeSet unicodeSet = new UnicodeSet(unicodeSetFilter);

            unicodeSet.freeze();
            return new FilteredNormalizer2(normalizer, unicodeSet);
        }
        return normalizer;
    }
}
