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

import com.ibm.icu.text.Normalizer2;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;


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
    private static final Normalizer2 ICU_FOLDING_NORMALIZER = Normalizer2.getInstance(
            ICUFoldingFilter.class.getResourceAsStream("utr30.nrm"), "utr30", Normalizer2.Mode.COMPOSE);

    private final Normalizer2 normalizer;

    public IcuFoldingTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.normalizer = IcuNormalizerTokenFilterFactory.wrapWithUnicodeSetFilter(indexSettings, ICU_FOLDING_NORMALIZER, settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new org.apache.lucene.analysis.icu.ICUNormalizer2Filter(tokenStream, normalizer);
    }

}
