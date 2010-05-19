/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.ibm.icu.text.Normalizer;
import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.elasticsearch.util.lucene.analysis.CharSequenceTermAttribute;

import java.io.IOException;

/**
 * Normalize token text with ICU's {@link com.ibm.icu.text.Normalizer2}
 * <p>
 * With this filter, you can normalize text in the following ways:
 * <ul>
 * <li> NFKC Normalization, Case Folding, and removing Ignorables (the default)
 * <li> Using a standard Normalization mode (NFC, NFD, NFKC, NFKD)
 * <li> Based on rules from a custom normalization mapping.
 * </ul>
 * <p>
 * If you use the defaults, this filter is a simple way to standardize Unicode text
 * in a language-independent way for search:
 * <ul>
 * <li> The case folding that it does can be seen as a replacement for
 * LowerCaseFilter.
 * <li> Ignorables such as Zero-Width Joiner and Variation Selectors are removed.
 * These are typically modifier characters that affect display.
 * </ul>
 *
 * @see com.ibm.icu.text.Normalizer2
 * @see com.ibm.icu.text.FilteredNormalizer2
 */
// LUCENE MONITOR: Once 3.1 is released use it instead
public class ICUNormalizer2Filter extends TokenFilter {

    private final TermAttribute termAtt = addAttribute(TermAttribute.class);

    private final Normalizer2 normalizer;

    private final StringBuilder buffer = new StringBuilder();

    private final CharSequenceTermAttribute charSequenceTermAtt;

    /**
     * Create a new Normalizer2Filter that combines NFKC normalization, Case
     * Folding, and removes Default Ignorables (NFKC_Casefold)
     */
    public ICUNormalizer2Filter(TokenStream input) {
        this(input, Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE));
    }

    /**
     * Create a new Normalizer2Filter with the specified Normalizer2
     *
     * @param input      stream
     * @param normalizer normalizer to use
     */
    public ICUNormalizer2Filter(TokenStream input, Normalizer2 normalizer) {
        super(input);
        this.normalizer = normalizer;
        this.charSequenceTermAtt = new CharSequenceTermAttribute(termAtt);
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            if (normalizer.quickCheck(charSequenceTermAtt) != Normalizer.YES) {
                buffer.setLength(0);
                normalizer.normalize(charSequenceTermAtt, buffer);
                termAtt.setTermBuffer(buffer.toString());
            }
            return true;
        } else {
            return false;
        }
    }
}
