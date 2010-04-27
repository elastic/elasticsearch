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

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.TokenStream;

/**
 * A TokenFilter that applies search term folding to Unicode text,
 * applying foldings from UTR#30 Character Foldings.
 * <p>
 * This filter applies the following foldings from the report to unicode text:
 * <ul>
 * <li>Accent removal
 * <li>Case folding
 * <li>Canonical duplicates folding
 * <li>Dashes folding
 * <li>Diacritic removal (including stroke, hook, descender)
 * <li>Greek letterforms folding
 * <li>Han Radical folding
 * <li>Hebrew Alternates folding
 * <li>Jamo folding
 * <li>Letterforms folding
 * <li>Math symbol folding
 * <li>Multigraph Expansions: All
 * <li>Native digit folding
 * <li>No-break folding
 * <li>Overline folding
 * <li>Positional forms folding
 * <li>Small forms folding
 * <li>Space folding
 * <li>Spacing Accents folding
 * <li>Subscript folding
 * <li>Superscript folding
 * <li>Suzhou Numeral folding
 * <li>Symbol folding
 * <li>Underline folding
 * <li>Vertical forms folding
 * <li>Width folding
 * </ul>
 * <p>
 * Additionally, Default Ignorables are removed, and text is normalized to NFKC.
 * All foldings, case folding, and normalization mappings are applied recursively
 * to ensure a fully folded and normalized result.
 * </p>
 */
public final class ICUFoldingFilter extends ICUNormalizer2Filter {
    private static final Normalizer2 normalizer = Normalizer2.getInstance(ICUFoldingFilter.class.getResourceAsStream("utr30.nrm"),
            "utr30", Normalizer2.Mode.COMPOSE);

    /**
     * Create a new ICUFoldingFilter on the specified input
     */
    public ICUFoldingFilter(TokenStream input) {
        super(input, normalizer);
    }
}
