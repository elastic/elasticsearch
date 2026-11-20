/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.elasticsearch.core.UpdateForV10;

/**
 * Keeps the Lucene 10 behaviour of {@code only_longest_match}. Lucene 11 removed that option from
 * {@link org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilter}. Its replacement also skips subwords that start inside
 * the longest match, so it emits fewer tokens than this filter for the same input.
 *
 * <p>Equivalent to the Lucene 10 filter with {@code reuseChars=true}, which Elasticsearch always used.
 * See <a href="https://github.com/elastic/elasticsearch/issues/163528">#163528</a>.
 */
@UpdateForV10(owner = UpdateForV10.Owner.SEARCH_RELEVANCE)  // remove when index compatibility with pre-Lucene-11 is not required
// @UpdateForV11
class XDictionaryCompoundWordTokenFilter extends CompoundWordTokenFilterBase {

    XDictionaryCompoundWordTokenFilter(
        TokenStream input,
        CharArraySet dictionary,
        int minWordSize,
        int minSubwordSize,
        int maxSubwordSize,
        boolean onlyLongestMatch
    ) {
        super(input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
    }

    @Override
    protected void decompose() {
        final int len = termAtt.length();
        for (int i = 0; i <= len - minSubwordSize; ++i) {
            CompoundToken longestMatchToken = null;
            for (int j = minSubwordSize; j <= maxSubwordSize; ++j) {
                if (i + j > len) {
                    break;
                }
                if (dictionary.contains(termAtt.buffer(), i, j)) {
                    if (onlyLongestMatch) {
                        if (longestMatchToken == null || longestMatchToken.txt.length() < j) {
                            longestMatchToken = new CompoundToken(i, j);
                        }
                    } else {
                        tokens.add(new CompoundToken(i, j));
                    }
                }
            }
            if (longestMatchToken != null) {
                tokens.add(longestMatchToken);
            }
        }
    }
}
