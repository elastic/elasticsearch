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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

/**
 * A token filter that generates unique tokens. Can remove unique tokens only on the same position increments as well.
 *
 * @deprecated this filter an old implementation superseded by {@link UniqueTokenFilter} only here for bwc reasons and should not be used
 * any more. For details see https://github.com/elastic/elasticsearch/issues/35411
 */
// TODO remove this filter in 9.0.0
@Deprecated
class XUniqueTokenFilter extends TokenFilter {

    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncAttribute = addAttribute(PositionIncrementAttribute.class);

    private final CharArraySet previous = new CharArraySet(8, false);
    private final boolean onlyOnSamePosition;

    XUniqueTokenFilter(TokenStream in, boolean onlyOnSamePosition) {
        super(in);
        this.onlyOnSamePosition = onlyOnSamePosition;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        while (input.incrementToken()) {
            final char term[] = termAttribute.buffer();
            final int length = termAttribute.length();

            final boolean duplicate;
            if (onlyOnSamePosition) {
                final int posIncrement = posIncAttribute.getPositionIncrement();
                if (posIncrement > 0) {
                    previous.clear();
                }

                duplicate = (posIncrement == 0 && previous.contains(term, 0, length));
            } else {
                duplicate = previous.contains(term, 0, length);
            }

            // clone the term, and add to the set of seen terms.
            char saved[] = new char[length];
            System.arraycopy(term, 0, saved, 0, length);
            previous.add(saved);

            if (duplicate == false) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final void reset() throws IOException {
        super.reset();
        previous.clear();
    }
}
