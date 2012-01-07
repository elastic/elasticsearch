/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.LinkedList;

public final class DoubleMetaphoneFilter extends TokenFilter {

    private static final String TOKEN_TYPE = "DoubleMetaphone";

    private final LinkedList<State> remainingTokens = new LinkedList<State>();
    private final DoubleMetaphone encoder;
    private final boolean inject;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);

    public DoubleMetaphoneFilter(TokenStream input, DoubleMetaphone encoder, boolean inject) {
        super(input);
        this.encoder = encoder;
        this.inject = inject;
    }

    @Override
    public boolean incrementToken() throws IOException {
        for (; ; ) {

            if (!remainingTokens.isEmpty()) {
                // clearAttributes();  // not currently necessary
                restoreState(remainingTokens.removeFirst());
                return true;
            }

            if (!input.incrementToken()) return false;

            int len = termAtt.length();
            if (len == 0) return true; // pass through zero length terms

            int firstAlternativeIncrement = inject ? 0 : posAtt.getPositionIncrement();

            String v = termAtt.toString();
            String primaryPhoneticValue = encoder.doubleMetaphone(v);
            String alternatePhoneticValue = encoder.doubleMetaphone(v, true);

            // a flag to lazily save state if needed... this avoids a save/restore when only
            // one token will be generated.
            boolean saveState = inject;

            if (primaryPhoneticValue != null && primaryPhoneticValue.length() > 0 && !primaryPhoneticValue.equals(v)) {
                if (saveState) {
                    remainingTokens.addLast(captureState());
                }
                posAtt.setPositionIncrement(firstAlternativeIncrement);
                firstAlternativeIncrement = 0;
                termAtt.setEmpty().append(primaryPhoneticValue);
                saveState = true;
            }

            if (alternatePhoneticValue != null && alternatePhoneticValue.length() > 0
                    && !alternatePhoneticValue.equals(primaryPhoneticValue)
                    && !primaryPhoneticValue.equals(v)) {
                if (saveState) {
                    remainingTokens.addLast(captureState());
                    saveState = false;
                }
                posAtt.setPositionIncrement(firstAlternativeIncrement);
                termAtt.setEmpty().append(alternatePhoneticValue);
                saveState = true;
            }

            // Just one token to return, so no need to capture/restore
            // any state, simply return it.
            if (remainingTokens.isEmpty()) {
                return true;
            }

            if (saveState) {
                remainingTokens.addLast(captureState());
            }
        }
    }

    @Override
    public void reset() throws IOException {
        input.reset();
        remainingTokens.clear();
    }
}
