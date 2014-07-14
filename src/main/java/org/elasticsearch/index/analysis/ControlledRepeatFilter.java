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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.ElasticsearchException;

import java.io.IOException;

/**
 * Repeats terms based on configuration from
 * {@linkplain ControlledRepeatControllerFilter}.
 */
public final class ControlledRepeatFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posInc = addAttribute(PositionIncrementAttribute.class);
    private State state;
    private int currentRepeats;
    /**
     * Maximum number of repeats. -1 is a sentinal for not yet having read the
     * number from the token stream.
     */
    private int maxRepeats;

    protected ControlledRepeatFilter(TokenStream input) {
        super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (state != null) {
            restoreState(state);
            posInc.setPositionIncrement(0);
            currentRepeats++;
            if (currentRepeats >= maxRepeats) {
                state = null;
            }
            return true;
        }
        if (!input.incrementToken()) {
            return false;
        }
        if (maxRepeats == -1) {
            try {
                maxRepeats = Integer.parseInt(termAtt.toString());
            } catch (NumberFormatException e) {
                throw new ElasticsearchException("Could not parse repeat count in first token:  " + termAtt, e);
            }
            if (maxRepeats < 0) {
                throw new ElasticsearchException("Repeats must be > 0 but was:  " + termAtt);
            }
            if (!input.incrementToken()) {
                return false;
            }
        }
        if (maxRepeats > 0) {
            state = captureState();
            currentRepeats = 0;
        }
        return true;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        state = null;
        currentRepeats = 0;
        maxRepeats = -1;
    }
}
