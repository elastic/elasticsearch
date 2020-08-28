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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.elasticsearch.script.ScriptContext;

/**
 * A predicate based on the current token in a TokenStream
 */
public abstract class AnalysisPredicateScript {

    /**
     * Encapsulation of the state of the current token
     */
    public static class Token {

        private final CharTermAttribute termAtt;
        private final PositionIncrementAttribute posIncAtt;
        private final PositionLengthAttribute posLenAtt;
        private final OffsetAttribute offsetAtt;
        private final TypeAttribute typeAtt;
        private final KeywordAttribute keywordAtt;

        // posInc is always 1 at the beginning of a tokenstream and the convention
        // from the _analyze endpoint is that tokenstream positions are 0-based
        private int pos = -1;

        /**
         * Create a token exposing values from an AttributeSource
         */
        public Token(AttributeSource source) {
            this.termAtt = source.addAttribute(CharTermAttribute.class);
            this.posIncAtt = source.addAttribute(PositionIncrementAttribute.class);
            this.posLenAtt = source.addAttribute(PositionLengthAttribute.class);
            this.offsetAtt = source.addAttribute(OffsetAttribute.class);
            this.typeAtt = source.addAttribute(TypeAttribute.class);
            this.keywordAtt = source.addAttribute(KeywordAttribute.class);
        }

        public void reset() {
            this.pos = -1;
        }

        public void updatePosition() {
            this.pos = this.pos + posIncAtt.getPositionIncrement();
        }

        public CharSequence getTerm() {
            return termAtt;
        }

        public int getPositionIncrement() {
            return posIncAtt.getPositionIncrement();
        }

        public int getPosition() {
            return pos;
        }

        public int getPositionLength() {
            return posLenAtt.getPositionLength();
        }

        public int getStartOffset() {
            return offsetAtt.startOffset();
        }

        public int getEndOffset() {
            return offsetAtt.endOffset();
        }

        public String getType() {
            return typeAtt.type();
        }

        public boolean isKeyword() {
            return keywordAtt.isKeyword();
        }
    }

    /**
     * Returns {@code true} if the current term matches the predicate
     */
    public abstract boolean execute(Token token);

    public interface Factory {
        AnalysisPredicateScript newInstance();
    }

    public static final String[] PARAMETERS = new String[]{ "token" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("analysis", Factory.class);

}
