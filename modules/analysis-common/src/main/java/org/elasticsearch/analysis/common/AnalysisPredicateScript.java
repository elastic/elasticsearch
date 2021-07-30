/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
