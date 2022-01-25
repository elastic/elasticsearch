/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.lucene;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;

import java.io.IOException;

public class StableLuceneFilterIterator implements PortableAnalyzeIterator {
    private final TokenStream stream;
    private final CharTermAttribute term;
    private final PositionIncrementAttribute posIncr;
    private final OffsetAttribute offset;
    private final TypeAttribute type;
    private final PositionLengthAttribute posLen;

    private final AnalyzeToken state = new AnalyzeToken();

    public StableLuceneFilterIterator(TokenStream stream) {
        this.stream = stream;
        term = stream.addAttribute(CharTermAttribute.class);
        posIncr = stream.addAttribute(PositionIncrementAttribute.class);
        offset = stream.addAttribute(OffsetAttribute.class);
        type = stream.addAttribute(TypeAttribute.class);
        posLen = stream.addAttribute(PositionLengthAttribute.class);
    }

    @Override
    public AnalyzeToken next() {
        try {
            boolean canIncrement = stream.incrementToken();
            if (canIncrement) {
                return currentState();
            }

            return null;
        } catch (IOException x) {
            throw new IllegalArgumentException("Unsupported token stream operation", x);
        }
    }

    private AnalyzeToken currentState() {
        return state.term(term.buffer())
            .termLen(term.length())
            .startOffset(offset.startOffset())
            .endOffset(offset.endOffset())
            .position(posIncr.getPositionIncrement())
            .positionLength(posLen.getPositionLength())
            .type(type.type());
    }

    @Override
    public AnalyzeToken reset() {
        try {
            stream.reset();
            return currentState();
        } catch (IOException x) {
            throw new IllegalArgumentException("Unsupported token stream operation", x);
        }
    }

    @Override
    public AnalyzeToken end() {
        try {
            stream.end();
            return currentState();
        } catch (IOException x) {
            throw new IllegalArgumentException("Unsupported token stream operation", x);
        }
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

}
