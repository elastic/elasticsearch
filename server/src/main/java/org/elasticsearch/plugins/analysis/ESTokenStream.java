/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.Closeable;
import java.io.IOException;

public class ESTokenStream implements Closeable {
    private final TokenStream stream;
    private final CharTermAttribute term;
    private final PositionIncrementAttribute posIncr;
    private final OffsetAttribute offset;
    private final TypeAttribute type;
    private final PositionLengthAttribute posLen;

    private int lastPosition = -1;

    public ESTokenStream(TokenStream stream) {
        this.stream = stream;
        term = stream.addAttribute(CharTermAttribute.class);
        posIncr = stream.addAttribute(PositionIncrementAttribute.class);
        offset = stream.addAttribute(OffsetAttribute.class);
        type = stream.addAttribute(TypeAttribute.class);
        posLen = stream.addAttribute(PositionLengthAttribute.class);
    }

    public AnalyzeToken incrementToken() throws IOException {
        boolean canIncrement = stream.incrementToken();
        if (canIncrement) {
            if (posIncr.getPositionIncrement() > 0) {
                lastPosition = lastPosition + posIncr.getPositionIncrement();
            }
            return new AnalyzeToken(
                term.toString(),
                lastPosition,
                offset.startOffset(),
                offset.endOffset(),
                posLen.getPositionLength(),
                type.type()
            );
        }

        return null;
    }

    public void reset() throws IOException {
        stream.reset();
        lastPosition = -1;
    }

    public void end() throws IOException {
        stream.end();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
