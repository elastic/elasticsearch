/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.*;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;

import java.io.IOException;

public class PluginIteratorStream extends Tokenizer {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);

    private final PortableAnalyzeIterator iterator;

    public PluginIteratorStream(PortableAnalyzeIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        setState(iterator.reset());
    }

    @Override
    public void close() throws IOException {
        super.close();
        iterator.close();
    }

    @Override
    public void end() throws IOException {
        super.end();
        setState(iterator.end());
    }

    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();

        AnalyzeToken currentToken = iterator.next();
        if (currentToken == null) {
            return false;
        }

        setState(currentToken);
        return true;
    }

    private void setState(AnalyzeToken currentToken) {
        posIncrAtt.setPositionIncrement(currentToken.getPosition());
        offsetAtt.setOffset(currentToken.getStartOffset(), currentToken.getEndOffset());
        typeAtt.setType(currentToken.getType());
        posLenAtt.setPositionLength(currentToken.getPositionLength());
        termAtt.setEmpty().append(currentToken.getTerm());
    }
}
