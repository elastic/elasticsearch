/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PipelineTokenizer extends Tokenizer {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);

    private final List<AnalyzeAction.AnalyzeToken> tokens;
    private Iterator<AnalyzeAction.AnalyzeToken> tokenIterator;

    public PipelineTokenizer(List<AnalyzeAction.AnalyzeToken> tokens) {
        this.tokens = tokens;
        this.tokenIterator = tokens.listIterator();
    }

    @Override
    public void reset() throws IOException {
        tokenIterator = tokens.listIterator();
    }

    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();

        if (tokenIterator.hasNext() == false) {
            return false;
        }

        AnalyzeAction.AnalyzeToken currentToken = tokenIterator.next();

        posIncrAtt.setPositionIncrement(1);
        offsetAtt.setOffset(currentToken.getStartOffset(), currentToken.getEndOffset());
        typeAtt.setType(currentToken.getType());
        posLenAtt.setPositionLength(currentToken.getPositionLength());
        termAtt.setEmpty().append(currentToken.getTerm());

        return true;
    }
}
