/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

public abstract class AbstractMlTokenizer extends Tokenizer {

    protected final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    protected final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    protected final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    /**
     * The internal offset stores the offset in the potentially filtered input to the tokenizer.
     * This must be corrected before setting the offset attribute for user-visible output.
     */
    protected int nextOffset;
    protected int skippedPositions;

    protected AbstractMlTokenizer() {
    }

    @Override
    public final void end() throws IOException {
        super.end();
        // Set final offset
        int finalOffset = nextOffset + (int) input.skip(Integer.MAX_VALUE);
        int correctedFinalOffset = correctOffset(finalOffset);
        offsetAtt.setOffset(correctedFinalOffset, correctedFinalOffset);
        // Adjust any skipped tokens
        posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        nextOffset = 0;
        skippedPositions = 0;
    }
}
