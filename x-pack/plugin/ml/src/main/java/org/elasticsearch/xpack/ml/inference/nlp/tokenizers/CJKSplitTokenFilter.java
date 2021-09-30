/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;
import java.util.LinkedList;
import java.util.PrimitiveIterator;

public class CJKSplitTokenFilter extends TokenFilter {
    protected final LinkedList<CJKToken> tokens;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    protected final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private State current;

    public CJKSplitTokenFilter(TokenStream input) {
        super(input);
        this.tokens = new LinkedList<>();
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokens.clear();
        current = null;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        // TODO seems like this + lowercase + tokenize cjk + wordpiece could all be the same thing....
        if (tokens.isEmpty() == false) {
            assert current != null;
            CJKToken token = tokens.removeFirst();
            restoreState(current); // keep all other attributes untouched
            termAtt.setEmpty().append(token.txt);
            offsetAtt.setOffset(token.startOffset, token.endOffset);
            return true;
        }

        current = null; // not really needed, but for safety
        if (input.incrementToken()) {
            boolean foundCjk = false;
            int start = 0;
            int length = 0;
            for (PrimitiveIterator.OfInt it = termAtt.codePoints().iterator(); it.hasNext();) {
                int cp = it.next();
                if (isCjkChar(cp)) {
                    foundCjk = true;
                    if (length > 0) {
                        tokens.add(new CJKToken(start, length));
                        start += length;
                    }
                    tokens.add(new CJKToken(start, Character.charCount(cp)));
                    start += Character.charCount(cp);
                    length = 0;
                } else {
                    length += Character.charCount(cp);
                }
            }
            if (length > 0 && foundCjk) {
                tokens.add(new CJKToken(start, length));
            } else {
                tokens.clear();
            }
            if (tokens.isEmpty() == false) {
                current = captureState();
                CJKToken token = tokens.removeFirst();
                termAtt.setEmpty().append(token.txt);
                offsetAtt.setOffset(token.startOffset, token.endOffset);
            }
            // return original token:
            return true;
        }
        return false;
    }

    private static boolean isCjkChar(int codePoint) {
        // https://en.wikipedia.org/wiki/CJK_Unified_Ideographs_(Unicode_block)
        Character.UnicodeBlock block = Character.UnicodeBlock.of(codePoint);
        return Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_C.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_D.equals(block)
            || Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_E.equals(block)
            || Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT.equals(block);
    }

    class CJKToken {
        public final CharSequence txt;
        public final int startOffset, endOffset;

        /**
         * Construct the compound token based on a slice of the current {@link
         * CJKToken#termAtt}.
         */
        CJKToken(int offset, int length) {
            this.txt = CJKSplitTokenFilter.this.termAtt.subSequence(offset, offset + length);

            // offsets of the original word
            this.startOffset = CJKSplitTokenFilter.this.offsetAtt.startOffset();
            this.endOffset = CJKSplitTokenFilter.this.offsetAtt.endOffset();
        }
    }
}
