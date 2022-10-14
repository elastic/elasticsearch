/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.CharArraySet;

import java.util.LinkedList;

final class TokenizerUtils {
    private TokenizerUtils() {}

    static LinkedList<DelimitedToken> splitOutNeverSplit(CharSequence input, CharTrie neverSplit, CharArraySet neverSplitSet) {
        CharTrie current = neverSplit;
        LinkedList<DelimitedToken> bigTokens = new LinkedList<>();
        int windowStart = 0;
        int neverSplitStart = 0;
        for (int i = 0; i < input.length(); i++) {
            CharTrie childNode = current.children().get(input.charAt(i));
            if (current == neverSplit && childNode != null) {
                neverSplitStart = i;
            }
            if (childNode == null) {
                if (current != neverSplit) {
                    current = neverSplit;
                }
                childNode = current.children().get(input.charAt(i));
                if (childNode != null) {
                    neverSplitStart = i;
                    current = childNode;
                }
            } else if (childNode.isLeaf()) {
                // build char seq view, verify its in never split
                CharSequence maybeNeverSplit = new CharSequenceRef(input, neverSplitStart, (i + 1) - neverSplitStart);
                if (neverSplitSet.contains(maybeNeverSplit)) {
                    if (windowStart < neverSplitStart) {
                        bigTokens.add(
                            new DelimitedToken(
                                new CharSequenceRef(input, windowStart, neverSplitStart - windowStart),
                                windowStart,
                                neverSplitStart
                            )
                        );
                    }
                    bigTokens.add(new DelimitedToken(maybeNeverSplit, neverSplitStart, i + 1));
                }
                windowStart = i + 1;
                current = neverSplit;
            } else {
                // still in potential never split
                current = childNode;
            }
        }
        int finalIndex = bigTokens.isEmpty() ? 0 : bigTokens.getLast().endOffset();
        if (finalIndex < input.length()) {
            bigTokens.add(
                new DelimitedToken(new CharSequenceRef(input, finalIndex, input.length() - finalIndex), finalIndex, input.length())
            );
        }
        return bigTokens;
    }

    static int numUtf8Bytes(int c) {
        if (c < 128) {
            return 1;
        }
        if (c < 2048) {
            return 2;
        }
        if (c < 65536) {
            return 3;
        }
        return 4;
    }

    public record CharSequenceRef(CharSequence wrapped, int offset, int len) implements CharSequence {

        public int getOffset() {
            return offset;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return wrapped.charAt(index + offset);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return wrapped.subSequence(start + offset, end + offset);
        }

        @Override
        public String toString() {
            return wrapped.subSequence(offset, offset + len).toString();
        }
    }

}
