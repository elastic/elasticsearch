/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xpack.ml.inference.nlp.MultiCharSequence;

import java.util.List;
import java.util.stream.Collectors;

public class DelimitedToken {

    static DelimitedToken mergeTokens(List<DelimitedToken> tokens) {
        if (tokens.size() == 1) {
            return tokens.get(0);
        }
        int startOffSet = tokens.get(0).startOffset;
        int endOffset = tokens.get(tokens.size() - 1).endOffset;
        return new DelimitedToken(
            new MultiCharSequence(tokens.stream().map(DelimitedToken::charSequence).collect(Collectors.toList())),
            startOffSet,
            endOffset
        );
    }

    private final CharSequence charSequence;
    private final int startOffset;
    private final int endOffset;

    public DelimitedToken(CharSequence charSequence, int startOffset, int endOffset) {
        this.charSequence = charSequence;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public CharSequence charSequence() {
        return charSequence;
    }

    public int startOffset() {
        return startOffset;
    }

    public int endOffset() {
        return endOffset;
    }

}
