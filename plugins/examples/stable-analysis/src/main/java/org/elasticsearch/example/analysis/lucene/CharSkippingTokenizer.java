/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis.lucene;

import org.apache.lucene.analysis.util.CharTokenizer;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
/*
A tokenizer that skips configured tokens.
configured tokens are passed as list of single char strings.
 */
public class CharSkippingTokenizer extends CharTokenizer {

    private final Set<Integer> setOfChars;

    public CharSkippingTokenizer(List<String> tokenizerListOfChars) {
        this.setOfChars = tokenizerListOfChars.stream().map(s -> (int) s.charAt(0)).collect(Collectors.toSet());
    }

    @Override
    protected boolean isTokenChar(int c) {
        return setOfChars.contains(c) == false;
    }
}
