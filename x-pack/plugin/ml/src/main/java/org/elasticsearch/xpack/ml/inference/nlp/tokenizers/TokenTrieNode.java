/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

class TokenTrieNode {

    private static final String EMPTY_STRING = "";

    private final Map<String, TokenTrieNode> children;

    private TokenTrieNode(Map<String, TokenTrieNode> children) {
        this.children = Objects.requireNonNull(children);
    }

    boolean isLeaf() {
        return children.isEmpty();
    }

    @Nullable
    TokenTrieNode getChild(String token) {
        return children.get(token);
    }

    private void insert(List<String> tokens) {
        if (tokens.isEmpty()) {
            return;
        }
        TokenTrieNode currentNode = this;
        int currentTokenIndex = 0;

        // find leaf
        while (currentTokenIndex < tokens.size() && currentNode.children.containsKey(tokens.get(currentTokenIndex))) {
            currentNode = currentNode.getChild(tokens.get(currentTokenIndex));
            currentTokenIndex++;
        }
        // add rest of tokens as new nodes
        while (currentTokenIndex < tokens.size()) {
            TokenTrieNode childNode = new TokenTrieNode(new HashMap<>());
            currentNode.children.put(tokens.get(currentTokenIndex), childNode);
            currentNode = childNode;
            currentTokenIndex++;
        }
    }

    static TokenTrieNode build(Collection<String> tokens, Function<String, List<String>> tokenizeFunction) {
        TokenTrieNode root = new TokenTrieNode(new HashMap<>());
        for (String token : tokens) {
            List<String> subTokens = tokenizeFunction.apply(token);
            root.insert(subTokens);
        }
        return root;
    }
}
