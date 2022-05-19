/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.CharArrayMap;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public record CharSeqTokenTrieNode(CharArrayMap<CharSeqTokenTrieNode> children) {

    public static final CharSeqTokenTrieNode EMPTY = new CharSeqTokenTrieNode(new CharArrayMap<>(0, false));

    public CharSeqTokenTrieNode(CharArrayMap<CharSeqTokenTrieNode> children) {
        this.children = Objects.requireNonNull(children);
    }

    boolean isLeaf() {
        return children.isEmpty();
    }

    public void clear() {
        if (isLeaf()) {
            return;
        }
        for (CharSeqTokenTrieNode c : children.values()) {
            c.clear();
        }
        children.clear();
    }

    @Nullable
    CharSeqTokenTrieNode getChild(CharSequence token) {
        return children.get(token);
    }

    private void insert(List<String> tokens) {
        if (tokens.isEmpty()) {
            return;
        }
        CharSeqTokenTrieNode currentNode = this;
        int currentTokenIndex = 0;

        // find leaf
        while (currentTokenIndex < tokens.size() && currentNode.children.containsKey(tokens.get(currentTokenIndex))) {
            currentNode = currentNode.getChild(tokens.get(currentTokenIndex));
            currentTokenIndex++;
        }
        // add rest of tokens as new nodes
        while (currentTokenIndex < tokens.size()) {
            CharSeqTokenTrieNode childNode = new CharSeqTokenTrieNode(new CharArrayMap<>(1, false));
            currentNode.children.put(tokens.get(currentTokenIndex), childNode);
            currentNode = childNode;
            currentTokenIndex++;
        }
    }

    public static CharSeqTokenTrieNode build(Collection<String> tokens, CheckedFunction<String, List<String>, IOException> tokenizeFunction)
        throws IOException {
        CharSeqTokenTrieNode root = new CharSeqTokenTrieNode(new CharArrayMap<>(1, false));
        for (String token : tokens) {
            List<String> subTokens = tokenizeFunction.apply(token);
            root.insert(subTokens);
        }
        return root;
    }
}
