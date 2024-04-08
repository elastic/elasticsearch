/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Char trie for fast string search based on characters
 */
public record CharTrie(Map<Character, CharTrie> children) {
    boolean isLeaf() {
        return children.isEmpty();
    }

    private void insert(char[] chars) {
        if (chars.length == 0) {
            return;
        }
        CharTrie currentNode = this;
        int currentTokenIndex = 0;

        // find leaf
        while (currentTokenIndex < chars.length) {
            CharTrie child = currentNode.children.get(chars[currentTokenIndex]);
            if (child == null) {
                break;
            } else {
                currentNode = child;
            }
            currentTokenIndex++;
        }
        // add rest of tokens as new nodes
        while (currentTokenIndex < chars.length) {
            CharTrie childNode = new CharTrie(new HashMap<>());
            currentNode.children.put(chars[currentTokenIndex++], childNode);
            currentNode = childNode;
        }
    }

    public static CharTrie build(Collection<String> tokens) {
        CharTrie root = new CharTrie(new HashMap<>());
        for (String token : tokens) {
            char[] chars = token.toCharArray();
            root.insert(chars);
        }
        return root;
    }
}
