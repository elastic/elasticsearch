/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CharSeqTokenTrieNodeTests extends ESTestCase {

    public void testEmpty() throws IOException {
        CharSeqTokenTrieNode root = CharSeqTokenTrieNode.build(Collections.emptyList(), s -> Arrays.asList(s.split(":")));
        assertThat(root.isLeaf(), is(true));
    }

    public void testTokensWithoutDelimiter() throws IOException {
        CharSeqTokenTrieNode root = CharSeqTokenTrieNode.build(List.of("a", "b", "c"), s -> Arrays.asList(s.split(":")));
        assertThat(root.isLeaf(), is(false));

        assertThat(root.getChild("a").isLeaf(), is(true));
        assertThat(root.getChild("b").isLeaf(), is(true));
        assertThat(root.getChild("c").isLeaf(), is(true));
        assertThat(root.getChild("d"), is(nullValue()));
    }

    public void testTokensWithDelimiter() throws IOException {
        CharSeqTokenTrieNode root = CharSeqTokenTrieNode.build(
            List.of("aa:bb:cc", "aa:bb:dd", "bb:aa:cc", "bb:bb:cc"),
            s -> Arrays.asList(s.split(":"))
        );
        assertThat(root.isLeaf(), is(false));

        // Let's look at the aa branch first
        {
            CharSeqTokenTrieNode aaNode = root.getChild("aa");
            assertThat(aaNode, is(notNullValue()));
            assertThat(aaNode.isLeaf(), is(false));
            assertThat(aaNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode bbNode = aaNode.getChild("bb");
            assertThat(bbNode, is(notNullValue()));
            assertThat(bbNode.isLeaf(), is(false));
            assertThat(bbNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode ccNode = bbNode.getChild("cc");
            assertThat(ccNode, is(notNullValue()));
            assertThat(ccNode.isLeaf(), is(true));
            assertThat(ccNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode ddNode = bbNode.getChild("dd");
            assertThat(ddNode, is(notNullValue()));
            assertThat(ddNode.isLeaf(), is(true));
            assertThat(ddNode.getChild("zz"), is(nullValue()));
        }
        // Now the bb branch
        {
            CharSeqTokenTrieNode bbNode = root.getChild("bb");
            assertThat(bbNode, is(notNullValue()));
            assertThat(bbNode.isLeaf(), is(false));
            assertThat(bbNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode aaNode = bbNode.getChild("aa");
            assertThat(aaNode, is(notNullValue()));
            assertThat(aaNode.isLeaf(), is(false));
            assertThat(aaNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode aaCcNode = aaNode.getChild("cc");
            assertThat(aaCcNode, is(notNullValue()));
            assertThat(aaCcNode.isLeaf(), is(true));
            assertThat(aaCcNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode bbBbNode = bbNode.getChild("bb");
            assertThat(bbBbNode, is(notNullValue()));
            assertThat(bbBbNode.isLeaf(), is(false));
            assertThat(bbBbNode.getChild("zz"), is(nullValue()));
            CharSeqTokenTrieNode bbCcNode = bbBbNode.getChild("cc");
            assertThat(bbCcNode, is(notNullValue()));
            assertThat(bbCcNode.isLeaf(), is(true));
            assertThat(bbCcNode.getChild("zz"), is(nullValue()));
        }
    }
}
