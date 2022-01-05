/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash.WILD_CARD_ID;
import static org.elasticsearch.xpack.ml.aggs.categorization.TextCategorizationTests.getTokens;
import static org.elasticsearch.xpack.ml.aggs.categorization.TextCategorizationTests.mockBigArrays;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class InnerTreeNodeTests extends ESTestCase {

    private final CategorizationTokenTree factory = new CategorizationTokenTree(3, 4, 60);
    private CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createRefHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(1L, mockBigArrays()));
    }

    @After
    public void closeRefHash() {
        bytesRefHash.close();
    }

    public void testAddText() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1, 0, 3);
        TextCategorization group = innerTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1, factory);
        assertArrayEquals(group.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "baz", "biz"));

        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 1, factory).getCategorization(),
            getTokens(bytesRefHash, "foo2", "bar", "baz", "biz")
        );
        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foo3", "bar", "baz", "biz"), 1, factory).getCategorization(),
            getTokens(bytesRefHash, "foo3", "bar", "baz", "biz")
        );
        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foo4", "bar", "baz", "biz"), 1, factory).getCategorization(),
            getTokens(bytesRefHash, "*", "bar", "baz", "biz")
        );
        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "bizzy"), 1, factory).getCategorization(),
            getTokens(bytesRefHash, "foo", "bar", "baz", "*")
        );
    }

    public void testAddTokensWithLargerIncoming() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1, 0, 3);
        TextCategorization group = innerTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 100, factory);
        assertArrayEquals(group.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "baz", "biz"));

        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 100, factory).getCategorization(),
            getTokens(bytesRefHash, "foo2", "bar", "baz", "biz")
        );
        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"), 1, factory).getCategorization(),
            getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz")
        );
        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foobigun", "bar", "baz", "biz"), 1000, factory).getCategorization(),
            getTokens(bytesRefHash, "foobigun", "bar", "baz", "biz")
        );
        assertThat(
            innerTreeNode.getCategorization(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz")).getCategorization(),
            equalTo(getTokens(bytesRefHash, "*", "bar", "baz", "biz"))
        );
    }

    public void testCollapseTinyChildren() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1000, 0, 4);
        TextCategorization group = innerTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1000, factory);
        assertArrayEquals(group.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "baz", "biz"));

        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 1000, factory).getCategorization(),
            getTokens(bytesRefHash, "foo2", "bar", "baz", "biz")
        );
        innerTreeNode.incCount(1000);
        assertArrayEquals(
            innerTreeNode.addText(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"), 1, factory).getCategorization(),
            getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz")
        );
        innerTreeNode.incCount(1);
        innerTreeNode.collapseTinyChildren();
        assertThat(innerTreeNode.hasChild(bytesRefHash.put(new BytesRef("foosmall"))), is(false));
        assertThat(innerTreeNode.hasChild(WILD_CARD_ID), is(true));
    }

    public void testMergeWith() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1000, 0, 3);
        innerTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1000, factory);
        innerTreeNode.incCount(1000);
        innerTreeNode.addText(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 1000, factory);

        expectThrows(UnsupportedOperationException.class, () -> innerTreeNode.mergeWith(new TreeNode.LeafTreeNode(1, 60)));

        TreeNode.InnerTreeNode mergeWith = new TreeNode.InnerTreeNode(1, 0, 3);
        innerTreeNode.addText(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"), 1, factory);
        innerTreeNode.incCount(1);
        innerTreeNode.addText(getTokens(bytesRefHash, "footiny", "bar", "baz", "biz"), 1, factory);

        innerTreeNode.mergeWith(mergeWith);
        assertThat(innerTreeNode.hasChild(WILD_CARD_ID), is(true));
        assertArrayEquals(
            innerTreeNode.getCategorization(getTokens(bytesRefHash, "footiny", "bar", "baz", "biz")).getCategorization(),
            getTokens(bytesRefHash, "*", "bar", "baz", "biz")
        );
    }
}
