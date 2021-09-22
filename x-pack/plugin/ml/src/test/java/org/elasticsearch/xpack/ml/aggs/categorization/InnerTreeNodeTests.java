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

import java.io.IOException;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash.WILD_CARD_ID;
import static org.elasticsearch.xpack.ml.aggs.categorization.TextCategorizationTests.getTokens;
import static org.elasticsearch.xpack.ml.aggs.categorization.TextCategorizationTests.mockBigArrays;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class InnerTreeNodeTests extends ESTestCase {

    private final TreeNodeFactory factory = new CategorizationTokenTree(3, 4, 60);
    private CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createRefHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(1L, mockBigArrays()));
    }

    @After
    public void closeRefHash() throws IOException {
        bytesRefHash.close();
    }

    public void testAddLog() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1, 0, 3);
        TextCategorization group = innerTreeNode.addLog(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1, factory);
        assertThat(group.getCategorization(), arrayContaining(getTokens(bytesRefHash, "foo", "bar", "baz", "biz")));

        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 1, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foo3", "bar", "baz", "biz"), 1, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foo3", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foo4", "bar", "baz", "biz"), 1, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "*", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foo", "bar", "baz", "bizzy"), 1, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foo", "bar", "baz", "*"))
        );
    }

    public void testAddLogWithLargerIncoming() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1, 0, 3);
        TextCategorization group = innerTreeNode.addLog(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 100, factory);
        assertThat(group.getCategorization(), arrayContaining(getTokens(bytesRefHash, "foo", "bar", "baz", "biz")));

        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 100, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"), 1, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foobigun", "bar", "baz", "biz"), 1000, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foobigun", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.getLogGroup(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz")).getCategorization(),
            equalTo(getTokens(bytesRefHash, "*", "bar", "baz", "biz"))
        );
    }

    public void testCollapseTinyChildren() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1000, 0, 4);
        TextCategorization group = innerTreeNode.addLog(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1000, factory);
        assertThat(group.getCategorization(), arrayContaining(getTokens(bytesRefHash, "foo", "bar", "baz", "biz")));

        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 1000, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"))
        );
        innerTreeNode.incCount(1000);
        assertThat(
            innerTreeNode.addLog(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"), 1, factory).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"))
        );
        innerTreeNode.incCount(1);
        innerTreeNode.collapseTinyChildren();
        assertThat(innerTreeNode.hasChild(bytesRefHash.put(new BytesRef("foosmall"))), is(false));
        assertThat(innerTreeNode.hasChild(WILD_CARD_ID), is(true));
    }

    public void testMergeWith() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1000, 0, 3);
        innerTreeNode.addLog(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1000, factory);
        innerTreeNode.incCount(1000);
        innerTreeNode.addLog(getTokens(bytesRefHash, "foo2", "bar", "baz", "biz"), 1000, factory);

        expectThrows(UnsupportedOperationException.class, () -> innerTreeNode.mergeWith(new TreeNode.LeafTreeNode(1, 60)));


        TreeNode.InnerTreeNode mergeWith = new TreeNode.InnerTreeNode(1, 0, 3);
        innerTreeNode.addLog(getTokens(bytesRefHash, "foosmall", "bar", "baz", "biz"), 1, factory);
        innerTreeNode.incCount(1);
        innerTreeNode.addLog(getTokens(bytesRefHash, "footiny", "bar", "baz", "biz"), 1, factory);

        innerTreeNode.mergeWith(mergeWith);
        assertThat(innerTreeNode.hasChild(WILD_CARD_ID), is(true));
        assertThat(
            innerTreeNode.getLogGroup(getTokens(bytesRefHash, "footiny", "bar", "baz", "biz")).getCategorization(),
            arrayContaining(getTokens(bytesRefHash, "*", "bar", "baz", "biz"))
        );
    }
}
