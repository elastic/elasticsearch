/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.ml.aggs.categorization.TextCategorizationTests.getTokens;
import static org.elasticsearch.xpack.ml.aggs.categorization.TextCategorizationTests.mockBigArrays;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class LeafTreeNodeTests extends ESTestCase {

    private final CategorizationTokenTree factory = new CategorizationTokenTree(10, 10, 60);

    private CategorizationBytesRefHash bytesRefHash;

    @Before
    public void createRefHash() {
        bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(1L, mockBigArrays()));
    }

    @After
    public void closeRefHash() {
        bytesRefHash.close();
    }

    public void testAddGroup() {
        TreeNode.LeafTreeNode leafTreeNode = new TreeNode.LeafTreeNode(0, 60);
        TextCategorization group = leafTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 1, factory);

        assertArrayEquals(group.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "baz", "biz"));
        assertThat(group.getCount(), equalTo(1L));
        assertThat(leafTreeNode.getAllChildrenTextCategorizations(), hasSize(1));
        long previousBytesUsed = leafTreeNode.ramBytesUsed();

        group = leafTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "bozo", "bizzy"), 1, factory);
        assertArrayEquals(group.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "bozo", "bizzy"));
        assertThat(group.getCount(), equalTo(1L));
        assertThat(leafTreeNode.getAllChildrenTextCategorizations(), hasSize(2));
        assertThat(leafTreeNode.ramBytesUsed(), greaterThan(previousBytesUsed));
        previousBytesUsed = leafTreeNode.ramBytesUsed();

        group = leafTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "different"), 3, factory);
        assertArrayEquals(group.getCategorization(), getTokens(bytesRefHash, "foo", "bar", "baz", "*"));
        assertThat(group.getCount(), equalTo(4L));
        assertThat(leafTreeNode.getAllChildrenTextCategorizations(), hasSize(2));
        assertThat(previousBytesUsed, equalTo(leafTreeNode.ramBytesUsed()));
    }

    public void testMergeWith() {
        TreeNode.LeafTreeNode leafTreeNode = new TreeNode.LeafTreeNode(0, 60);
        leafTreeNode.mergeWith(null);
        assertThat(leafTreeNode, equalTo(new TreeNode.LeafTreeNode(0, 60)));

        expectThrows(UnsupportedOperationException.class, () -> leafTreeNode.mergeWith(new TreeNode.InnerTreeNode(1, 2, 3)));

        leafTreeNode.incCount(5);
        leafTreeNode.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "biz"), 5, factory);

        TreeNode.LeafTreeNode toMerge = new TreeNode.LeafTreeNode(0, 60);
        leafTreeNode.incCount(1);
        toMerge.addText(getTokens(bytesRefHash, "foo", "bar", "baz", "bizzy"), 1, factory);
        leafTreeNode.incCount(1);
        toMerge.addText(getTokens(bytesRefHash, "foo", "bart", "bat", "built"), 1, factory);
        leafTreeNode.mergeWith(toMerge);

        assertThat(leafTreeNode.getAllChildrenTextCategorizations(), hasSize(2));
        assertThat(leafTreeNode.getCount(), equalTo(7L));
        assertArrayEquals(
            leafTreeNode.getAllChildrenTextCategorizations().get(0).getCategorization(),
            getTokens(bytesRefHash, "foo", "bar", "baz", "*")
        );
        assertArrayEquals(
            leafTreeNode.getAllChildrenTextCategorizations().get(1).getCategorization(),
            getTokens(bytesRefHash, "foo", "bart", "bat", "built")
        );
    }
}
