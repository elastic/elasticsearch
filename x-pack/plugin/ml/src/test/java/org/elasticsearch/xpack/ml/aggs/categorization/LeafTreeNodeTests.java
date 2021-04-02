/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.ml.aggs.categorization.LogGroupTests.getTokens;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class LeafTreeNodeTests extends ESTestCase {

    private final TreeNodeFactory factory = new CategorizationTokenTree(10, 10, 0.6);

    public void testAddGroup() {
        TreeNode.LeafTreeNode leafTreeNode = new TreeNode.LeafTreeNode(0, 0.6);
        LogGroup group = leafTreeNode.addLog(getTokens("foo", "bar", "baz", "biz"), 1, factory);

        assertThat(group.getLogEvent(), arrayContaining(getTokens("foo", "bar", "baz", "biz")));
        assertThat(group.getCount(), equalTo(1L));
        assertThat(leafTreeNode.getAllChildrenLogGroups(), hasSize(1));
        long previousBytesUsed = leafTreeNode.ramBytesUsed();

        group = leafTreeNode.addLog(getTokens("foo", "bar", "bozo", "bizzy"), 1, factory);
        assertThat(group.getLogEvent(), arrayContaining(getTokens("foo", "bar", "bozo", "bizzy")));
        assertThat(group.getCount(), equalTo(1L));
        assertThat(leafTreeNode.getAllChildrenLogGroups(), hasSize(2));
        assertThat(leafTreeNode.ramBytesUsed(), greaterThan(previousBytesUsed));
        previousBytesUsed = leafTreeNode.ramBytesUsed();


        group = leafTreeNode.addLog(getTokens("foo", "bar", "baz", "different"), 3, factory);
        assertThat(group.getLogEvent(), arrayContaining(getTokens("foo", "bar", "baz", "*")));
        assertThat(group.getCount(), equalTo(4L));
        assertThat(leafTreeNode.getAllChildrenLogGroups(), hasSize(2));
        assertThat(previousBytesUsed, equalTo(leafTreeNode.ramBytesUsed()));
    }

    public void testMergeWith() {
        TreeNode.LeafTreeNode leafTreeNode = new TreeNode.LeafTreeNode(0, 0.6);
        leafTreeNode.mergeWith(null);
        assertThat(leafTreeNode, equalTo(new TreeNode.LeafTreeNode(0, 0.6)));

        expectThrows(UnsupportedOperationException.class, () -> leafTreeNode.mergeWith(new TreeNode.InnerTreeNode(1, 2, 3)));

        leafTreeNode.incCount(5);
        leafTreeNode.addLog(getTokens("foo", "bar", "baz", "biz"), 5, factory);

        TreeNode.LeafTreeNode toMerge = new TreeNode.LeafTreeNode(0, 0.6);
        leafTreeNode.incCount(1);
        toMerge.addLog(getTokens("foo", "bar", "baz", "bizzy"), 1, factory);
        leafTreeNode.incCount(1);
        toMerge.addLog(getTokens("foo", "bart", "bat", "built"), 1, factory);
        leafTreeNode.mergeWith(toMerge);

        assertThat(leafTreeNode.getAllChildrenLogGroups(), hasSize(2));
        assertThat(leafTreeNode.getCount(), equalTo(7L));
        assertThat(leafTreeNode.getAllChildrenLogGroups().get(0).getLogEvent(), arrayContaining(getTokens("foo", "bar", "baz", "*")));
        assertThat(leafTreeNode.getAllChildrenLogGroups().get(1).getLogEvent(), arrayContaining(getTokens("foo", "bart", "bat", "built")));
    }
}
