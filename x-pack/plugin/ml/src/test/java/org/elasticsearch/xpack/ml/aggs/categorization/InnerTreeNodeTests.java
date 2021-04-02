/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.ml.aggs.categorization.LogGroupTests.getTokens;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class InnerTreeNodeTests extends ESTestCase {

    private final TreeNodeFactory factory = new CategorizationTokenTree(3, 4, 0.6);

    public void testAddLog() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1, 0, 3);
        LogGroup group = innerTreeNode.addLog(getTokens("foo", "bar", "baz", "biz"), 1, factory);
        assertThat(group.getLogEvent(), arrayContaining(getTokens("foo", "bar", "baz", "biz")));

        assertThat(
            innerTreeNode.addLog(getTokens("foo2", "bar", "baz", "biz"), 1, factory).getLogEvent(),
            arrayContaining(getTokens("foo2", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens("foo3", "bar", "baz", "biz"), 1, factory).getLogEvent(),
            arrayContaining(getTokens("foo3", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens("foo4", "bar", "baz", "biz"), 1, factory).getLogEvent(),
            arrayContaining(getTokens("*", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens("foo", "bar", "baz", "bizzy"), 1, factory).getLogEvent(),
            arrayContaining(getTokens("foo", "bar", "baz", "*"))
        );
    }

    public void testAddLogWithLargerIncoming() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1, 0, 3);
        LogGroup group = innerTreeNode.addLog(getTokens("foo", "bar", "baz", "biz"), 100, factory);
        assertThat(group.getLogEvent(), arrayContaining(getTokens("foo", "bar", "baz", "biz")));

        assertThat(
            innerTreeNode.addLog(getTokens("foo2", "bar", "baz", "biz"), 100, factory).getLogEvent(),
            arrayContaining(getTokens("foo2", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens("foosmall", "bar", "baz", "biz"), 1, factory).getLogEvent(),
            arrayContaining(getTokens("foosmall", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.addLog(getTokens("foobigun", "bar", "baz", "biz"), 1000, factory).getLogEvent(),
            arrayContaining(getTokens("foobigun", "bar", "baz", "biz"))
        );
        assertThat(
            innerTreeNode.getLogGroup(getTokens("foosmall", "bar", "baz", "biz")).getLogEvent(),
            equalTo(getTokens("*", "bar", "baz", "biz"))
        );
    }

    public void testCollapseTinyChildren() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1000, 0, 4);
        LogGroup group = innerTreeNode.addLog(getTokens("foo", "bar", "baz", "biz"), 1000, factory);
        assertThat(group.getLogEvent(), arrayContaining(getTokens("foo", "bar", "baz", "biz")));

        assertThat(
            innerTreeNode.addLog(getTokens("foo2", "bar", "baz", "biz"), 1000, factory).getLogEvent(),
            arrayContaining(getTokens("foo2", "bar", "baz", "biz"))
        );
        innerTreeNode.incCount(1000);
        assertThat(
            innerTreeNode.addLog(getTokens("foosmall", "bar", "baz", "biz"), 1, factory).getLogEvent(),
            arrayContaining(getTokens("foosmall", "bar", "baz", "biz"))
        );
        innerTreeNode.incCount(1);
        innerTreeNode.collapseTinyChildren();
        assertThat(innerTreeNode.hasChild(new BytesRef("foosmall")), is(false));
        assertThat(innerTreeNode.hasChild(new BytesRef("*")), is(true));
    }

    public void testMergeWith() {
        TreeNode.InnerTreeNode innerTreeNode = new TreeNode.InnerTreeNode(1000, 0, 3);
        innerTreeNode.addLog(getTokens("foo", "bar", "baz", "biz"), 1000, factory);
        innerTreeNode.incCount(1000);
        innerTreeNode.addLog(getTokens("foo2", "bar", "baz", "biz"), 1000, factory);

        expectThrows(UnsupportedOperationException.class, () -> innerTreeNode.mergeWith(new TreeNode.LeafTreeNode(1, 0.6)));


        TreeNode.InnerTreeNode mergeWith = new TreeNode.InnerTreeNode(1, 0, 3);
        innerTreeNode.addLog(getTokens("foosmall", "bar", "baz", "biz"), 1, factory);
        innerTreeNode.incCount(1);
        innerTreeNode.addLog(getTokens("footiny", "bar", "baz", "biz"), 1, factory);

        innerTreeNode.mergeWith(mergeWith);
        assertThat(innerTreeNode.hasChild(new BytesRef("*")), is(true));
        assertThat(
            innerTreeNode.getLogGroup(getTokens("footiny", "bar", "baz", "biz")).getLogEvent(),
            arrayContaining(getTokens("*", "bar", "baz", "biz"))
        );
    }
}
