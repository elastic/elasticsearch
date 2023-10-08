/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

public class ContentPathTests extends ESTestCase {

    public void testAddPath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        String pathAsText = contentPath.pathAsText("baz");
        assertEquals("foo.bar.baz", pathAsText);
    }

    public void testRemovePath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.remove();
        assertEquals(0, contentPath.length());
        String pathAsText = contentPath.pathAsText("bar");
        assertEquals("bar", pathAsText);
    }

    public void testRemovePathException() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.remove();
        expectThrows(IllegalStateException.class, contentPath::remove);
    }

    public void testRootPath() {
        ContentPath contentPath = new ContentPath();
        assertEquals("root", contentPath.pathAsText("root"));
        assertEquals(0, contentPath.length());
    }

    public void testNestedPath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("root");
        contentPath.add("inner");
        assertEquals("root.inner.leaf1", contentPath.pathAsText("leaf1"));
        assertEquals("root.inner.leaf2", contentPath.pathAsText("leaf2"));
    }

    public void testBehaviourWithLongerPath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("1");
        contentPath.add("2");
        contentPath.add("3");
        contentPath.add("4");
        contentPath.add("5");
        contentPath.add("6");
        contentPath.add("7");
        contentPath.add("8");
        contentPath.add("9");
        contentPath.add("10");
        assertEquals(10, contentPath.length());
        String pathAsText = contentPath.pathAsText("11");
        assertEquals("1.2.3.4.5.6.7.8.9.10.11", pathAsText);
    }

    public void testLengthOfPath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        assertEquals(3, contentPath.length());
    }

    public void testLengthOfPathAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        assertEquals(3, contentPath.length());
        contentPath.remove();
        contentPath.remove();
        assertEquals(1, contentPath.length());
    }

    public void testPathAsText() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        assertEquals("foo.bar.baz", contentPath.pathAsText("baz"));
    }

    public void testPathTextAfterLeafRemoval() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("root");
        contentPath.add("inner");
        contentPath.add("leaf");
        contentPath.remove();
        assertEquals("root.inner.newLeaf", contentPath.pathAsText("newLeaf"));
    }

    public void testPathAsTextAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        contentPath.remove();
        contentPath.remove();
        assertEquals("foo.qux", contentPath.pathAsText("qux"));
    }

    public void testPathAsTextAfterRemoveAndMoreAdd() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.remove();
        contentPath.add("baz");
        assertEquals("foo.baz.qux", contentPath.pathAsText("qux"));
    }

    public void testPathTextAfterRootRemovalAndNewPathAdded() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("root");
        contentPath.add("inner");
        contentPath.add("leaf");
        contentPath.remove();
        contentPath.remove();
        contentPath.remove();
        contentPath.add("newRoot");
        contentPath.add("newInner");
        assertEquals("newRoot.newInner.newLeaf", contentPath.pathAsText("newLeaf"));
    }

    public void testPathTextRemovalAfterPathAsTextHasBeenCalled() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("root");
        contentPath.add("inner");
        contentPath.pathAsText("leaf");
        contentPath.remove();
        contentPath.add("newInner");
        assertEquals("root.newInner.newLeaf", contentPath.pathAsText("newLeaf"));
    }
}
