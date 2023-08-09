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
        contentPath.add("baz");
        String[] path = contentPath.getPath();
        assertEquals(path[0], "foo");
        assertEquals(path[1], "bar");
        assertEquals(path[2], "baz");
    }

    public void testRemovePath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        String[] path = contentPath.getPath();
        assertEquals(path[0], "foo");
        contentPath.remove();
        assertNull(path[0]);
    }

    public void testRemovePathException() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.remove();
        expectThrows(IndexOutOfBoundsException.class, () -> contentPath.remove());
    }

    public void testExpandpath() {
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
        String[] path = contentPath.getPath();
        assertEquals(path.length, 10);
        contentPath.add("10");
        // retrieve new path array
        path = contentPath.getPath();
        assertEquals(path.length, 20);
    }

    public void testLenghtOfPath() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        assertEquals(contentPath.length(), 3);
    }

    public void testLenghtOfPathAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        assertEquals(contentPath.length(), 3);
        contentPath.remove();
        contentPath.remove();
        assertEquals(contentPath.length(), 1);
    }

    public void testPathAsText() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        assertEquals(contentPath.pathAsText("baz"), "foo.bar.baz");
    }

    public void testPathAsTextAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        contentPath.remove();
        contentPath.remove();
        assertEquals(contentPath.pathAsText("qux"), "foo.qux");
    }

    public void testPathAsTextAfterRemoveAndMoreAdd() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.remove();
        contentPath.add("baz");
        assertEquals(contentPath.pathAsText("qux"), "foo.baz.qux");
    }
}
