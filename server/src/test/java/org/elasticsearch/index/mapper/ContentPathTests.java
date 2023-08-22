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
        String[] path = contentPath.getPath();
        assertEquals("foo", path[0]);
        assertEquals("foo", contentPath.remove());
        assertNull(path[0]);
        assertEquals(0, contentPath.length());
        String pathAsText = contentPath.pathAsText("bar");
        assertEquals("bar", pathAsText);
    }

    public void testRemovePathException() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        assertEquals("foo", contentPath.remove());
        expectThrows(IndexOutOfBoundsException.class, contentPath::remove);
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
        assertEquals("baz", contentPath.remove());
        assertEquals("bar", contentPath.remove());
        assertEquals(1, contentPath.length());
    }

    public void testPathAsText() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        assertEquals("foo.bar.baz", contentPath.pathAsText("baz"));
    }

    public void testPathAsTextAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        contentPath.add("baz");
        assertEquals("baz", contentPath.remove());
        assertEquals("bar", contentPath.remove());
        assertEquals("foo.qux", contentPath.pathAsText("qux"));
    }

    public void testPathAsTextAfterRemoveAndMoreAdd() {
        ContentPath contentPath = new ContentPath();
        contentPath.add("foo");
        contentPath.add("bar");
        assertEquals("bar", contentPath.remove());
        contentPath.add("baz");
        assertEquals("foo.baz.qux", contentPath.pathAsText("qux"));
    }

    public void testAddDottedFieldName() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.addDottedFieldName("bar");
        String pathAsText = contentPath.dottedFieldName("baz");
        assertEquals("foo.bar.baz", pathAsText);
    }

    public void testRemoveDottedFieldName() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        String[] dottedFieldNameArray = contentPath.getDottedFieldName();
        assertEquals("foo", dottedFieldNameArray[0]);
        contentPath.removeDottedFieldName();
        assertNull(dottedFieldNameArray[0]);
        assertEquals(0, contentPath.dottedFieldNamelength());
        String dottedFieldName = contentPath.dottedFieldName("bar");
        assertEquals("bar", dottedFieldName);
    }

    public void testRemoveDottedFieldNameException() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.removeDottedFieldName();
        expectThrows(IndexOutOfBoundsException.class, contentPath::removeDottedFieldName);
    }

    public void testBehaviourWithLongerDottedFieldName() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("1");
        contentPath.addDottedFieldName("2");
        contentPath.addDottedFieldName("3");
        contentPath.addDottedFieldName("4");
        contentPath.addDottedFieldName("5");
        contentPath.addDottedFieldName("6");
        contentPath.addDottedFieldName("7");
        contentPath.addDottedFieldName("8");
        contentPath.addDottedFieldName("9");
        contentPath.addDottedFieldName("10");
        assertEquals(10, contentPath.dottedFieldNamelength());
        String dottedFieldName = contentPath.dottedFieldName("11");
        assertEquals("1.2.3.4.5.6.7.8.9.10.11", dottedFieldName);
    }

    public void testLengthOfDottedFieldName() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.addDottedFieldName("bar");
        contentPath.addDottedFieldName("baz");
        assertEquals(3, contentPath.dottedFieldNamelength());
    }

    public void testLengthOfDottedFieldNameAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.addDottedFieldName("bar");
        contentPath.addDottedFieldName("baz");
        assertEquals(3, contentPath.dottedFieldNamelength());
        contentPath.removeDottedFieldName();
        contentPath.removeDottedFieldName();
        assertEquals(1, contentPath.dottedFieldNamelength());
    }

    public void testDottedFieldName() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.addDottedFieldName("bar");
        assertEquals("foo.bar.baz", contentPath.dottedFieldName("baz"));
    }

    public void testDottedFieldNameAfterRemove() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.addDottedFieldName("bar");
        contentPath.addDottedFieldName("baz");
        contentPath.removeDottedFieldName();
        contentPath.removeDottedFieldName();
        assertEquals("foo.qux", contentPath.dottedFieldName("qux"));
    }

    public void testDottedFieldNameAfterRemoveAndMoreAdd() {
        ContentPath contentPath = new ContentPath();
        contentPath.addDottedFieldName("foo");
        contentPath.addDottedFieldName("bar");
        contentPath.removeDottedFieldName();
        contentPath.addDottedFieldName("baz");
        assertEquals("foo.baz.qux", contentPath.dottedFieldName("qux"));
    }
}
