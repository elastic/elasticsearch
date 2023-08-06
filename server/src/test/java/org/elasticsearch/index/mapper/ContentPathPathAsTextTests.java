/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

public class ContentPathPathAsTextTests extends ESTestCase {

    private ContentPath contentPathWithPath(List<String> path) {
        ContentPath contentPath = new ContentPath();
        path.forEach(contentPath::add);
        return contentPath;
    }

    public void testRootPath() {
        ContentPath contentPath = contentPathWithPath(Collections.emptyList());
        assertEquals("root", contentPath.pathAsText("root"));
    }

    public void testNestedPath() {
        ContentPath contentPath = contentPathWithPath(List.of("root", "inner"));
        assertEquals("root.inner.leaf1", contentPath.pathAsText("leaf1"));
        assertEquals("root.inner.leaf2", contentPath.pathAsText("leaf2"));
    }

    public void testDeepPath() {
        ContentPath contentPath = contentPathWithPath(List.of("root", "inner1", "inner2", "inner3", "inner4", "inner5", "inner6", "inner7", "inner8", "inner9", "inner10"));
        assertEquals("root.inner1.inner2.inner3.inner4.inner5.inner6.inner7.inner8.inner9.inner10.leaf", contentPath.pathAsText("leaf"));
    }

    public void testPathTextAfterLeafRemoval() {
        ContentPath contentPath = contentPathWithPath(List.of("root", "inner", "leaf"));
        contentPath.remove();
        assertEquals("root.inner.newLeaf", contentPath.pathAsText("newLeaf"));
    }

    public void testPathTextAfterInnerRemoval() {
        ContentPath contentPath = contentPathWithPath(List.of("root", "inner", "leaf"));
        contentPath.remove();
        contentPath.remove();
        assertEquals("root.newInner", contentPath.pathAsText("newInner"));
    }

    public void testPathTextAfterRootRemoval() {
        ContentPath contentPath = contentPathWithPath(List.of("root", "inner", "leaf"));
        contentPath.remove();
        contentPath.remove();
        contentPath.remove();
        assertEquals("newRoot", contentPath.pathAsText("newRoot"));
    }

    public void testPathTextAfterRootRemovalAndNewPathAdded() {
        ContentPath contentPath = contentPathWithPath(List.of("root", "inner", "leaf"));
        contentPath.remove();
        contentPath.remove();
        contentPath.remove();
        List<String> newPath = List.of("newRoot", "newInner");
        newPath.forEach(contentPath::add);
        assertEquals("newRoot.newInner.newLeaf", contentPath.pathAsText("newLeaf"));
    }

    public void testPathTextRemovalAfterPathAsTextHasBeenCalled() {
        List<String> path = List.of("root", "inner");
        ContentPath contentPath = contentPathWithPath(path);
        contentPath.pathAsText("leaf");
        contentPath.remove();
        contentPath.add("newInner");
        assertEquals("root.newInner.newLeaf", contentPath.pathAsText("newLeaf"));
    }
}
