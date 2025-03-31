/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.entitlement.runtime.policy.FileUtils.PATH_ORDER;
import static org.elasticsearch.entitlement.runtime.policy.FileUtils.isAbsolutePath;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class FileUtilsTests extends ESTestCase {

    public void testPathIsAbsolute() {
        var windowsNamedPipe = "\\\\.\\pipe";
        var windowsDosAbsolutePath = "C:\\temp";
        var unixAbsolutePath = "/tmp/foo";
        var unixStyleUncPath = "//C/temp";
        var uncPath = "\\\\C\\temp";
        var longPath = "\\\\?\\C:\\temp";

        var relativePath = "foo";
        var headingSlashRelativePath = "\\foo";

        assertThat(isAbsolutePath(windowsNamedPipe), is(true));
        assertThat(isAbsolutePath(windowsDosAbsolutePath), is(true));
        assertThat(isAbsolutePath(unixAbsolutePath), is(true));
        assertThat(isAbsolutePath(unixStyleUncPath), is(true));
        assertThat(isAbsolutePath(uncPath), is(true));
        assertThat(isAbsolutePath(longPath), is(true));

        assertThat(isAbsolutePath(relativePath), is(false));
        assertThat(isAbsolutePath(headingSlashRelativePath), is(false));
        assertThat(isAbsolutePath(""), is(false));
    }

    public void testPathOrderPosix() {
        assumeFalse("path ordering rules specific to non-Windows path styles", Platform.WINDOWS.isCurrent());

        // Unix-style
        // Directories come BEFORE files; note that this differs from natural lexicographical order
        assertThat(PATH_ORDER.compare("/a/b", "/a.xml"), lessThan(0));

        // Natural lexicographical order is respected in all the other cases
        assertThat(PATH_ORDER.compare("/a/b", "/a/b.txt"), lessThan(0));
        assertThat(PATH_ORDER.compare("/a/c", "/a/b.txt"), greaterThan(0));
        assertThat(PATH_ORDER.compare("/a/b", "/a/b/foo.txt"), lessThan(0));

        // Inverted-windows style
        // Directories come BEFORE files; note that this differs from natural lexicographical order
        assertThat(PATH_ORDER.compare("C:/a/b", "C:/a.xml"), lessThan(0));

        // Natural lexicographical order is respected in all the other cases
        assertThat(PATH_ORDER.compare("C:/a/b", "C:/a/b.txt"), lessThan(0));
        assertThat(PATH_ORDER.compare("C:/a/c", "C:/a/b.txt"), greaterThan(0));
        assertThat(PATH_ORDER.compare("C:/a/b", "C:/a/b/foo.txt"), lessThan(0));

        // "\" is a valid file name character on Posix, test we treat it like that
        assertThat(PATH_ORDER.compare("/a\\b", "/a/b.txt"), greaterThan(0));
    }

    public void testPathOrderWindows() {
        assumeTrue("path ordering rules specific to Windows", Platform.WINDOWS.isCurrent());

        // Directories come BEFORE files; note that this differs from natural lexicographical order
        assertThat(PATH_ORDER.compare("C:\\a\\b", "C:\\a.xml"), lessThan(0));

        // Natural lexicographical order is respected in all the other cases
        assertThat(PATH_ORDER.compare("C:\\a\\b", "C:\\a\\b.txt"), lessThan(0));
        assertThat(PATH_ORDER.compare("C:\\a\\b", "C:\\a\\b\\foo.txt"), lessThan(0));
        assertThat(PATH_ORDER.compare("C:\\a\\c", "C:\\a\\b.txt"), greaterThan(0));
    }

    public void testPathOrderingSpecialCharacters() {
        assertThat(PATH_ORDER.compare("aa\uD801\uDC28", "aa\uD801\uDC28"), is(0));
        assertThat(PATH_ORDER.compare("aa\uD801\uDC28", "aa\uD801\uDC28a"), lessThan(0));

        var s = PathUtils.getDefaultFileSystem().getSeparator();
        // Similarly to the other tests, we assert that Directories come BEFORE files, even when names are special characters
        assertThat(PATH_ORDER.compare(s + "\uD801\uDC28" + s + "b", s + "\uD801\uDC28.xml"), lessThan(0));
        assertThat(PATH_ORDER.compare(s + "\uD801\uDC28" + s + "b", s + "b.xml"), greaterThan(0));
    }
}
