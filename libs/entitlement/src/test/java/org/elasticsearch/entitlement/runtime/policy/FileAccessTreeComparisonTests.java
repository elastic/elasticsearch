/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class FileAccessTreeComparisonTests extends ESTestCase {

    public void testPathOrderPosix() {
        var pathComparator = new CaseSensitiveComparison('/').pathComparator();

        // Unix-style
        // Directories come BEFORE files; note that this differs from natural lexicographical order
        assertThat(pathComparator.compare("/a/b", "/a.xml"), lessThan(0));

        // Natural lexicographical order is respected in all the other cases
        assertThat(pathComparator.compare("/a/b", "/a/b.txt"), lessThan(0));
        assertThat(pathComparator.compare("/a/c", "/a/b.txt"), greaterThan(0));
        assertThat(pathComparator.compare("/a/b", "/a/b/foo.txt"), lessThan(0));

        // Inverted-windows style
        // Directories come BEFORE files; note that this differs from natural lexicographical order
        assertThat(pathComparator.compare("C:/a/b", "C:/a.xml"), lessThan(0));

        // Natural lexicographical order is respected in all the other cases
        assertThat(pathComparator.compare("C:/a/b", "C:/a/b.txt"), lessThan(0));
        assertThat(pathComparator.compare("C:/a/c", "C:/a/b.txt"), greaterThan(0));
        assertThat(pathComparator.compare("C:/a/b", "C:/a/b/foo.txt"), lessThan(0));

        // "\" is a valid file name character on Posix, test we treat it like that
        assertThat(pathComparator.compare("/a\\b", "/a/b.txt"), greaterThan(0));
    }

    public void testPathOrderWindows() {
        var pathComparator = new CaseInsensitiveComparison('\\').pathComparator();

        // Directories come BEFORE files; note that this differs from natural lexicographical order
        assertThat(pathComparator.compare("C:\\a\\b", "C:\\a.xml"), lessThan(0));

        // Natural lexicographical order is respected in all the other cases
        assertThat(pathComparator.compare("C:\\a\\b", "C:\\a\\b.txt"), lessThan(0));
        assertThat(pathComparator.compare("C:\\a\\b", "C:\\a\\b\\foo.txt"), lessThan(0));
        assertThat(pathComparator.compare("C:\\a\\c", "C:\\a\\b.txt"), greaterThan(0));
    }

    public void testPathOrderingSpecialCharacters() {
        var s = randomFrom('/', '\\');
        var pathComparator = (randomBoolean() ? new CaseInsensitiveComparison(s) : new CaseSensitiveComparison(s)).pathComparator();

        assertThat(pathComparator.compare("aa\uD801\uDC28", "aa\uD801\uDC28"), is(0));
        assertThat(pathComparator.compare("aa\uD801\uDC28", "aa\uD801\uDC28a"), lessThan(0));

        // Similarly to the other tests, we assert that Directories come BEFORE files, even when names are special characters
        assertThat(pathComparator.compare(s + "\uD801\uDC28" + s + "b", s + "\uD801\uDC28.xml"), lessThan(0));
        assertThat(pathComparator.compare(s + "\uD801\uDC28" + s + "b", s + "b.xml"), greaterThan(0));
    }
}
