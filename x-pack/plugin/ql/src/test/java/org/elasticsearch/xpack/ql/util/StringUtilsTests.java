/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.ql.util.StringUtils.wildcardToJavaPattern;

public class StringUtilsTests extends ESTestCase {

    public void testNoWildcard() {
        assertEquals("^fooBar$", wildcardToJavaPattern("fooBar", '\\'));
    }

    public void testSimpleWildcard() {
        assertEquals("^foo.bar$", wildcardToJavaPattern("foo?bar", '\\'));
        assertEquals("^foo.*bar$", wildcardToJavaPattern("foo*bar", '\\'));
    }

    public void testMultipleWildcards() {
        assertEquals("^.*foo.*bar.$", wildcardToJavaPattern("*foo*bar?", '\\'));
        assertEquals("^foo.*bar.$", wildcardToJavaPattern("foo*bar?", '\\'));
        assertEquals("^foo.*bar...$", wildcardToJavaPattern("foo*bar???", '\\'));
        assertEquals("^foo.*bar..*.$", wildcardToJavaPattern("foo*bar?*?", '\\'));
    }

    public void testDot() {
        assertEquals("^foo\\.$", wildcardToJavaPattern("foo.", '\\'));
        assertEquals("^\\..*foobar$", wildcardToJavaPattern(".*foobar", '\\'));
        assertEquals("^foo\\..*bar$", wildcardToJavaPattern("foo.*bar", '\\'));
        assertEquals("^foobar\\..*$", wildcardToJavaPattern("foobar.*", '\\'));
    }

    public void testEscapedJavaRegex() {
        assertEquals("^\\[a-zA-Z\\]$", wildcardToJavaPattern("[a-zA-Z]", '\\'));
    }

    public void testWildcard() {
        assertEquals("^foo\\?$", wildcardToJavaPattern("foo\\?", '\\'));
        assertEquals("^foo\\?bar$", wildcardToJavaPattern("foo\\?bar", '\\'));
        assertEquals("^foo\\?.$", wildcardToJavaPattern("foo\\??", '\\'));
        assertEquals("^foo\\*$", wildcardToJavaPattern("foo\\*", '\\'));
        assertEquals("^foo\\*bar$", wildcardToJavaPattern("foo\\*bar", '\\'));
        assertEquals("^foo\\*.*$", wildcardToJavaPattern("foo\\**", '\\'));

        assertEquals("^foo\\?$", wildcardToJavaPattern("foox?", 'x'));
        assertEquals("^foo\\*$", wildcardToJavaPattern("foox*", 'x'));
    }

}
