/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.luceneWildcardToRegExp;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.wildcardToJavaPattern;

import static org.hamcrest.Matchers.is;

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

    public void testEscapedEscape() {
        assertEquals("^\\\\\\\\$", wildcardToJavaPattern("\\\\\\\\", '\\'));
    }

    public void testLuceneWildcardToRegExp() {
        assertThat(luceneWildcardToRegExp(""), is(""));
        assertThat(luceneWildcardToRegExp("*"), is(".*"));
        assertThat(luceneWildcardToRegExp("?"), is("."));
        assertThat(luceneWildcardToRegExp("\\\\"), is("\\\\"));
        assertThat(luceneWildcardToRegExp("foo?bar"), is("foo.bar"));
        assertThat(luceneWildcardToRegExp("foo*bar"), is("foo.*bar"));
        assertThat(luceneWildcardToRegExp("foo\\\\bar"), is("foo\\\\bar"));
        assertThat(luceneWildcardToRegExp("foo*bar?baz"), is("foo.*bar.baz"));
        assertThat(luceneWildcardToRegExp("foo\\*bar"), is("foo\\*bar"));
        assertThat(luceneWildcardToRegExp("foo\\?bar\\?"), is("foo\\?bar\\?"));
        assertThat(luceneWildcardToRegExp("foo\\?bar\\"), is("foo\\?bar\\\\"));
        assertThat(luceneWildcardToRegExp("[](){}^$.|+"), is("\\[\\]\\(\\)\\{\\}\\^\\$\\.\\|\\+"));
        assertThat(luceneWildcardToRegExp("foo\\\uD83D\uDC14bar"), is("foo\uD83D\uDC14bar"));
        assertThat(luceneWildcardToRegExp("foo\uD83D\uDC14bar"), is("foo\uD83D\uDC14bar"));
    }
}
