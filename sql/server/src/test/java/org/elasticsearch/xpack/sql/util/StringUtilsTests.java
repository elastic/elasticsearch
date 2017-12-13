/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.util.StringUtils.sqlToJavaPattern;

public class StringUtilsTests extends ESTestCase {

    public void testNoRegex() {
        assertEquals("^fooBar$", sqlToJavaPattern("fooBar"));
    }

    public void testEscapedJavaRegex() {
        assertEquals("^\\.\\d$", sqlToJavaPattern("\\.\\d"));
    }

    public void testSimpleSqlRegex1() {
        assertEquals("^foo.bar$", sqlToJavaPattern("foo_bar"));
    }

    public void testSimpleSqlRegex2() {
        assertEquals("^foo.*bar$", sqlToJavaPattern("foo%bar"));
    }

    public void testMultipleSqlRegexes() {
        assertEquals("^foo.*bar.$", sqlToJavaPattern("foo%bar_"));
    }

    public void testJavaRegexNoSqlRegex() {
        assertEquals("^foo\\.\\*bar$", sqlToJavaPattern("foo.*bar"));
    }

    public void testMultipleRegexAndSqlRegex() {
        assertEquals("^foo\\.\\*bar\\..*$", sqlToJavaPattern("foo.*bar.%"));
    }

    public void testComplicatedJavaRegex() {
        assertEquals("^\\^\\[\\d\\]\\.\\*\\$$", sqlToJavaPattern("^[\\d].*$"));
    }
}
