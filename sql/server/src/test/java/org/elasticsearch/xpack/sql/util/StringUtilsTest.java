/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import org.junit.Test;

import static org.elasticsearch.xpack.sql.util.StringUtils.sqlToJavaPattern;
import static org.junit.Assert.assertEquals;

public class StringUtilsTest {

    @Test
    public void testNoRegex() {
        assertEquals("^fooBar$", sqlToJavaPattern("fooBar"));
    }

    @Test
    public void testEscapedJavaRegex() {
        assertEquals("^\\.\\d$", sqlToJavaPattern("\\.\\d"));
    }

    @Test
    public void testSimpleSqlRegex1() {
        assertEquals("^foo.bar$", sqlToJavaPattern("foo_bar"));
    }

    @Test
    public void testSimpleSqlRegex2() {
        assertEquals("^foo.*bar$", sqlToJavaPattern("foo%bar"));
    }

    @Test
    public void testMultipleSqlRegexes() {
        assertEquals("^foo.*bar.$", sqlToJavaPattern("foo%bar_"));
    }

    @Test
    public void testJavaRegexNoSqlRegex() {
        assertEquals("^foo\\.\\*bar$", sqlToJavaPattern("foo.*bar"));
    }

    @Test
    public void testMultipleRegexAndSqlRegex() {
        assertEquals("^foo\\.\\*bar\\..*$", sqlToJavaPattern("foo.*bar.%"));
    }

    @Test
    public void testComplicatedJavaRegex() {
        assertEquals("^\\^\\[\\d\\]\\.\\*\\$$", sqlToJavaPattern("^[\\d].*$"));
    }
}
