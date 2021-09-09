/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.eql.util.StringUtils.toLikePattern;

public class StringUtilsTests extends ESTestCase {

    public void testLikePatternNoPattern() throws Exception {
        String string = "abc123";
        assertEquals(string, toLikePattern(string).exactMatch());
    }

    public void testLikePatternLikeChars() throws Exception {
        String string = "a%bc%%12_3__";
        String escape = Character.toString(1);
        LikePattern pattern = toLikePattern(string);
        assertEquals(string, pattern.exactMatch());
        assertEquals("a" + escape + "%bc" + escape + "%" + escape + "%" +
            "12" + escape + "_" + "3" + escape + "_" + escape + "_", pattern.pattern());
        assertEquals(string, pattern.asLuceneWildcard());
    }

    public void testLikePatternEqlChars() throws Exception {
        String string = "abc*123?";
        LikePattern pattern = toLikePattern(string);
        assertNull(pattern.exactMatch());
        assertEquals("abc%123_", pattern.pattern());
        assertEquals(string, pattern.asLuceneWildcard());
    }

    public void testLikePatternMixEqlAndLikeChars() throws Exception {
        String string = "abc*%123?_";
        String escape = Character.toString(1);
        LikePattern pattern = toLikePattern(string);
        assertNull(pattern.exactMatch());
        assertEquals("abc%" + escape + "%123_" + escape + "_", pattern.pattern());
        assertEquals(string, pattern.asLuceneWildcard());
    }

    public void testIsExactMatch() throws Exception {
        List<String> list = asList("abc%123", "abc_123", "abc%%123__");
        for (int i = 0; i < list.size(); i++) {
            String string = list.get(i);
            LikePattern pattern = toLikePattern(string);
            assertEquals(string, pattern.exactMatch());
        }
    }

    public void testIsNonExactMatch() throws Exception {
        List<String> list = asList("abc*123", "abc?123", "abc**123??");
        for (String string : list) {
            LikePattern pattern = toLikePattern(string);
            assertNull(pattern.exactMatch());
        }
    }
}
