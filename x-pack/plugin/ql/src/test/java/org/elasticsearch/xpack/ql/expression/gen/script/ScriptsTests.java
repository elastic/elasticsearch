/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.script;

import org.elasticsearch.test.ESTestCase;

import java.util.regex.Pattern;

public class ScriptsTests extends ESTestCase {

    public void testSplitWithMatches() {
        String input = "A1B2C3";
        Pattern pattern = Pattern.compile("[0-9]+");
        
        assertArrayEquals(new String[] {"A", "1", "B", "2", "C", "3"}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesNoMatches() {
        String input = "AxBxCx";
        Pattern pattern = Pattern.compile("[0-9]+");
        
        assertArrayEquals(new String[] {input}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesOneMatch() {
        String input = "ABC";
        Pattern pattern = Pattern.compile("ABC");
        
        assertArrayEquals(new String[] {input}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesSameMatch() {
        String input = "xxxx";
        Pattern pattern = Pattern.compile("x");
        
        assertArrayEquals(new String[] {"x","x","x","x"}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesTwoPatterns() {
        String input = "xyxy";
        Pattern pattern = Pattern.compile("x|y");
        
        assertArrayEquals(new String[] {"x","y","x","y"}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesTwoPatterns2() {
        String input = "A1B2C3";
        Pattern pattern = Pattern.compile("[0-9]{1}|[A-F]{1}");
        
        assertArrayEquals(new String[] {"A", "1", "B", "2", "C", "3"}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesTwoPatterns3() {
        String input = "A111BBB2C3";
        Pattern pattern = Pattern.compile("[0-9]+|[A-F]+");
        
        assertArrayEquals(new String[] {"A", "111", "BBB", "2", "C", "3"}, Scripts.splitWithMatches(input, pattern));
    }

    public void testSplitWithMatchesTwoPatterns4() {
        String input = "xA111BxBB2C3x";
        Pattern pattern = Pattern.compile("[0-9]+|[A-F]+");
        
        assertArrayEquals(new String[] {"x", "A", "111", "B", "x", "BB", "2", "C", "3", "x"}, Scripts.splitWithMatches(input, pattern));
    }
}
