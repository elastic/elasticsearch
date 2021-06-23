/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.analysis.VerificationException;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

public class QueryTranslatorFailTests extends AbstractQueryTranslatorTestCase {

    private String error(String query) {
        VerificationException e = expectThrows(VerificationException.class, () -> plan(query));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        return e.getMessage().substring(header.length());
    }

    private String errorParsing(String eql) {
        Exception e = expectThrows(Exception.class, () -> plan(eql));
        assertTrue(e.getClass().getSimpleName().endsWith("ParsingException"));
        final String header = "line ";
        return e.getMessage().substring(header.length());
    }

    public void testBetweenMissingOrNullParams() {
        final String[] queries = {
                "process where between() == \"yst\"",
                "process where between(process_name) == \"yst\"",
                "process where between(process_name, \"s\") == \"yst\"",
                "process where between(null) == \"yst\"",
                "process where between(process_name, null) == \"yst\"",
                "process where between(process_name, \"s\", \"e\", false, false) == \"yst\"",
        };

        for (String query : queries) {
            ParsingException e = expectThrows(ParsingException.class,
                    () -> plan(query));
            assertEquals("line 1:16: error building [between]: expects three or four arguments", e.getMessage());
        }
    }

    public void testBetweenWrongTypeParams() {
        assertEquals("1:15: second argument of [between(process_name, 1, 2)] must be [string], found value [1] type [integer]",
                error("process where between(process_name, 1, 2)"));

        assertEquals("1:15: third argument of [between(process_name, \"s\", 2)] must be [string], found value [2] type [integer]",
                error("process where between(process_name, \"s\", 2)"));

        assertEquals("1:15: fourth argument of [between(process_name, \"s\", \"e\", 1)] must be [boolean], found value [1] type [integer]",
                error("process where between(process_name, \"s\", \"e\", 1)"));

        assertEquals("1:15: fourth argument of [between(process_name, \"s\", \"e\", \"true\")] must be [boolean], " +
                        "found value [\"true\"] type [keyword]",
                error("process where between(process_name, \"s\", \"e\", \"true\")"));
    }

    public void testCIDRMatchAgainstField() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where cidrMatch(source_address, hostname)"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
            "line 1:15: second argument of [cidrMatch(source_address, hostname)] must be a constant, received [hostname]", msg);
    }

    public void testCIDRMatchMissingValue() {
        ParsingException e = expectThrows(ParsingException.class,
            () -> plan("process where cidrMatch(source_address)"));
        String msg = e.getMessage();
        assertEquals("line 1:16: error building [cidrmatch]: expects at least two arguments", msg);
    }

    public void testCIDRMatchNonIPField() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where cidrMatch(hostname, \"10.0.0.0/8\")"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
            "line 1:15: first argument of [cidrMatch(hostname, \"10.0.0.0/8\")] must be [ip], found value [hostname] type [text]", msg);
    }

    public void testCIDRMatchNonString() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where cidrMatch(source_address, 12345)"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
            "line 1:15: second argument of [cidrMatch(source_address, 12345)] must be [string], found value [12345] type [integer]", msg);
    }

    public void testConcatWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where concat(plain_text)"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [concat(plain_text)] cannot operate on field of data type "
            + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }

    public void testEndsWithFunctionWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where endsWith(plain_text, \"foo\") == true"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [endsWith(plain_text, \"foo\")] cannot operate on first argument field of data type "
            + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }

    public void testIndexOfFunctionWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where indexOf(plain_text, \"foo\") == 1"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [indexOf(plain_text, \"foo\")] cannot operate on first argument field of data type "
            + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);

        e = expectThrows(VerificationException.class,
            () -> plan("process where indexOf(\"bla\", plain_text) == 1"));
        msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [indexOf(\"bla\", plain_text)] cannot operate on second argument field of data type "
            + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }

    public void testLengthFunctionWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where length(plain_text) > 0"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [length(plain_text)] cannot operate on field of data type [text]: No keyword/multi-field "
            + "defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }

    public void testMatchIsNotValidFunction() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where match(plain_text, \"foo.*\")"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
            "line 1:15: Unknown function [match], did you mean [cidrmatch]?", msg);
    }

    public void testNumberFunctionAlreadyNumber() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where number(pid) == 1"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: first argument of [number(pid)] must be [string], "
            + "found value [pid] type [long]", msg);
    }

    public void testNumberFunctionFloatBase() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where number(process_name, 1.0) == 1"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: second argument of [number(process_name, 1.0)] must be [integer], "
            + "found value [1.0] type [double]", msg);

    }

    public void testNumberFunctionNonString() {
        VerificationException e = expectThrows(VerificationException.class,
            () -> plan("process where number(plain_text) == 1"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [number(plain_text)] cannot operate on first argument field of data type "
            + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);

    }

    public void testPropertyEquationFilterUnsupported() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where (serial_event_id<9 and serial_event_id >= 7) or (opcode == pid)"));
        String msg = e.getMessage();
        assertEquals("Line 1:74: Comparisons against fields are not (currently) supported; offender [pid] in [==]", msg);
    }

    public void testPropertyEquationInClauseFilterUnsupported() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where opcode in (1,3) and process_name in (parent_process_name, \"SYSTEM\")"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:35: Comparisons against fields are not (currently) supported; " +
                "offender [parent_process_name] in [process_name in (parent_process_name, \"SYSTEM\")]", msg);
    }

    public void testSequenceWithBeforeBy() {
        String msg = errorParsing("sequence with maxspan=1s by key [a where true] [b where true]");
        assertEquals("1:2: Please specify sequence [by] before [with] not after", msg);
    }

    public void testSequenceWithNoTimeUnit() {
        String msg = errorParsing("sequence with maxspan=30 [a where true] [b where true]");
        assertEquals("1:24: No time unit specified, did you mean [s] as in [30s]?", msg);
    }

    public void testStartsWithFunctionWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where startsWith(plain_text, \"foo\") == true"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [startsWith(plain_text, \"foo\")] cannot operate on first argument field of data type "
                + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }

    public void testStringContainsWrongParams() {
        assertEquals("1:16: error building [stringcontains]: expects exactly two arguments",
                errorParsing("process where stringContains()"));

        assertEquals("1:16: error building [stringcontains]: expects exactly two arguments",
                errorParsing("process where stringContains(process_name)"));

        assertEquals("1:15: second argument of [stringContains(process_name, 1)] must be [string], found value [1] type [integer]",
                error("process where stringContains(process_name, 1)"));
    }

    public void testWildcardNotEnoughArguments() {
        ParsingException e = expectThrows(ParsingException.class,
                () -> plan("process where wildcard(process_name)"));
        String msg = e.getMessage();
        assertEquals("line 1:16: error building [wildcard]: expects at least two arguments", msg);
    }

    public void testWildcardAgainstVariable() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where wildcard(process_name, parent_process_name)"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: second argument of [wildcard(process_name, parent_process_name)] " +
                "must be a constant, received [parent_process_name]", msg);
    }

    public void testWildcardWithNumericPattern() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where wildcard(process_name, 1)"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
                "line 1:15: second argument of [wildcard(process_name, 1)] must be [string], found value [1] type [integer]", msg);
    }

    public void testWildcardWithNumericField() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where wildcard(pid, \"*.exe\")"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
                "line 1:15: first argument of [wildcard(pid, \"*.exe\")] must be [string], found value [pid] type [long]", msg);
    }
}
