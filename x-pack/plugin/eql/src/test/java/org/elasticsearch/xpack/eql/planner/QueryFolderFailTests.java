/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.analysis.VerificationException;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

public class QueryFolderFailTests extends AbstractQueryFolderTestCase {
    public void testPropertyEquationFilterUnsupported() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where (serial_event_id<9 and serial_event_id >= 7) or (opcode == pid)"));
        String msg = e.getMessage();
        assertEquals("Line 1:74: Comparisons against variables are not (currently) supported; offender [pid] in [==]", msg);
    }

    public void testPropertyEquationInClauseFilterUnsupported() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where opcode in (1,3) and process_name in (parent_process_name, \"SYSTEM\")"));
        String msg = e.getMessage();
        assertEquals("Line 1:52: Comparisons against variables are not (currently) supported; offender [parent_process_name] in [==]", msg);
    }

    public void testCIDRMatchNonIPField() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where cidrMatch(hostname, \"10.0.0.0/8\")"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
                "line 1:15: first argument of [cidrMatch(hostname, \"10.0.0.0/8\")] must be [ip], found value [hostname] type [text]", msg);
    }

    public void testCIDRMatchMissingValue() {
        ParsingException e = expectThrows(ParsingException.class,
                () -> plan("process where cidrMatch(source_address)"));
        String msg = e.getMessage();
        assertEquals("line 1:16: error building [cidrmatch]: expects at least two arguments", msg);
    }

    public void testCIDRMatchAgainstField() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where cidrMatch(source_address, hostname)"));
        String msg = e.getMessage();
        assertEquals("Line 1:41: Comparisons against variables are not (currently) supported; offender [hostname] in [==]", msg);
    }

    public void testCIDRMatchNonString() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where cidrMatch(source_address, 12345)"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\n" +
                "line 1:15: argument of [cidrMatch(source_address, 12345)] must be [string], found value [12345] type [integer]", msg);
    }
}
