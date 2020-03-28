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

    private String error(String query) {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan(query));

        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        return e.getMessage().substring(header.length());
    }

    private String errorParsing(String eql) {
        ParsingException e = expectThrows(ParsingException.class, () -> plan(eql));
        final String header = "line ";
        assertTrue(e.getMessage().startsWith(header));
        return e.getMessage().substring(header.length());
    }

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


    public void testStringContainsWrongParams() {
        assertEquals("1:16: error building [stringcontains]: expects two or three arguments",
                errorParsing("process where stringContains()"));

        assertEquals("1:16: error building [stringcontains]: expects two or three arguments",
                errorParsing("process where stringContains(process_name)"));

        assertEquals("1:15: second argument of [stringContains(process_name, 1)] must be [string], found value [1] type [integer]",
                error("process where stringContains(process_name, 1)"));

        assertEquals("1:15: third argument of [stringContains(process_name, \"foo\", 2)] must be [boolean], found value [2] type [integer]",
                error("process where stringContains(process_name, \"foo\", 2)"));

    }
}
