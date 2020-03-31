/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.analysis.VerificationException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

public class QueryFolderFailTests extends AbstractQueryFolderTestCase {
    public void testPropertyEquationFilterUnsupported() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where (serial_event_id<9 and serial_event_id >= 7) or (opcode == pid)"));
        String msg = e.getMessage();
        assertEquals("Line 1:74: Comparisons against variables are not (currently) supported; offender [pid] in [==]", msg);
    }

    public void testPropertyEquationInClauseFilterUnsupported() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where opcode in (1,3) and process_name in (parent_process_name, \"SYSTEM\")"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:35: Comparisons against variables are not (currently) supported; " +
            "offender [parent_process_name] in [process_name in (parent_process_name, \"SYSTEM\")]", msg);
    }

    public void testLengthFunctionWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where length(plain_text) > 0"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [length(plain_text)] cannot operate on field of data type [text]: No keyword/multi-field "
                + "defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }

    public void testStartsWithFunctionWithInexact() {
        VerificationException e = expectThrows(VerificationException.class,
                () -> plan("process where startsWith(plain_text, \"foo\") == true"));
        String msg = e.getMessage();
        assertEquals("Found 1 problem\nline 1:15: [startsWith(plain_text, \"foo\")] cannot operate on first argument field of data type "
                + "[text]: No keyword/multi-field defined exact matches for [plain_text]; define one or use MATCH/QUERY instead", msg);
    }
}
