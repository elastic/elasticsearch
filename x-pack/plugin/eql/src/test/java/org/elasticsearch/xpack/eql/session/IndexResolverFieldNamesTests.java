/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.parser.EqlParser;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexResolverFieldNamesTests extends ESTestCase {

    private static final EqlParser parser = new EqlParser();

    public void testSimpleQueryEqual() {
        assertFieldNames(
            """
                process where serial_event_id == 1""",
            Set.of("serial_event_id.*", "serial_event_id", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testSimpleQueryHeadSix() {
        assertFieldNames("""
            process where true | head 6""", Set.of("event.category.*", "event.category", "@timestamp.*", "@timestamp"));
    }

    public void testProcessWhereFalse() {
        assertFieldNames("""
            process where false""", Set.of("event.category.*", "event.category", "@timestamp.*", "@timestamp"));
    }

    public void testProcessNameInexistent() {
        assertFieldNames(
            """
                process where process_name : "impossible name" or (serial_event_id < 4.5 and serial_event_id >= 3.1)""",
            Set.of(
                "process_name.*",
                "process_name",
                "serial_event_id.*",
                "serial_event_id",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testSerialEventIdLteAndGt() {
        assertFieldNames(
            """
                process where serial_event_id<=8 and serial_event_id > 7""",
            Set.of("serial_event_id.*", "serial_event_id", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testMinusOneLtExitCode() {
        assertFieldNames(
            """
                process where -1 < exit_code""",
            Set.of("exit_code.*", "exit_code", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testNotExitCodeGtWithHead1() {
        assertFieldNames(
            """
                process where not (exit_code > -1)
                  and serial_event_id in (58, 64, 69, 74, 80, 85, 90, 93, 94)
                | head 10""",
            Set.of(
                "exit_code.*",
                "exit_code",
                "serial_event_id.*",
                "serial_event_id",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testProcessWithMultipleConditions1() {
        assertFieldNames(
            """
                process where (serial_event_id<=8 and serial_event_id > 7) and (opcode==3 and opcode>2)""",
            Set.of(
                "opcode.*",
                "opcode",
                "serial_event_id.*",
                "serial_event_id",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testWildcardAndMultipleConditions1() {
        assertFieldNames(
            """
                file where file_path:"x"
                  and opcode in (0,1,2) and user_name:\"vagrant\"""",
            Set.of(
                "user_name.*",
                "user_name",
                "opcode.*",
                "opcode",
                "file_path.*",
                "file_path",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testSequenceOneOneMatch() {
        assertFieldNames(
            """
                sequence
                  [process where serial_event_id == 1]
                  [process where serial_event_id == 2]""",
            Set.of("serial_event_id.*", "serial_event_id", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testSequenceOneManyMany_Runs() {
        assertFieldNames(
            """
                sequence
                  [process where serial_event_id == 1]
                  [process where true] with runs=2""",
            Set.of("serial_event_id.*", "serial_event_id", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testTwoSequencesWithKeys() {
        assertFieldNames(
            """
                sequence
                  [process where true]        by unique_pid
                  [process where opcode == 1] by unique_ppid""",
            Set.of(
                "opcode.*",
                "opcode",
                "unique_ppid.*",
                "unique_ppid",
                "unique_pid.*",
                "unique_pid",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testTwoSequencesWithTwoKeys() {
        assertFieldNames(
            """
                sequence
                  [process where true]        by unique_pid,  process_path
                  [process where opcode == 1] by unique_ppid, parent_process_path""",
            Set.of(
                "opcode.*",
                "opcode",
                "unique_ppid.*",
                "unique_ppid",
                "unique_pid.*",
                "unique_pid",
                "process_path.*",
                "process_path",
                "parent_process_path.*",
                "parent_process_path",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testFourSequencesByPidWithUntil1() {
        assertFieldNames(
            """
                sequence
                  [process where opcode == 1] by unique_pid
                  [file where opcode == 0]    by unique_pid
                  [file where opcode == 0]    by unique_pid
                  [file where opcode == 0]    by unique_pid
                until
                  [file where opcode == 2]    by unique_pid""",
            Set.of("opcode.*", "opcode", "unique_pid.*", "unique_pid", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testSequencesOnDifferentEventTypesWithBy() {
        assertFieldNames(
            """
                sequence
                  [file where opcode==0 and file_name:"svchost.exe"] by unique_pid
                  [process where opcode == 1] by unique_ppid""",
            Set.of(
                "opcode.*",
                "opcode",
                "unique_ppid.*",
                "unique_ppid",
                "unique_pid.*",
                "unique_pid",
                "file_name.*",
                "file_name",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testMultipleConditions2() {
        assertFieldNames(
            """
                process where opcode == 1
                  and process_name in ("net.exe", "net1.exe")
                  and not (parent_process_name : "net.exe"
                  and process_name : "net1.exe")
                  and command_line : "*group *admin*" and command_line != \"*x*\"""",
            Set.of(
                "opcode.*",
                "opcode",
                "process_name.*",
                "process_name",
                "parent_process_name.*",
                "parent_process_name",
                "command_line.*",
                "command_line",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testTwoSequencesWithKeys2() {
        assertFieldNames(
            """
                sequence
                  [file where file_name:"lsass.exe"] by file_path,process_path
                  [process where true] by process_path,parent_process_path""",
            Set.of(
                "file_name.*",
                "file_name",
                "file_path.*",
                "file_path",
                "process_path.*",
                "process_path",
                "parent_process_path.*",
                "parent_process_path",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testEndsWithAndCondition() {
        assertFieldNames(
            """
                file where opcode==0 and serial_event_id == 88 and startsWith~("explorer.exeaAAAA", "EXPLORER.exe")""",
            Set.of(
                "opcode.*",
                "opcode",
                "serial_event_id.*",
                "serial_event_id",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testStringContains2() {
        assertFieldNames(
            """
                file where opcode==0 and stringContains("ABCDEFGHIexplorer.exeJKLMNOP", file_name)""",
            Set.of("opcode.*", "opcode", "file_name.*", "file_name", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testConcatCaseInsensitive() {
        assertFieldNames(
            "process where concat(serial_event_id, \":\", process_name, opcode) : \"x\"",
            Set.of(
                "opcode.*",
                "opcode",
                "process_name.*",
                "process_name",
                "serial_event_id.*",
                "serial_event_id",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testCidrMatch4() {
        assertFieldNames(
            """
                network where cidrMatch(source_address, "0.0.0.0/0")""",
            Set.of("source_address.*", "source_address", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testNumberStringConversion5() {
        assertFieldNames(
            """
                any where number(string(serial_event_id), 16) == 17""",
            Set.of("serial_event_id.*", "serial_event_id", "@timestamp.*", "@timestamp")
        );
    }

    public void testSimpleRegex() {
        assertFieldNames(
            "process where command_line regex \".*\"",
            Set.of("command_line.*", "command_line", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testSequenceWithOptionalUserDomain() {
        assertFieldNames(
            """
                sequence by ?user_domain [process where true] [registry where true]""",
            Set.of("user_domain.*", "user_domain", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testTwoSequencesWithTwoKeys_AndOptionals() {
        assertFieldNames(
            """
                sequence by ?x
                  [process where true]        by unique_pid,  process_path,        ?z
                  [process where opcode == 1] by unique_ppid, parent_process_path, ?w""",
            Set.of(
                "opcode.*",
                "opcode",
                "x.*",
                "x",
                "parent_process_path.*",
                "parent_process_path",
                "process_path.*",
                "process_path",
                "unique_pid.*",
                "unique_pid",
                "unique_ppid.*",
                "unique_ppid",
                "z.*",
                "z",
                "w.*",
                "w",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testOptionalDefaultNullValueFieldEqualNull() {
        assertFieldNames(
            """
                OPTIONAL where ?optional_field_default_null == null""",
            Set.of(
                "optional_field_default_null.*",
                "optional_field_default_null",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testSequenceOptionalFieldAsQueryKeys() {
        assertFieldNames(
            """
                sequence by ?x, transID
                  [ERROR where true] by ?x
                  [OPTIONAL where true] by ?y""",
            Set.of("x.*", "x", "y.*", "y", "transID.*", "transID", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testSequenceAllKeysOptional() {
        assertFieldNames(
            """
                sequence by ?process.entity_id, ?process.pid
                  [process where transID == 2]
                  [file where transID == 0] with runs=2""",
            Set.of(
                "process.entity_id.*",
                "process.entity_id",
                "process.pid.*",
                "process.pid",
                "transID.*",
                "transID",
                "event.category.*",
                "event.category",
                "@timestamp.*",
                "@timestamp"
            )
        );
    }

    public void testMultipleMissing1() {
        assertFieldNames("""
            sequence with maxspan=1s
                [ test4 where tag == "A" ]
                [ test4 where tag == "B" ]
               ![ test4 where tag == "M1"]
                [ test4 where tag == "C" ]
               ![ test4 where tag == "M2"]
                [ test4 where tag == "D" ]""", Set.of("tag.*", "tag", "event.category.*", "event.category", "@timestamp.*", "@timestamp"));
    }

    public void testWithByKey_runs() {
        assertFieldNames(
            """
                sequence by k1 with maxspan=1s
                    [ test5 where tag == "normal" ] by k2 with runs=2
                   ![ test5 where tag == "missing" ] by k2
                    [ test5 where tag == "normal" ] by k2""",
            Set.of("tag.*", "tag", "k1.*", "k1", "k2.*", "k2", "event.category.*", "event.category", "@timestamp.*", "@timestamp")
        );
    }

    public void testComplexFiltersWithSample() {
        assertFieldNames(
            """
                sample by host
                    [any where uptime > 0 and host == "doom" and (uptime > 15 or bool == true)] by os
                    [any where port > 100 and ip == "10.0.0.5" or op_sys : "REDHAT"] by op_sys
                    [any where bool == true] by os""",
            Set.of(
                "host.*",
                "host",
                "uptime.*",
                "uptime",
                "bool.*",
                "bool",
                "os.*",
                "os",
                "port.*",
                "port",
                "ip.*",
                "ip",
                "op_sys.*",
                "op_sys"
            )
        );
    }

    public void testOptionalFieldAsKeyAndMultipleConditions() {
        assertFieldNames(
            """
                sample by ?x, ?y
                    [failure where (?x == null or ?y == null) and id == 17]
                    [success where (?y == null and ?x == null) and id == 18]""",
            Set.of("x.*", "x", "y.*", "y", "id.*", "id", "event.category.*", "event.category")
        );
    }

    private void assertFieldNames(String query, Set<String> expected) {
        Set<String> fieldNames = EqlSession.fieldNames(parser.createStatement(query));
        assertThat(fieldNames, equalTo(expected));
    }
}
