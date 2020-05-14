/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.parser.ParsingException;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;

import java.util.Map;

public class VerifierTests extends ESTestCase {

    private static final String INDEX_NAME = "test";

    private EqlParser parser = new EqlParser();

    private IndexResolution index = loadIndexResolution("mapping-default.json");

    private static Map<String, EsField> loadEqlMapping(String name) {
        return TypesTests.loadMapping(name);
    }

    private IndexResolution loadIndexResolution(String name) {
        return IndexResolution.valid(new EsIndex(INDEX_NAME, loadEqlMapping(name)));
    }

    private LogicalPlan accept(IndexResolution resolution, String eql) {
        PreAnalyzer preAnalyzer = new PreAnalyzer();
        Analyzer analyzer = new Analyzer(EqlTestUtils.TEST_CFG, new EqlFunctionRegistry(), new Verifier());
        return analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution));
    }

    private LogicalPlan accept(String eql) {
        return accept(index, eql);
    }

    private String error(String sql) {
        return error(index, sql);
    }

    private String error(IndexResolution resolution, String eql) {
        VerificationException e = expectThrows(VerificationException.class, () -> accept(resolution, eql));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 1 problem\nline ";
        return e.getMessage().substring(header.length());
    }

    private String errorParsing(String sql) {
        return errorParsing(index, sql);
    }

    private String errorParsing(IndexResolution resolution, String eql) {
        ParsingException e = expectThrows(ParsingException.class, () -> accept(resolution, eql));
        final String header = "line ";
        assertTrue(e.getMessage().startsWith(header));
        return e.getMessage().substring(header.length());
    }

    public void testBasicQuery() {
        accept("foo where true");
    }

    public void testQueryStartsWithNumber() {
        assertEquals("1:1: no viable alternative at input '42'", errorParsing("42 where true"));
        assertEquals("1:1: no viable alternative at input ''42''", errorParsing("'42' where true"));
    }

    public void testMissingColumn() {
        assertEquals("1:11: Unknown column [xxx]", error("foo where xxx == 100"));
    }

    public void testMisspelledColumn() {
        assertEquals("1:11: Unknown column [md4], did you mean [md5]?", error("foo where md4 == 1"));
    }

    public void testMisspelledColumnWithMultipleOptions() {
        assertEquals("1:11: Unknown column [pib], did you mean any of [pid, ppid]?", error("foo where pib == 1"));
    }

    public void testPipesUnsupported() {
        assertEquals("1:20: Pipes are not supported", errorParsing("process where true | head 6"));
    }

    public void testProcessRelationshipsUnsupported() {
        assertEquals("2:7: Process relationships are not supported",
                errorParsing("process where opcode=1 and process_name == \"csrss.exe\"\n" +
                        "  and descendant of [file where file_name == \"csrss.exe\" and opcode=0]"));
        assertEquals("2:7: Process relationships are not supported",
                errorParsing("process where process_name=\"svchost.exe\"\n" +
                        "  and child of [file where file_name=\"svchost.exe\" and opcode=0]"));
    }

    // Some functions fail with "Unsupported" message at the parse stage
    public void testArrayFunctionsUnsupported() {
        assertEquals("1:16: Unknown function [arrayContains], did you mean [stringcontains]?",
                error("registry where arrayContains(bytes_written_string_list, 'En')"));
        assertEquals("1:16: Unknown function [arraySearch]",
            error("registry where arraySearch(bytes_written_string_list, bytes_written_string, true)"));
        assertEquals("1:16: Unknown function [arrayCount]",
            error("registry where arrayCount(bytes_written_string_list, bytes_written_string, true) == 1"));
    }

    // Some functions fail with "Unknown" message at the parse stage
    public void testFunctionParsingUnknown() {
        assertEquals("1:15: Unknown function [safe]",
                error("network where safe(process_name)"));
    }

    // Test unsupported array indexes
    public void testArrayIndexesUnsupported() {
        assertEquals("1:84: Array indexes are not supported",
                errorParsing("registry where length(bytes_written_string_list) > 0 and bytes_written_string_list[0] == 'EN-us"));
    }

    // Test valid/supported queries
    public void testQueryOk() {
        // Mismatched type, still ok
        accept("process where serial_event_id = 'abcdef'");

        // Equals condition
        accept("process where serial_event_id = 1");

        // Less then condition
        accept("process where serial_event_id < 4");

        // Greater than
        accept("process where exit_code > -1");
        accept("process where -1 < exit_code");

        // Or and And/And Not
        accept("process where process_name == \"impossible name\" or (serial_event_id < 4.5 and serial_event_id >= 3.1)");
        accept("process where (serial_event_id<=8 and not serial_event_id > 7) and (opcode=3 and opcode>2)");

        // In statement
        accept("process where not (exit_code > -1)\n" +
                "  and serial_event_id in (58, 64, 69, 74, 80, 85, 90, 93, 94)");

        // Combination
        accept("file where serial_event_id == 82 and (true == (process_name in ('svchost.EXE', 'bad.exe', 'bad2.exe')))");

        // String handling
        accept("process where process_path == \"*\\\\MACHINE\\\\SAM\\\\SAM\\\\*\\\\Account\\\\Us*ers\\\\00*03E9\\\\F\"");

        // Arithmetic operators
        accept("file where serial_event_id - 1 == 81");
        accept("file where serial_event_id + 1 == 83");
        accept("file where serial_event_id * 2 == 164");
        accept("file where serial_event_id / 2 == 41");
        accept("file where serial_event_id % 40 == 2");
    }

    // Test mapping that doesn't have property event.category defined
    public void testMissingEventCategory() {
        final IndexResolution idxr = loadIndexResolution("mapping-missing-event-category.json");
        assertEquals("1:1: Unknown column [event.category]", error(idxr, "foo where true"));
    }

    public void testAliasErrors() {
        final IndexResolution idxr = loadIndexResolution("mapping-alias.json");

        // Check unsupported
        assertEquals("1:11: Cannot use field [user_name_alias] with unsupported type [alias]",
                error(idxr, "foo where user_name_alias == 'bob'"));

        // Check alias name typo
        assertEquals("1:11: Unknown column [user_name_alia], did you mean any of [user_name, user_domain]?",
                error(idxr, "foo where user_name_alia == 'bob'"));
    }

    // Test all elasticsearch numeric field types
    public void testNumeric() {
        final IndexResolution idxr = loadIndexResolution("mapping-numeric.json");
        accept(idxr, "foo where long_field == 0");
        accept(idxr, "foo where integer_field == 0");
        accept(idxr, "foo where short_field == 0");
        accept(idxr, "foo where byte_field == 0");
        accept(idxr, "foo where double_field == 0");
        accept(idxr, "foo where float_field == 0");
        accept(idxr, "foo where half_float_field == 0");
        accept(idxr, "foo where scaled_float_field == 0");

        // Test query against unsupported field type int
        assertEquals("1:11: Cannot use field [wrong_int_type_field] with unsupported type [int]",
                error(idxr, "foo where wrong_int_type_field == 0"));
    }

    public void testNoDoc() {
        final IndexResolution idxr = loadIndexResolution("mapping-nodoc.json");
        accept(idxr, "foo where description_nodoc == ''");
        // TODO: add sort test on nodoc field once we have pipes support
    }

    public void testDate() {
        final IndexResolution idxr = loadIndexResolution("mapping-date.json");
        accept(idxr, "foo where date == ''");
        accept(idxr, "foo where date == '2020-02-02'");
        accept(idxr, "foo where date == '2020-02-41'");
        accept(idxr, "foo where date == '20200241'");

        accept(idxr, "foo where date_with_format == ''");
        accept(idxr, "foo where date_with_format == '2020-02-02'");
        accept(idxr, "foo where date_with_format == '2020-02-41'");
        accept(idxr, "foo where date_with_format == '20200241'");

        accept(idxr, "foo where date_with_multi_format == ''");
        accept(idxr, "foo where date_with_multi_format == '2020-02-02'");
        accept(idxr, "foo where date_with_multi_format == '2020-02-41'");
        accept(idxr, "foo where date_with_multi_format == '20200241'");
        accept(idxr, "foo where date_with_multi_format == '11:12:13'");

        // Test query against unsupported field type date_nanos
        assertEquals("1:11: Cannot use field [date_nanos_field] with unsupported type [date_nanos]",
                error(idxr, "foo where date_nanos_field == ''"));
    }

    public void testBoolean() {
        final IndexResolution idxr = loadIndexResolution("mapping-boolean.json");
        accept(idxr, "foo where boolean_field == true");
        accept(idxr, "foo where boolean_field == 'bar'");
        accept(idxr, "foo where boolean_field == 0");
        accept(idxr, "foo where boolean_field == 123456");
    }

    public void testBinary() {
        final IndexResolution idxr = loadIndexResolution("mapping-binary.json");
        accept(idxr, "foo where blob == ''");
        accept(idxr, "foo where blob == 'bar'");
        accept(idxr, "foo where blob == 0");
        accept(idxr, "foo where blob == 123456");
    }

    public void testRange() {
        final IndexResolution idxr = loadIndexResolution("mapping-range.json");
        assertEquals("1:11: Cannot use field [integer_range_field] with unsupported type [integer_range]",
                error(idxr, "foo where integer_range_field == ''"));
        assertEquals("1:11: Cannot use field [float_range_field] with unsupported type [float_range]",
                error(idxr, "foo where float_range_field == ''"));
        assertEquals("1:11: Cannot use field [long_range_field] with unsupported type [long_range]",
                error(idxr, "foo where long_range_field == ''"));
        assertEquals("1:11: Cannot use field [double_range_field] with unsupported type [double_range]",
                error(idxr, "foo where double_range_field == ''"));
        assertEquals("1:11: Cannot use field [date_range_field] with unsupported type [date_range]",
                error(idxr, "foo where date_range_field == ''"));
        assertEquals("1:11: Cannot use field [ip_range_field] with unsupported type [ip_range]",
                error(idxr, "foo where ip_range_field == ''"));
    }

    public void testMixedSet() {
        final IndexResolution idxr = loadIndexResolution("mapping-numeric.json");
        assertEquals("1:11: 2nd argument of [long_field in (1, 'string')] must be [long], found value ['string'] type [keyword]",
            error(idxr, "foo where long_field in (1, 'string')"));
    }

    public void testObject() {
        final IndexResolution idxr = loadIndexResolution("mapping-object.json");
        accept(idxr, "foo where endgame.pid == 0");

        assertEquals("1:11: Unknown column [endgame.pi], did you mean [endgame.pid]?",
                error(idxr, "foo where endgame.pi == 0"));
    }

    public void testNested() {
        final IndexResolution idxr = loadIndexResolution("mapping-nested.json");
        assertEquals("1:11: Cannot use field [processes] type [nested] due to nested fields not being supported yet",
            error(idxr, "foo where processes == 0"));
        assertEquals("1:11: Cannot use field [processes.pid] type [long] with unsupported nested type in hierarchy (field [processes])",
            error(idxr, "foo where processes.pid == 0"));
        assertEquals("1:11: Unknown column [processe.pid], did you mean any of [processes.pid, processes.path, processes.path.keyword]?",
                error(idxr, "foo where processe.pid == 0"));
        accept(idxr, "foo where long_field == 123");
    }

    public void testGeo() {
        final IndexResolution idxr = loadIndexResolution("mapping-geo.json");
        assertEquals("1:11: Cannot use field [location] with unsupported type [geo_point]",
                error(idxr, "foo where location == 0"));
        assertEquals("1:11: Cannot use field [site] with unsupported type [geo_shape]",
                error(idxr, "foo where site == 0"));
    }

    public void testIP() {
        final IndexResolution idxr = loadIndexResolution("mapping-ip.json");
        accept(idxr, "foo where ip_addr == 0");
    }

    public void testJoin() {
        final IndexResolution idxr = loadIndexResolution("mapping-join.json");
        accept(idxr, "foo where serial_event_id == 0");
    }

    public void testMultiField() {
        final IndexResolution idxr = loadIndexResolution("mapping-multi-field.json");
        accept(idxr, "foo where multi_field.raw == 'bar'");

        assertEquals("1:11: [multi_field.english == 'bar'] cannot operate on first argument field of data type [text]: " +
                        "No keyword/multi-field defined exact matches for [english]; define one or use MATCH/QUERY instead",
                error(idxr, "foo where multi_field.english == 'bar'"));

        accept(idxr, "foo where multi_field_options.raw == 'bar'");
        accept(idxr, "foo where multi_field_options.key == 'bar'");
        accept(idxr, "foo where multi_field_ambiguous.one == 'bar'");
        accept(idxr, "foo where multi_field_ambiguous.two == 'bar'");

        assertEquals("1:11: [multi_field_ambiguous.normalized == 'bar'] cannot operate on first argument field of data type [keyword]: " +
                        "Normalized keyword field cannot be used for exact match operations",
                error(idxr, "foo where multi_field_ambiguous.normalized == 'bar'"));
        assertEquals("1:11: Cannot use field [multi_field_nested.dep_name] type [text] with unsupported nested type in hierarchy " +
                        "(field [multi_field_nested])",
                error(idxr, "foo where multi_field_nested.dep_name == 'bar'"));
        assertEquals("1:11: Cannot use field [multi_field_nested.dep_id.keyword] type [keyword] with unsupported nested type in " +
                        "hierarchy (field [multi_field_nested])",
                error(idxr, "foo where multi_field_nested.dep_id.keyword == 'bar'"));
        assertEquals("1:11: Cannot use field [multi_field_nested.end_date] type [datetime] with unsupported nested type in " +
                        "hierarchy (field [multi_field_nested])",
                error(idxr, "foo where multi_field_nested.end_date == ''"));
        assertEquals("1:11: Cannot use field [multi_field_nested.start_date] type [datetime] with unsupported nested type in " +
                        "hierarchy (field [multi_field_nested])",
                error(idxr, "foo where multi_field_nested.start_date == 'bar'"));
    }

    public void testStringFunctionWithText() {
        final IndexResolution idxr = loadIndexResolution("mapping-multi-field.json");
        assertEquals("1:15: [string(multi_field.english)] cannot operate on field " +
                "of data type [text]: No keyword/multi-field defined exact matches for [english]; " +
                "define one or use MATCH/QUERY instead",
            error(idxr, "process where string(multi_field.english) == 'foo'"));
    }
}
