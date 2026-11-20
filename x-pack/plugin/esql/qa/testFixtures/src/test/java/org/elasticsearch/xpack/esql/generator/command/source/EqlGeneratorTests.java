/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.QuerySchema;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.ValidationResult;
import org.elasticsearch.xpack.esql.generator.command.source.EqlGenerator.Branch;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation.Mode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class EqlGeneratorTests extends ESTestCase {

    private static final String INDEX = "events";
    private static final Pattern EQL_BRANCH = Pattern.compile(
        "EQL "
            + INDEX
            + " (\"(?:[^\"\\\\]|\\\\.)*\")(?: METADATA ((?:_index|_id|_source)(?:, (?:_index|_id|_source))*))?(?: WITH \\{([^}]*)})?"
    );
    private static final Pattern MV_COUNT = Pattern.compile("MV_COUNT\\(`([^`]+)`\\)");
    private static final Pattern SIZE = Pattern.compile("\"size\": (\\d+)");
    private static final Pattern HEAD_OR_TAIL = Pattern.compile(" \\| (?:head|tail) (\\d+)$");

    private static final List<Column> COLUMNS = List.of(
        new Column("@timestamp", "date", List.of()),
        new Column("host", "keyword", List.of()),
        new Column("pid", "long", List.of()),
        new Column("ok", "boolean", List.of()),
        new Column("source.ip", "ip", List.of()),
        new Column("tags", "keyword", List.of()),
        new Column("first-name", "keyword", List.of()),
        new Column("message", "text", List.of()),
        new Column("_source", "_source", List.of())
    );
    private static final List<List<Object>> ROWS = List.of(
        row("2026-07-22T10:00:00.000Z", "web \"1\"", 100, true, "10.0.0.1", List.of("a", "b"), "x", "hello"),
        row("2026-07-22T10:00:01.000Z", "db\\2", 200, false, "10.0.0.2", "c", "y", "world")
    );

    public void testQuoteStringEscapesEsqlAndEqlSpecialCharacters() {
        assertThat(EqlGenerator.quoteString("a\"b\\c\nd\re\tf"), equalTo("\"a\\\"b\\\\c\\nd\\re\\tf\""));
    }

    public void testOnlyKeywordFreeIdentifierPathsAreEqlFieldNames() {
        for (String name : List.of("@timestamp", "event.category", "process_name", "source.ip", "a1._b", "event.By")) {
            assertTrue(name, EqlGenerator.isEqlFieldName(name));
        }
        for (String name : List.of("first-name", "1abc", "sequence", "event.by", "a b", "a..b", "na`me")) {
            assertFalse(name, EqlGenerator.isEqlFieldName(name));
        }
    }

    public void testGeneratesEveryModeAndRecordsItsShape() {
        assumeEqlCommand();
        Set<Mode> modes = EnumSet.noneOf(Mode.class);
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2)));
            assertThat(description.commandString(), description.commandName(), equalTo(EqlGenerator.EQL));
            Branch branch = branch(description);
            String[] parsed = parseBranches(description.commandString()).getFirst();
            String eql = unescapeStringLiteral(parsed[0]);
            modes.add(branch.mode());
            switch (branch.mode()) {
                case EVENT -> {
                    assertFalse(eql, eql.startsWith("sequence ") || eql.startsWith("sample "));
                    assertEquals(eql, 1, branch.stages());
                }
                case SEQUENCE -> {
                    assertThat(eql, startsWith("sequence "));
                    assertEquals(eql, countStages(eql), branch.stages());
                }
                case SAMPLE -> {
                    assertThat(eql, startsWith("sample "));
                    assertThat(eql, containsString(" by "));
                    assertEquals(eql, countStages(eql), branch.stages());
                }
            }
            assertEquals(description.commandString(), expectedMaxMatches(eql, parsed[2]), branch.maxMatches());
        }
        assertThat(modes, equalTo(EnumSet.allOf(Mode.class)));
    }

    public void testMetadataClauseMatchesTheRecordedBranch() {
        assumeEqlCommand();
        int withMetadata = 0;
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2)));
            Branch branch = branch(description);
            String[] parsed = parseBranches(description.commandString()).getFirst();
            assertEquals(description.commandString(), String.join(", ", branch.metadata()), parsed[1] == null ? "" : parsed[1]);
            withMetadata += branch.metadata().isEmpty() ? 0 : 1;
        }
        if (EsqlCapabilities.Cap.EQL_COMMAND_METADATA.isEnabled()) {
            assertThat(withMetadata, greaterThan(0));
        }
    }

    public void testTiebreakerIsASingleValuedNonNullLongInSource() {
        assumeEqlCommand();
        int withTiebreaker = 0;
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2)));
            String options = options(description);
            if (options.contains("tiebreaker_field")) {
                assertThat(options, containsString("\"tiebreaker_field\": \"pid\""));
                withTiebreaker++;
            }
        }
        assertThat(withTiebreaker, greaterThan(0));
    }

    public void testSampleAndTimestampOptionsFollowTheMode() {
        assumeEqlCommand();
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2)));
            String options = options(description);
            if (branch(description).mode() == Mode.SAMPLE) {
                assertThat(options, not(containsString("timestamp_field")));
                assertThat(options, not(containsString("tiebreaker_field")));
            } else {
                assertThat(options, not(containsString("max_samples_per_key")));
            }
        }
    }

    public void testOnlyUsesEligibleFieldsAndEscapesSampledValues() {
        assumeEqlCommand();
        boolean escapedValue = false;
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2)));
            for (String[] parsed : parseBranches(description.commandString())) {
                String eql = unescapeStringLiteral(parsed[0]);
                assertThat(eql, not(containsString("first-name")));
                assertThat(eql, not(containsString("message")));
                assertThat(eql, not(containsString("by tags")));
                escapedValue |= eql.contains("\"web \\\"1\\\"\"") || eql.contains("\"db\\\\2\"");
            }
        }
        assertTrue(escapedValue);
    }

    public void testSubqueryWrapsEqlAloneOrInAUnion() {
        assumeEqlCommand();
        assumeTrue("EQL in subqueries is snapshot-only", EsqlCapabilities.Cap.EQL_IN_SUBQUERY.isEnabled());
        int lone = 0;
        int unions = 0;
        for (int i = 0; i < 2000; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2)));
            String query = stripSetPrefix(description);
            boolean union = Boolean.TRUE.equals(description.context().get(FromGenerator.HAS_UNION_ALL));
            int eqlBranches = parseBranches(query).size();
            if (query.startsWith("FROM ")) {
                assertThat(query, eqlBranches, equalTo(query.startsWith("FROM " + INDEX + ", (") ? 1 : union ? 2 : 1));
                assertThat(query, union, equalTo(query.startsWith("FROM " + INDEX + ", (") || eqlBranches == 2));
                lone += union ? 0 : 1;
                unions += union ? 1 : 0;
            } else {
                assertThat(query, startsWith("EQL "));
                assertFalse(query, union);
            }
        }
        assertThat(lone, greaterThan(0));
        assertThat(unions, greaterThan(0));
    }

    public void testSequencesNeedATimestampPresentInSource() {
        assumeEqlCommand();
        List<List<Object>> rowsWithoutSource = new ArrayList<>();
        for (List<Object> row : ROWS) {
            List<Object> copy = new ArrayList<>(row);
            copy.add(null);
            rowsWithoutSource.add(copy);
        }
        for (int i = 0; i < 200; i++) {
            CommandDescription description = generate(executor(COLUMNS, rowsWithoutSource, Map.of()));
            assertThat(description.commandString(), branch(description).mode(), equalTo(Mode.EVENT));
        }
    }

    public void testNeverUsesANullableTiebreaker() {
        assumeEqlCommand();
        for (int i = 0; i < 500; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("tags", 2), Map.of("pid", 1)));
            assertThat(description.commandString(), not(containsString("tiebreaker_field")));
        }
    }

    public void testOnlyGeneratesEventsWhenAColumnIsNamedLikeASyntheticColumn() {
        assumeEqlCommand();
        List<Column> columns = new ArrayList<>(COLUMNS);
        columns.add(columns.size() - 1, new Column("_sequence", "long", List.of()));
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : ROWS) {
            List<Object> copy = new ArrayList<>(row);
            copy.add(1);
            rows.add(copy);
        }
        for (int i = 0; i < 200; i++) {
            CommandDescription description = generate(executor(columns, rows, Map.of("tags", 2)));
            assertThat(description.commandString(), branch(description).mode(), equalTo(Mode.EVENT));
        }
    }

    public void testNoFromBranchNextToEqlWhenATypeIsNotConvertible() {
        assumeEqlCommand();
        List<Column> columns = new ArrayList<>(COLUMNS);
        columns.add(columns.size() - 1, new Column("location", "geo_point", List.of()));
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : ROWS) {
            List<Object> copy = new ArrayList<>(row);
            copy.add("POINT (1 2)");
            rows.add(copy);
        }
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(executor(columns, rows, Map.of("tags", 2)));
            assertThat(stripSetPrefix(description), not(startsWith("FROM " + INDEX + ", ")));
        }
    }

    public void testFromBranchNextToEqlWhenEveryTypeIsConvertible() {
        assumeEqlCommand();
        assumeTrue("EQL in subqueries is snapshot-only", EsqlCapabilities.Cap.EQL_IN_SUBQUERY.isEnabled());
        boolean fromBranch = false;
        for (int i = 0; i < 2000 && fromBranch == false; i++) {
            fromBranch = stripSetPrefix(generate(executor(COLUMNS, ROWS, Map.of("tags", 2)))).startsWith("FROM " + INDEX + ", (");
        }
        assertTrue(fromBranch);
    }

    public void testNestedSourceObjectsCountAsInSource() {
        assumeEqlCommand();
        List<Column> columns = List.of(new Column("source.ip", "ip", List.of()), new Column("_source", "_source", List.of()));
        List<List<Object>> rows = List.of(
            row("10.0.0.1", Map.of("source", Map.of("ip", "10.0.0.1"))),
            row("10.0.0.2", Map.of("source", List.of(Map.of("ip", "10.0.0.2"))))
        );
        for (int i = 0; i < 100; i++) {
            CommandDescription description = generate(executor(columns, rows, Map.of()));
            assertThat(description.commandString(), description.commandName(), equalTo(EqlGenerator.EQL));
            assertThat(description.commandString(), branch(description).mode(), equalTo(Mode.SAMPLE));
        }
    }

    public void testFallsBackToFromWhenNoFieldHasSampledValues() {
        assumeEqlCommand();
        List<Column> columns = List.of(new Column("missing", "keyword", List.of()), new Column("_source", "_source", List.of()));
        List<List<Object>> rows = List.of(row(null, Map.of()), row(null, Map.of()));
        CommandDescription description = generate(executor(columns, rows, Map.of()));
        assertThat(description.commandString(), description.commandName(), equalTo("from"));
    }

    public void testOnlyGeneratesSamplesWithoutASingleValuedDateField() {
        assumeEqlCommand();
        for (int i = 0; i < 200; i++) {
            CommandDescription description = generate(executor(COLUMNS, ROWS, Map.of("@timestamp", 2)));
            assertThat(description.commandString(), branch(description).mode(), equalTo(Mode.SAMPLE));
        }
    }

    public void testFallsBackToFromWithoutUsableFields() {
        List<Column> columns = List.of(new Column("message", "text", List.of()), new Column("first-name", "keyword", List.of()));
        List<List<Object>> rows = List.of(List.of("hello", "x"));
        CommandDescription description = generate(executor(columns, rows, Map.of()));
        assertThat(description.commandString(), description.commandName(), equalTo("from"));
    }

    public void testFallsBackToFromWhenSamplingFails() {
        CommandDescription description = generate(
            (query, depth) -> new QueryExecuted(query, depth, null, null, new RuntimeException("boom"))
        );
        assertThat(description.commandString(), description.commandName(), equalTo("from"));
    }

    public void testValidateOutputAcceptsMatchingSequenceOutput() {
        Branch branch = new Branch(Mode.SEQUENCE, 2, 1, List.of("_id"));
        List<Column> columns = columns("_sequence", "_sequence_stage", "join_keys", "host", "_id");
        List<List<Object>> output = List.of(List.of(0, 0, "a", "h", "1"), List.of(0, 1, "a", "h", "2"));
        assertTrue(validate(branch, false, columns, output).success());
    }

    public void testValidateOutputAcceptsEventOutputWithoutSyntheticColumns() {
        Branch branch = new Branch(Mode.EVENT, 1, 2, List.of("_index"));
        assertTrue(validate(branch, false, columns("host", "_index"), List.of(List.of("a", "i"), List.of("b", "i"))).success());
    }

    public void testValidateOutputRejectsANegativeSequence() {
        Branch branch = new Branch(Mode.SAMPLE, 2, null, List.of());
        List<List<Object>> output = List.of(row(-1, 0, "a"));
        ValidationResult result = validate(branch, false, columns("_sequence", "_sequence_stage", "join_keys"), output);
        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("_sequence must be a non-negative number"));
    }

    public void testValidateOutputRejectsMissingSyntheticColumns() {
        Branch branch = new Branch(Mode.SAMPLE, 2, null, List.of());
        ValidationResult result = validate(branch, false, columns("_sequence", "host"), List.of());
        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("missing synthetic columns"));
    }

    public void testValidateOutputRejectsANonNumericSequence() {
        Branch branch = new Branch(Mode.SEQUENCE, 2, null, List.of());
        List<List<Object>> output = List.of(row("x", 0, "a"));
        ValidationResult result = validate(branch, false, columns("_sequence", "_sequence_stage", "join_keys"), output);
        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("_sequence must be a non-negative number"));
    }

    public void testValidateOutputRejectsStageOutsideTheQueryStages() {
        Branch branch = new Branch(Mode.SEQUENCE, 2, null, List.of());
        List<List<Object>> output = List.of(row(0, 2, "a"));
        ValidationResult result = validate(branch, false, columns("_sequence", "_sequence_stage", "join_keys"), output);
        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("_sequence_stage must be in [0, 2)"));
    }

    public void testValidateOutputRejectsMoreRowsThanTheRequestedMatches() {
        Branch branch = new Branch(Mode.EVENT, 1, 1, List.of());
        ValidationResult result = validate(branch, false, columns("host"), List.of(List.of("a"), List.of("b")));
        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("at most [1] matches"));
    }

    public void testValidateOutputRejectsMissingMetadata() {
        Branch branch = new Branch(Mode.EVENT, 1, null, List.of("_index"));
        ValidationResult result = validate(branch, false, columns("host"), List.of());
        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("[_index] is missing"));
    }

    public void testValidateOutputSkipsUnions() {
        Branch branch = new Branch(Mode.EVENT, 1, 1, List.of("_index"));
        assertTrue(validate(branch, true, columns("host"), List.of(List.of("a"), List.of("b"))).success());
    }

    public void testValidateOutputDelegatesTheFromFallback() {
        CommandDescription from = new CommandDescription("from", FromGenerator.INSTANCE, "from " + INDEX, Map.of());
        assertTrue(EqlGenerator.INSTANCE.validateOutput(List.of(), from, null, null, columns("host"), List.of()).success());
    }

    private static void assumeEqlCommand() {
        assumeTrue("the EQL command is snapshot-only", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private static CommandDescription generate(QueryExecutor executor) {
        QuerySchema schema = new QuerySchema(List.of(INDEX), List.of(), List.of(), Set.of());
        return EqlGenerator.INSTANCE.generate(List.of(), List.of(), schema, executor, GenerationContext.root(Set.of()));
    }

    private static QueryExecutor executor(List<Column> columns, List<List<Object>> rows, Map<String, Integer> mvCounts) {
        return executor(columns, rows, mvCounts, Map.of());
    }

    private static QueryExecutor executor(
        List<Column> columns,
        List<List<Object>> rows,
        Map<String, Integer> mvCounts,
        Map<String, Integer> nullCounts
    ) {
        List<List<Object>> rowsWithSource = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            if (row.size() == columns.size()) {
                rowsWithSource.add(row);
                continue;
            }
            Map<String, Object> source = new HashMap<>();
            for (int i = 0; i < row.size(); i++) {
                source.put(columns.get(i).name(), row.get(i));
            }
            List<Object> withSource = new ArrayList<>(row);
            withSource.add(source);
            rowsWithSource.add(withSource);
        }
        return (query, depth) -> {
            if (query.startsWith("FROM " + INDEX + " METADATA _source | LIMIT ")) {
                return new QueryExecuted(query, depth, columns, rowsWithSource, null);
            }
            if (query.startsWith("FROM " + INDEX + " | STATS total = COUNT(*)")) {
                int total = 100;
                List<Column> countColumns = new ArrayList<>();
                List<Object> counts = new ArrayList<>();
                countColumns.add(new Column("total", "long", List.of()));
                counts.add(total);
                Matcher matcher = MV_COUNT.matcher(query);
                for (int i = 0; matcher.find(); i++) {
                    countColumns.add(new Column("m" + i, "integer", List.of()));
                    counts.add(mvCounts.getOrDefault(matcher.group(1), 1));
                    countColumns.add(new Column("c" + i, "long", List.of()));
                    counts.add(total - nullCounts.getOrDefault(matcher.group(1), 0));
                }
                return new QueryExecuted(query, depth, countColumns, List.of(counts), null);
            }
            throw new AssertionError("unexpected query [" + query + "]");
        };
    }

    private static ValidationResult validate(Branch branch, boolean union, List<Column> columns, List<List<Object>> output) {
        Map<String, Object> context = new HashMap<>();
        context.put(EqlGenerator.BRANCH, branch);
        context.put(FromGenerator.HAS_UNION_ALL, union);
        CommandDescription description = new CommandDescription(EqlGenerator.EQL, EqlGenerator.INSTANCE, "", context);
        return EqlGenerator.INSTANCE.validateOutput(List.of(), description, null, null, columns, output);
    }

    private static List<Column> columns(String... names) {
        List<Column> columns = new ArrayList<>(names.length);
        for (String name : names) {
            columns.add(new Column(name, "keyword", List.of()));
        }
        return columns;
    }

    private static Branch branch(CommandDescription description) {
        return (Branch) description.context().get(EqlGenerator.BRANCH);
    }

    private static String options(CommandDescription description) {
        String options = parseBranches(description.commandString()).getFirst()[2];
        return options == null ? "" : options;
    }

    private static String stripSetPrefix(CommandDescription description) {
        String query = description.commandString();
        return query.startsWith(FromGenerator.SET_UNMAPPED_FIELDS_PREFIX)
            ? query.substring(FromGenerator.SET_UNMAPPED_FIELDS_PREFIX.length())
            : query;
    }

    private static List<String[]> parseBranches(String command) {
        List<String[]> branches = new ArrayList<>();
        Matcher matcher = EQL_BRANCH.matcher(command);
        while (matcher.find()) {
            branches.add(new String[] { matcher.group(1), matcher.group(2), matcher.group(3) });
        }
        return branches;
    }

    private static int countStages(String eql) {
        int until = eql.indexOf(" until [");
        String stages = until < 0 ? eql : eql.substring(0, until);
        return stages.split(" \\[", -1).length - 1;
    }

    private static Integer expectedMaxMatches(String eql, String options) {
        Integer maxMatches = null;
        Matcher size = SIZE.matcher(options == null ? "" : options);
        if (size.find()) {
            maxMatches = Integer.parseInt(size.group(1));
        }
        Matcher limit = HEAD_OR_TAIL.matcher(eql);
        if (limit.find()) {
            int pipeLimit = Integer.parseInt(limit.group(1));
            maxMatches = maxMatches == null ? pipeLimit : Math.min(maxMatches, pipeLimit);
        }
        return maxMatches;
    }

    private static String unescapeStringLiteral(String quoted) {
        StringBuilder value = new StringBuilder();
        for (int i = 1; i < quoted.length() - 1; i++) {
            char c = quoted.charAt(i);
            if (c == '\\') {
                c = switch (quoted.charAt(++i)) {
                    case 'n' -> '\n';
                    case 'r' -> '\r';
                    case 't' -> '\t';
                    default -> quoted.charAt(i);
                };
            }
            value.append(c);
        }
        return value.toString();
    }
}
