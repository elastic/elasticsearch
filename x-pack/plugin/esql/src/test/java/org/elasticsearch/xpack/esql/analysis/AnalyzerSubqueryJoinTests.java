/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryJoin;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Analysis and verification of {@code LEFT OUTER JOIN} on a subquery. The join cannot run yet, so these tests stop at the analyzed
 * plan; {@link SubqueryJoin#rejectUntilSupported} rejects it before optimization in the session.
 */
public class AnalyzerSubqueryJoinTests extends AnalyzerTestCase {

    private static final String JOIN_LANGUAGES = """
        FROM test
        | EVAL language_code = languages
        | LEFT OUTER JOIN (FROM languages) ON language_code
        """;

    public AnalyzerSubqueryJoinTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void checkGrammar() {
        assumeTrue("LEFT OUTER JOIN requires snapshot builds", new EsqlConfig(TEST_FUNCTION_REGISTRY).isDevVersion());
    }

    @Override
    protected TestAnalyzer analyzer() {
        return super.analyzer().addEmployees("test").addLanguages();
    }

    public void testResolvesKeysOnEachSide() {
        SubqueryJoin join = findJoin(analyzer().query(JOIN_LANGUAGES));

        Attribute leftKey = join.config().leftFields().getFirst();
        Eval eval = as(join.left(), Eval.class);
        assertThat(leftKey.id(), equalTo(eval.fields().getFirst().id()));
        // ES|QL reads a byte field as an integer
        assertThat(leftKey.dataType(), equalTo(DataType.INTEGER));

        Attribute rightKey = join.config().rightFields().getFirst();
        assertThat(rightKey.name(), equalTo("language_code"));
        assertTrue(join.right().outputSet().contains(rightKey));
        assertThat(rightKey.dataType(), equalTo(DataType.INTEGER));
    }

    public void testResolvesMultipleKeysPairwise() {
        SubqueryJoin join = findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages, language_name = first_name
            | LEFT OUTER JOIN (FROM languages) ON language_code, language_name
            """));

        assertThat(join.config().leftFields().stream().map(Attribute::name).toList(), contains("language_code", "language_name"));
        assertThat(join.config().leftFields().stream().map(Attribute::dataType).toList(), contains(DataType.INTEGER, DataType.KEYWORD));
        assertThat(join.config().rightFields().stream().map(Attribute::name).toList(), contains("language_code", "language_name"));
        assertThat(join.config().rightFields().stream().map(Attribute::dataType).toList(), contains(DataType.INTEGER, DataType.KEYWORD));
        assertTrue(join.right().outputSet().containsAll(join.config().rightFields()));
    }

    /**
     * Without a {@code Project} or an {@code Aggregate} of its own, a data node running the subquery could reduce its fields to
     * {@code _doc}, so the analyzer pins its output.
     */
    public void testPinsSubqueryOutput() {
        SubqueryJoin join = findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | WHERE language_code > 1) ON language_code
            """));
        Project pin = as(join.right(), Project.class);
        assertThat(pin.output().stream().map(Attribute::name).toList(), contains("language_code", "language_name"));
        assertThat(pin.child().anyMatch(Project.class::isInstance), equalTo(false));
    }

    /**
     * {@code INLINE STATS} holds an {@code Aggregate}, but the subquery's columns pass through it, so its output is pinned as well.
     */
    public void testPinsInlineStatsSubqueryOutput() {
        SubqueryJoin join = findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | INLINE STATS c = COUNT(*) BY language_code) ON language_code
            """));
        Project pin = as(join.right(), Project.class);
        as(pin.child(), InlineStats.class);
        assertThat(pin.output().stream().map(Attribute::name).toList(), hasItems("language_name", "c"));
    }

    public void testDoesNotPinSubqueryWithItsOwnProjectOrAggregate() {
        SubqueryJoin keep = findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | KEEP language_code, language_name) ON language_code
            """));
        assertThat(keep.right().collect(Project.class::isInstance), hasSize(1));

        SubqueryJoin stats = findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | STATS c = COUNT(*) BY language_code) ON language_code
            """));
        assertThat(stats.right(), instanceOf(Aggregate.class));
    }

    /**
     * The subquery's columns come from its result, not from an index the main query reads, so they are references. The join key
     * keeps the left side's attribute, and a subquery column shadows a left column of the same name.
     */
    public void testSubqueryColumnsAreReferences() {
        SubqueryJoin join = findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages, language_name = first_name
            | KEEP emp_no, language_code, language_name
            | LEFT OUTER JOIN (FROM languages) ON language_code
            """));

        List<Attribute> output = join.output();
        assertThat(output.stream().map(Attribute::name).toList(), contains("emp_no", "language_code", "language_name"));
        assertThat(output.get(1).id(), equalTo(join.config().leftFields().getFirst().id()));

        Attribute languageName = output.get(2);
        assertThat(languageName, instanceOf(ReferenceAttribute.class));
        Attribute subqueryLanguageName = join.right().output().get(1);
        assertThat(subqueryLanguageName.name(), equalTo("language_name"));
        assertThat(languageName.id(), equalTo(subqueryLanguageName.id()));
        assertThat(join.rightOutputFields(), contains(languageName));
    }

    public void testUnknownKeys() {
        analyzer().error("""
            FROM test
            | LEFT OUTER JOIN (FROM languages) ON language_code
            """, containsString("Unknown column [language_code] in left side of join"));
        analyzer().error("""
            FROM test
            | LEFT OUTER JOIN (FROM languages) ON emp_no
            """, containsString("Unknown column [emp_no] in right side of join"));
    }

    public void testNumericKeysOfDifferentTypes() {
        for (String types : List.of("1, 1.0", "1, 1::long", "1::long, 1.5", "1.5, 1", "1::long, 1::long")) {
            String[] values = types.split(", ");
            SubqueryJoin join = findJoin(
                analyzer().query("ROW k = " + values[0] + " | LEFT OUTER JOIN (ROW k = " + values[1] + ", v = 2) ON k")
            );
            assertTrue(types, join.config().leftFields().getFirst().resolved());
        }
    }

    public void testKeywordAndTextKeys() {
        SubqueryJoin join = findJoin(analyzer().query("""
            FROM test
            | KEEP emp_no, gender
            | LEFT OUTER JOIN (ROW gender = "F", g = 1) ON gender
            """));
        assertThat(join.config().leftFields().getFirst().dataType(), equalTo(DataType.TEXT));
        assertThat(join.config().rightFields().getFirst().dataType(), equalTo(DataType.KEYWORD));
    }

    public void testIncompatibleKeyTypes() {
        for (String types : List.of(
            "\"a\", 1, KEYWORD, INTEGER",
            "true, 1, BOOLEAN, INTEGER",
            "1::unsigned_long, 1.0, UNSIGNED_LONG, DOUBLE",
            "1, 1::unsigned_long, INTEGER, UNSIGNED_LONG",
            "TO_DATETIME(\"2023-01-01T00:00:00Z\"), TO_DATE_NANOS(\"2023-01-01T00:00:00Z\"), DATETIME, DATE_NANOS"
        )) {
            String[] parts = types.split(", ");
            analyzer().error(
                "ROW k = " + parts[0] + " | LEFT OUTER JOIN (ROW k = " + parts[1] + ", v = 2) ON k",
                containsString(
                    "JOIN subquery left field [k] of type ["
                        + parts[2]
                        + "] is incompatible with right field [k] of type ["
                        + parts[3]
                        + "]"
                )
            );
        }
    }

    /**
     * The second key is checked as well as the first.
     */
    public void testIncompatibleSecondKey() {
        analyzer().error(
            "ROW a = 1, b = \"x\" | LEFT OUTER JOIN (ROW a = 1, b = 2) ON a, b",
            allOf(
                containsString("Found 1 problem"),
                containsString("JOIN subquery left field [b] of type [KEYWORD] is incompatible with right field [b] of type [INTEGER]")
            )
        );
    }

    public void testUnsupportedKeyTypes() {
        for (String value : List.of("1::unsigned_long", "TO_GEOPOINT(\"POINT(1 2)\")")) {
            analyzer().error(
                "ROW k = " + value + " | LEFT OUTER JOIN (ROW k = " + value + ", v = 2) ON k",
                containsString("JOIN subquery with right field [k] of type [")
            );
        }
    }

    /**
     * An unknown key is rejected the same way: {@code ResolveUnmapped} neither rewrites nor rejects it first.
     */
    public void testUnmappedFieldsMustBeDefault() {
        for (UnmappedResolution resolution : List.of(UnmappedResolution.NULLIFY, UnmappedResolution.LOAD)) {
            for (String key : List.of("language_code", "does_not_exist")) {
                analyzer().unmappedResolution(resolution)
                    .error(
                        JOIN_LANGUAGES.replace("ON language_code", "ON " + key),
                        allOf(
                            containsString("Found 1 problem"),
                            containsString(
                                "line 3:3: JOIN on a subquery is not supported with unmapped_fields=\"" + resolution.settingValue() + "\""
                            )
                        )
                    );
            }
        }
    }

    /**
     * A view decides the {@code unmapped_fields} mode of its own body: the join is rejected in a view that sets a mode, and allowed in
     * a view without one, whatever mode the query reading the view sets.
     */
    public void testUnmappedFieldsModeOfView() {
        assumeTrue("Requires branching views", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        String body = JOIN_LANGUAGES.strip();
        analyzer().addView("v", "SET unmapped_fields=\"nullify\";\n" + body)
            .statementError("FROM v", containsString("JOIN on a subquery is not supported with unmapped_fields=\"nullify\""));
        LogicalPlan plan = analyzer().addView("v", body).statement("SET unmapped_fields=\"nullify\"; FROM v");
        assertTrue(findJoin(plan).resolved());
    }

    /**
     * A {@code LIMIT} in the subquery limits only the subquery, so a full-text function may follow the join. A {@code LIMIT} on the
     * left side still counts.
     */
    public void testFullTextFunctionAfterLimitInSubquery() {
        findJoin(analyzer().query("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages | LIMIT 5) ON language_code
            | WHERE MATCH(first_name, "x")
            """));
        analyzer().error("""
            FROM test
            | LIMIT 5
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM languages) ON language_code
            | WHERE MATCH(first_name, "x")
            """, containsString("[MATCH] function cannot be used after LIMIT"));
    }

    public void testRemoteIndicesAreRejected() {
        TestAnalyzer analyzer = analyzer().addIndex("remote:languages", remoteLanguages());
        analyzer.error("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (FROM remote:languages) ON language_code
            """, containsString("line 3:3: JOIN subquery does not support remote indices yet, found [remote:languages]"));
        analyzer.error("""
            FROM remote:languages
            | LEFT OUTER JOIN (FROM languages) ON language_code
            """, containsString("line 2:3: JOIN subquery does not support remote indices yet, found [remote:languages]"));
    }

    /**
     * Each join reports the remote indices of its own sides, so a remote index read before two chained joins is reported once.
     */
    public void testRemoteIndexIsReportedOnceForChainedJoins() {
        analyzer().addIndex("remote:languages", remoteLanguages())
            .error(
                """
                    FROM remote:languages
                    | LEFT OUTER JOIN (FROM languages) ON language_code
                    | LEFT OUTER JOIN (FROM languages) ON language_code
                    """,
                allOf(
                    containsString("Found 1 problem"),
                    containsString("line 2:3: JOIN subquery does not support remote indices yet, found [remote:languages]")
                )
            );
    }

    /**
     * An {@code IN} subquery runs on its own, so a remote index it reads says nothing about the {@code JOIN}.
     */
    public void testRemoteIndexInInSubqueryIsNotTheJoins() {
        SubqueryJoin join = findJoin(analyzer().addIndex("remote:languages", remoteLanguages()).query("""
            FROM test
            | EVAL language_code = languages
            | WHERE language_code IN (FROM remote:languages | KEEP language_code)
            | LEFT OUTER JOIN (FROM languages) ON language_code
            """));
        assertTrue(join.resolved());
    }

    public void testNestedJoinResolves() {
        LogicalPlan plan = analyzer().query("""
            FROM test
            | EVAL language_code = languages
            | LEFT OUTER JOIN (
                FROM languages
                | LEFT OUTER JOIN (FROM languages | EVAL x = 1 | KEEP language_code, x) ON language_code
              ) ON language_code
            """);
        List<LogicalPlan> joins = plan.collect(SubqueryJoin.class::isInstance);
        assertThat(joins, hasSize(2));
        for (LogicalPlan join : joins) {
            assertTrue(join.resolved());
        }
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItems("language_name", "x"));
    }

    public void testRejectUntilSupported() {
        LogicalPlan plan = analyzer().query(JOIN_LANGUAGES);
        VerificationException e = expectThrows(VerificationException.class, () -> SubqueryJoin.rejectUntilSupported(plan));
        assertThat(e.getMessage(), containsString("line 3:3: JOIN on a subquery is not supported yet"));

        LogicalPlan withoutJoin = analyzer().query("FROM test | EVAL language_code = languages");
        SubqueryJoin.rejectUntilSupported(withoutJoin);
    }

    /**
     * A field with conflicting types can only be converted where it is read, inside the subquery. After the join it is a column of
     * the subquery's result with type {@code unsupported}, so converting it there fails like converting any unsupported column.
     */
    public void testUnionTypedSubqueryColumn() {
        TestAnalyzer analyzer = super.analyzer().addIndex(AnalyzerTestUtils.indexWithDateDateNanosUnionType());
        analyzer.error("""
            FROM index*
            | EVAL k = 1
            | LEFT OUTER JOIN (FROM index* | EVAL k = 1 | KEEP k, date_and_date_nanos_and_long) ON k
            | EVAL c = date_and_date_nanos_and_long::date_nanos
            """, containsString("found value [date_and_date_nanos_and_long] type [unsupported]"));

        LogicalPlan plan = analyzer.query("""
            FROM index*
            | EVAL k = 1
            | LEFT OUTER JOIN (FROM index* | EVAL k = 1, c = date_and_date_nanos_and_long::date_nanos | KEEP k, c) ON k
            | KEEP c
            """);
        assertThat(plan.output().getFirst().dataType(), equalTo(DataType.DATE_NANOS));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private static SubqueryJoin findJoin(LogicalPlan plan) {
        List<LogicalPlan> joins = plan.collect(SubqueryJoin.class::isInstance);
        assertThat(joins, hasSize(1));
        return (SubqueryJoin) joins.getFirst();
    }

    /**
     * The {@code languages} mapping, with its only concrete index on the {@code remote} cluster.
     */
    private static IndexResolution remoteLanguages() {
        EsIndex languages = TestAnalyzer.loadMapping("mapping-languages.json", "languages").get();
        return IndexResolution.valid(
            new EsIndex(
                "remote:languages",
                languages.mapping(),
                Map.of("languages", new IndexProperties(IndexMode.STANDARD, 0)),
                Map.of("remote", List.of("languages")),
                Map.of("remote", List.of("languages"))
            )
        );
    }
}
