/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.hamcrest.Matchers.containsString;

/**
 * Negative tests for subquery analysis in {@code FROM} (and the related {@code ViewUnionAll}/{@code UnionAll} planning), or those don't
 * fit the golden tests. The successful plan-shape (positive) tests over real CSV datasets now live in {@code AnalyzerSubqueryGoldenTests}.
 */
public class AnalyzerSubqueryTests extends ESTestCase {

    /*
     * Casts the same field ({@code emp_no}) to multiple types in one query, producing several synthetic
     * {@code $$emp_no$converted_to$<type>} attributes from a single source. The relative order of those synthetics in the
     * {@code UnionAll} output (and in the {@code Eval} that defines them) is not deterministic across runs, so this test stays here, it is
     * not a good candidate for golden tests.
     *
     * Project[[!avg_worked_seconds, birth_date{r}#961, !emp_no, !gender, height{r}#965, height.float{r}#966, height.half_float{r}#967,
     *          height.scaled_float{r}#968, hire_date{r}#969, !job_positions, languages{r}#972, languages.byte{r}#973,
     *          languages.long{r}#974, languages.short{r}#975, !last_name, !salary, salary_change{r}#978, salary_change.int{r}#979,
     *          salary_change.keyword{r}#980, salary_change.long{r}#981, height.double{r}#983, languages.int{r}#984, x{r}#891, y{r}#894,
     *          z{r}#897, first_name{r}#900, still_hired{r}#904, is_rehired{r}#907]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[still_hired{r}#904,ASC,LAST], Order[is_rehired{r}#907,ASC,LAST]]]
     *     \_Eval[[$$still_hired$converted_to$keyword{r$}#1002 AS still_hired#904,
     *             $$is_rehired$converted_to$keyword{r$}#1001 AS is_rehired#907]]
     *       \_Filter[z{r}#897 > 10000[INTEGER]]
     *         \_Eval[[$$emp_no$converted_to$long{r$}#998 AS x#891, $$emp_no$converted_to$keyword{r$}#997 AS y#894,
     *                 $$emp_no$converted_to$double{r$}#999 AS z#897, $$first_name$converted_to$keyword{r$}#1000 AS first_name#900]]
     *           \_UnionAll[[!avg_worked_seconds, birth_date{r}#961, !emp_no, $$emp_no$converted_to$keyword{r$}#997,
     *                       $$emp_no$converted_to$long{r$}#998, $$emp_no$converted_to$double{r$}#999, !first_name,
     *                       $$first_name$converted_to$keyword{r$}#1000, !gender, height{r}#965, height.float{r}#966,
     *                       height.half_float{r}#967, height.scaled_float{r}#968, hire_date{r}#969, !is_rehired,
     *                       $$is_rehired$converted_to$keyword{r$}#1001, !job_positions, languages{r}#972, languages.byte{r}#973,
     *                       languages.long{r}#974, languages.short{r}#975, !last_name, !salary, salary_change{r}#978,
     *                       salary_change.int{r}#979, salary_change.keyword{r}#980, salary_change.long{r}#981, !still_hired,
     *                       $$still_hired$converted_to$keyword{r$}#1002, height.double{r}#983, languages.int{r}#984]]
     *             |_Project[[avg_worked_seconds{r}#1003, birth_date{f}#914, emp_no{r}#1004, $$emp_no$converted_to$keyword{r$}#985,
     *                        $$emp_no$converted_to$long{r$}#986, $$emp_no$converted_to$double{r$}#987, first_name{r}#1005,
     *                        $$first_name$converted_to$keyword{r$}#988, gender{r}#1006, height{f}#921, height.float{f}#922,
     *                        height.half_float{f}#924, height.scaled_float{f}#923, hire_date{r}#1007, is_rehired{r}#1008,
     *                        $$is_rehired$converted_to$keyword{r$}#989, job_positions{r}#1009, languages{f}#917, languages.byte{f}#920,
     *                        languages.long{f}#918, languages.short{f}#919, last_name{r}#1010, salary{r}#1011, salary_change{f}#929,
     *                        salary_change.int{f}#930, salary_change.keyword{f}#932, salary_change.long{f}#931, still_hired{r}#1012,
     *                        $$still_hired$converted_to$keyword{r$}#990, height.double{r}#953, languages.int{r}#954]]
     *             | \_Eval[[null[KEYWORD] AS avg_worked_seconds#1003, null[KEYWORD] AS emp_no#1004, null[KEYWORD] AS first_name#1005,
     *                       null[KEYWORD] AS gender#1006, TODATENANOS(hire_date{f}#915) AS hire_date#1007,
     *                       null[KEYWORD] AS is_rehired#1008, null[KEYWORD] AS job_positions#1009, null[KEYWORD] AS last_name#1010,
     *                       null[KEYWORD] AS salary#1011, null[KEYWORD] AS still_hired#1012]]
     *             |   \_Eval[[TOSTRING(emp_no{f}#910) AS $$emp_no$converted_to$keyword#985,
     *                         TOLONG(emp_no{f}#910) AS $$emp_no$converted_to$long#986,
     *                         TODOUBLE(emp_no{f}#910) AS $$emp_no$converted_to$double#987,
     *                         TOSTRING(first_name{f}#911) AS $$first_name$converted_to$keyword#988,
     *                         TOSTRING(is_rehired{f}#928) AS $$is_rehired$converted_to$keyword#989,
     *                         TOSTRING(still_hired{f}#925) AS $$still_hired$converted_to$keyword#990]]
     *             |     \_Eval[[null[DOUBLE] AS height.double#953, null[INTEGER] AS languages.int#954]]
     *             |       \_EsRelation[test][avg_worked_seconds{f}#926, birth_date{f}#914, emp_n..]
     *             \_Project[[avg_worked_seconds{r}#1013, birth_date{f}#937, emp_no{r}#1014, $$emp_no$converted_to$keyword{r$}#991,
     *                        $$emp_no$converted_to$long{r$}#992, $$emp_no$converted_to$double{r$}#993, first_name{r}#1015,
     *                        $$first_name$converted_to$keyword{r$}#994, gender{r}#1016, height{f}#944, height.float{r}#955,
     *                        height.half_float{f}#947, height.scaled_float{f}#946, hire_date{f}#938, is_rehired{r}#1017,
     *                        $$is_rehired$converted_to$keyword{r$}#995, job_positions{r}#1018, languages{f}#940, languages.byte{r}#956,
     *                        languages.long{f}#941, languages.short{f}#942, last_name{r}#1019, salary{r}#1020, salary_change{f}#952,
     *                        salary_change.int{r}#957, salary_change.keyword{r}#958, salary_change.long{r}#959, still_hired{r}#1021,
     *                        $$still_hired$converted_to$keyword{r$}#996, height.double{f}#945, languages.int{f}#943]]
     *               \_Eval[[null[KEYWORD] AS avg_worked_seconds#1013, null[KEYWORD] AS emp_no#1014, null[KEYWORD] AS first_name#1015,
     *                       null[KEYWORD] AS gender#1016, null[KEYWORD] AS is_rehired#1017, null[KEYWORD] AS job_positions#1018,
     *                       null[KEYWORD] AS last_name#1019, null[KEYWORD] AS salary#1020, null[KEYWORD] AS still_hired#1021]]
     *                 \_Eval[[TOSTRING(emp_no{f}#933) AS $$emp_no$converted_to$keyword#991,
     *                         TOLONG(emp_no{f}#933) AS $$emp_no$converted_to$long#992,
     *                         TODOUBLE(emp_no{f}#933) AS $$emp_no$converted_to$double#993,
     *                         TOSTRING(first_name{f}#934) AS $$first_name$converted_to$keyword#994,
     *                         TOSTRING(is_rehired{f}#951) AS $$is_rehired$converted_to$keyword#995,
     *                         TOSTRING(still_hired{f}#948) AS $$still_hired$converted_to$keyword#996]]
     *                   \_Eval[[null[DOUBLE] AS height.float#955, null[INTEGER] AS languages.byte#956,
     *                           null[INTEGER] AS salary_change.int#957, null[KEYWORD] AS salary_change.keyword#958,
     *                           null[LONG] AS salary_change.long#959]]
     *                     \_Subquery[]
     *                       \_Filter[languages{f}#940 > 0[INTEGER]]
     *                         \_EsRelation[test_mixed_types][avg_worked_seconds{f}#949, birth_date{f}#937, emp_n..]
     */
    public void testMixedDataTypesWithMultipleExplicitCastingInSubquery() {
        LogicalPlan plan = analyzer().addDefaultIndex().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL x = emp_no::long, y = emp_no::string, z = emp_no::double, first_name = first_name::string
            | WHERE z > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(28, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias a = aliases.get(0);
        assertEquals("still_hired", a.name());
        ReferenceAttribute still_hired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$still_hired$converted_to$keyword", still_hired.name());
        a = aliases.get(1);
        assertEquals("is_rehired", a.name());
        ReferenceAttribute is_rehired = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$is_rehired$converted_to$keyword", is_rehired.name());
        Filter filter = as(eval.child(), Filter.class);
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(4, aliases.size());
        a = aliases.get(0);
        assertEquals("x", a.name());
        ReferenceAttribute emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        a = aliases.get(1);
        assertEquals("y", a.name());
        emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$keyword", emp_no.name());
        a = aliases.get(2);
        assertEquals("z", a.name());
        emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$double", emp_no.name());
        a = aliases.get(3);
        assertEquals("first_name", a.name());
        ReferenceAttribute first_name = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$first_name$converted_to$keyword", first_name.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(31, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(6, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(6, explicitCastingEval.fields().size());
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(5, missingFieldEval.fields().size());
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        assertEquals(INTEGER, fa.dataType());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(0, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[MATCH(client_ip{r}#2140,127.0.0.1[KEYWORD])]
     *   \_UnionAll[[_meta_field{r}#2128, emp_no{r}#2129, first_name{r}#2130, gender{r}#2131, hire_date{r}#2132, job{r}#2133,
     *               job.raw{r}#2134, languages{r}#2135, last_name{r}#2136, long_noidx{r}#2137, salary{r}#2138, @timestamp{r}#2139,
     *               client_ip{r}#2140, event_duration{r}#2141, message{r}#2142]]
     *     |_Project[[_meta_field{f}#2104, emp_no{f}#2098, first_name{f}#2099, gender{f}#2100, hire_date{f}#2105, job{f}#2106,
     *                job.raw{f}#2107, languages{f}#2101, last_name{f}#2102, long_noidx{f}#2108, salary{f}#2103, @timestamp{r}#2113,
     *                client_ip{r}#2114, event_duration{r}#2115, message{r}#2116]]
     *     | \_Eval[[null[DATETIME] AS @timestamp#2113, null[IP] AS client_ip#2114, null[LONG] AS event_duration#2115,
     *               null[KEYWORD] AS message#2116]]
     *     |   \_EsRelation[test][_meta_field{f}#2104, emp_no{f}#2098, first_name{f}#..]
     *     \_Project[[_meta_field{r}#2117, emp_no{r}#2118, first_name{r}#2119, gender{r}#2120, hire_date{r}#2121, job{r}#2122,
     *                job.raw{r}#2123, languages{r}#2124, last_name{r}#2125, long_noidx{r}#2126, salary{r}#2127, @timestamp{f}#2109,
     *                client_ip{f}#2110, event_duration{f}#2111, message{f}#2112]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#2117, null[INTEGER] AS emp_no#2118, null[KEYWORD] AS first_name#2119,
     *               null[TEXT] AS gender#2120, null[DATETIME] AS hire_date#2121, null[TEXT] AS job#2122, null[KEYWORD] AS job.raw#2123,
     *               null[INTEGER] AS languages#2124, null[KEYWORD] AS last_name#2125, null[LONG] AS long_noidx#2126,
     *               null[INTEGER] AS salary#2127]]
     *         \_Subquery[]
     *           \_EsRelation[sample_data][@timestamp{f}#2109, client_ip{f}#2110, event_durati..]
     */
    public void testPruneEmptySubquery() {
        LogicalPlan plan = analyzer().addEmployees("test").addSampleData().addRemoteMissingIndex().query("""
            FROM test, (FROM remote:missingIndex | WHERE message:"error"), (FROM sample_data)
            | WHERE match(client_ip,"127.0.0.1")
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match matchFunction = as(filter.condition(), Match.class);
        ReferenceAttribute clientIP = as(matchFunction.field(), ReferenceAttribute.class);
        assertEquals("client_ip", clientIP.name());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(15, output.size());
        // the subquery with remote:missingIndex is pruned, validate PruneEmptyUnionAllBranch
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    // no_fields_index has empty mapping, however there is entry in indexNameWithModes,originalIndices and concreteIndices
    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[]]
     *   |_Project[[]]
     *   | \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     *   |_Project[[]]
     *   | \_Subquery[]
     *   |   \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     *   \_Project[[]]
     *     \_Subquery[]
     *       \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     */
    public void testSubqueryInFromWithNoFieldsIndices() {
        LogicalPlan plan = analyzer().addNoFieldsIndex().query("""
            FROM
                no_fields_index,
                (FROM no_fields_index),
                (FROM no_fields_index)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(0, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());
    }

    // empty_index has empty mapping,indexNameWithModes,originalIndices and concreteIndices
    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[]]
     *   |_Project[[]]
     *   | \_EsRelation[empty_index][<no-fields>{r$}#2]
     *   |_Project[[]]
     *   | \_Subquery[]
     *   |   \_EsRelation[empty_index][<no-fields>{r$}#2]
     *   \_Project[[]]
     *     \_Subquery[]
     *       \_EsRelation[empty_index][<no-fields>{r$}#2]
     */
    public void testSubqueryInFromWithEmptyIndex() {
        LogicalPlan plan = analyzer().addEmptyIndex().query("""
            FROM
                empty_index,
                (FROM empty_index),
                (FROM empty_index)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(0, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());
    }

    // no_fields_index has empty mapping, however there is entry in indexNameWithModes,originalIndices and concreteIndices
    // empty_index has empty mapping,indexNameWithModes,originalIndices and concreteIndices
    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[]]
     *   |_Project[[]]
     *   | \_Subquery[]
     *   |   \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     *   |_Project[[]]
     *   | \_Subquery[]
     *   |   \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     *   \_Project[[]]
     *     \_Subquery[]
     *       \_EsRelation[empty_index][<no-fields>{r$}#2]
     */
    public void testSubqueryInFromWithNoFieldsAndEmptyIndex() {
        LogicalPlan plan = analyzer().addNoFieldsIndex().addEmptyIndex().query("""
            FROM
                (FROM no_fields_index),
                (FROM no_fields_index),
                (FROM empty_index)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(0, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        EsRelation subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("no_fields_index", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        assertTrue(subqueryProject.projections().isEmpty());
        subquery = as(subqueryProject.child(), Subquery.class);
        subqueryIndex = as(subquery.child(), EsRelation.class);
        assertEquals("empty_index", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count()#2367]]
     *   \_UnionAll[[]]
     *     |_Project[[]]
     *     | \_Subquery[]
     *     |   \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     *     \_Project[[]]
     *       \_Subquery[]
     *         \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     */
    public void testCountWithSubqueryWithNoFields() {
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM no_fields_index), (FROM no_fields_index)
                | STATS {}
                """, count);
            var plan = analyzer().addNoFieldsIndex().query(query);

            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(2, unionAll.children().size());

            for (int i = 0; i < 2; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals("no_fields_index", relation.indexPattern());
            }
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count()#171]]
     *   \_UnionAll[[]]
     *     |_Project[[]]
     *     | \_Subquery[]
     *     |   \_EsRelation[empty_index][<no-fields>{r$}#2]
     *     \_Project[[]]
     *       \_Subquery[]
     *         \_EsRelation[empty_index][<no-fields>{r$}#2]
     */
    public void testCountWithSubqueryWithEmptyIndex() {
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM empty_index), (FROM empty_index)
                | STATS {}
                """, count);
            var plan = analyzer().addEmptyIndex().query(query);

            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(2, unionAll.children().size());

            for (int i = 0; i < 2; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals("empty_index", relation.indexPattern());
            }
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count()#885]]
     *   \_UnionAll[[]]
     *     |_Project[[]]
     *     | \_Subquery[]
     *     |   \_EsRelation[no_fields_index][<no-fields>{r$}#2]
     *     \_Project[[]]
     *       \_Subquery[]
     *         \_EsRelation[empty_index][<no-fields>{r$}#2]
     */
    public void testCountWithSubqueryWithNoFieldsAndEmptyIndex() {
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM no_fields_index), (FROM empty_index)
                | STATS {}
                """, count);
            var plan = analyzer().addEmptyIndex().addNoFieldsIndex().query(query);

            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(2, unionAll.children().size());

            for (int i = 0; i < 2; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals(i == 0 ? "no_fields_index" : "empty_index", relation.indexPattern());
            }
        }
    }

    /**
     * Regression test for the {@code ResolveUnionTypesInUnionAll} rule crashing with
     * {@code UnresolvedException: Invalid call to attribute on an unresolved object} (surfacing as an HTTP 500).
     * <p>
     * When a convert function (here {@code to_string(date_and_date_nanos)}) is pushed down into the {@link UnionAll}
     * branches, {@code carryOverSyntheticAttributesThroughProjects} walks every {@link Project} in the plan. A
     * {@code KEEP *} above a still-unresolved union-typed field reference ({@code date_and_date_nanos_and_long}, which
     * is ambiguous and cannot be auto-resolved) is an unresolved {@link Project}, so computing its output threw. The
     * fix skips such unresolved Projects; the query now fails with the proper ambiguity verification error instead of
     * an internal exception.
     */
    public void testConvertPushDownWithUnresolvedWildcardProjectAboveUnionType() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzeMaybeNullify(analyzer().addIndex(AnalyzerTestUtils.indexWithDateDateNanosUnionType()), """
                FROM index*, (FROM index*)
                | EVAL converted = to_string(date_and_date_nanos), ambiguous = date_and_date_nanos_and_long
                | KEEP *
                """)
        );
        assertThat(e.getMessage(), containsString("Cannot use field [date_and_date_nanos_and_long] due to ambiguities"));
    }

    /**
     * Control / boundary case for {@link #testConvertPushDownWithUnresolvedWildcardProjectAboveUnionType()}: here the
     * ambiguous union-typed field {@code date_and_date_nanos_and_long} is only <em>passed through</em> by
     * {@code KEEP *} and never referenced in an expression, so it stays a tolerated {@link UnsupportedAttribute}
     * rather than an unresolved reference. Because nothing below the wildcard is unresolved, {@code KEEP *} expands
     * normally and the query analyzes cleanly — no ambiguity error and, critically, no internal exception. This pins
     * down that the crash exercised by the sibling tests requires the ambiguous field to actually be <em>used</em>
     * below an unexpanded wildcard, not merely present in the output.
     */
    public void testConvertPushDownWithUnusedAmbiguousFieldPassedThroughWildcard() {
        LogicalPlan plan = analyzeMaybeNullify(analyzer().addIndex(AnalyzerTestUtils.indexWithDateDateNanosUnionType()), """
            FROM index*, (FROM index*)
            | EVAL converted = to_string(date_and_date_nanos)
            | KEEP *
            """);

        Project project = as(plan, Project.class);
        // the ambiguous field is tolerated as an UnsupportedAttribute because it is never used
        Attribute ambiguous = project.output()
            .stream()
            .filter(a -> "date_and_date_nanos_and_long".equals(a.name()))
            .findFirst()
            .orElseThrow();
        as(ambiguous, UnsupportedAttribute.class);
        // the pushed-down conversion is present and resolved alongside it
        assertTrue(project.output().stream().anyMatch(a -> "converted".equals(a.name())));
    }

    /**
     * Variant where two convert functions push synthetic attributes into the {@link UnionAll} branches at once
     * ({@code to_string} and {@code to_long} on the same union-typed field). The carry-over walk must thread both
     * synthetics through the resolved Projects while still skipping the unresolved {@code KEEP *} above the ambiguous
     * reference. Crashed before the fix.
     */
    public void testMultipleConvertPushDownWithUnresolvedWildcardProjectAboveUnionType() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzeMaybeNullify(analyzer().addIndex(AnalyzerTestUtils.indexWithDateDateNanosUnionType()), """
                FROM index*, (FROM index*)
                | EVAL a = to_string(date_and_date_nanos), b = to_long(date_and_date_nanos), ambiguous = date_and_date_nanos_and_long
                | KEEP *
                """)
        );
        assertThat(e.getMessage(), containsString("Cannot use field [date_and_date_nanos_and_long] due to ambiguities"));
    }

    /**
     * Variant with an intermediate {@code RENAME} (another {@link Project}) sitting between the conversion and the
     * unresolved {@code KEEP *}. The carry-over walk must thread the synthetic through the resolved RENAME Project
     * and skip the still-unresolved wildcard Project above it. Crashed before the fix.
     */
    public void testConvertPushDownWithRenameAndUnresolvedWildcardProjectAboveUnionType() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzeMaybeNullify(analyzer().addIndex(AnalyzerTestUtils.indexWithDateDateNanosUnionType()), """
                FROM index*, (FROM index*)
                | EVAL converted = to_string(date_and_date_nanos), ambiguous = date_and_date_nanos_and_long
                | RENAME converted AS c
                | KEEP *
                """)
        );
        assertThat(e.getMessage(), containsString("Cannot use field [date_and_date_nanos_and_long] due to ambiguities"));
    }

    /**
     * Variant where the still-unresolved union-typed reference lives in a {@code WHERE} (not an {@code EVAL}) below
     * the {@code KEEP *}. The unresolved filter keeps {@code KEEP *} from being expanded, so the wildcard Project is
     * still unresolved when the convert-function carry-over walk reaches it. Crashed before the fix.
     */
    public void testConvertPushDownWithUnresolvedFilterBelowWildcardProjectAboveUnionType() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> analyzeMaybeNullify(analyzer().addIndex(AnalyzerTestUtils.indexWithDateDateNanosUnionType()), """
                FROM index*, (FROM index*)
                | EVAL converted = to_string(date_and_date_nanos)
                | WHERE date_and_date_nanos_and_long IS NOT NULL
                | KEEP *
                """)
        );
        assertThat(e.getMessage(), containsString("Cannot use field [date_and_date_nanos_and_long] due to ambiguities"));
    }

    /**
     * Runs {@code query} through {@code analyzer}, randomly prefixing {@code SET unmapped_fields="nullify";} (parsed via
     * {@link TestAnalyzer#statement(String)}) so the union-type carry-over regression tests exercise the fix in both the
     * default and the "nullify" unmapped-field modes. Nullify only rewrites fields that are entirely absent from the
     * mappings; the union-typed fields these tests reference are present (just type-conflicting), so toggling nullify
     * must not change the ambiguity behaviour being asserted — randomizing it here guards against that regressing.
     */
    private static LogicalPlan analyzeMaybeNullify(TestAnalyzer analyzer, String query) {
        return randomBoolean() ? analyzer.statement("SET unmapped_fields=\"nullify\";\n" + query) : analyzer.query(query);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
