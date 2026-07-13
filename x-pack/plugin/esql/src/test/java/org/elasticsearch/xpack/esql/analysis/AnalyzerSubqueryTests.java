/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Negative tests for subquery analysis in {@code FROM} (and the related {@code ViewUnionAll}/{@code UnionAll} planning), or those don't
 * fit the golden tests. The successful plan-shape (positive) tests over real CSV datasets now live in {@code AnalyzerSubqueryGoldenTests}.
 */
public class AnalyzerSubqueryTests extends ESTestCase {

    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    private static void requireExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireNullifySupport() {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
    }

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
        assertNoFieldUnionAll(analyzer().addNoFieldsIndex(), """
            FROM
                no_fields_index,
                (FROM no_fields_index),
                (FROM no_fields_index)
            """, directBranch("no_fields_index"), subqueryBranch("no_fields_index"), subqueryBranch("no_fields_index"));
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
        assertNoFieldUnionAll(analyzer().addEmptyIndex(), """
            FROM
                empty_index,
                (FROM empty_index),
                (FROM empty_index)
            """, directBranch("empty_index"), subqueryBranch("empty_index"), subqueryBranch("empty_index"));
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
        assertNoFieldUnionAll(analyzer().addNoFieldsIndex().addEmptyIndex(), """
            FROM
                (FROM no_fields_index),
                (FROM no_fields_index),
                (FROM empty_index)
            """, subqueryBranch("no_fields_index"), subqueryBranch("no_fields_index"), subqueryBranch("empty_index"));
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
        assertCountOverNoFieldSubqueries(analyzer().addNoFieldsIndex(), "no_fields_index", "no_fields_index");
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
        assertCountOverNoFieldSubqueries(analyzer().addEmptyIndex(), "empty_index", "empty_index");
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
        assertCountOverNoFieldSubqueries(analyzer().addEmptyIndex().addNoFieldsIndex(), "no_fields_index", "empty_index");
    }

    // validate UnsupportedAttribute and UNSUPPORTED types

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!client_ip]]
     *   \_UnionAll[[@timestamp{r}#879, !client_ip, event_duration{r}#881, message{r}#882]]
     *     |_Project[[@timestamp{f}#871, client_ip{r}#883, event_duration{f}#873, message{f}#874]]
     *     | \_Eval[[null[KEYWORD] AS client_ip#883]]
     *     |   \_Subquery[]
     *     |     \_EsRelation[sample_data][@timestamp{f}#871, client_ip{f}#872, event_duration..]
     *     \_Project[[@timestamp{f}#875, client_ip{r}#884, event_duration{f}#877, message{f}#878]]
     *       \_Eval[[null[KEYWORD] AS client_ip#884]]
     *         \_Subquery[]
     *           \_Eval[[1[INTEGER] AS client_ip#869]]
     *             \_EsRelation[sample_data][@timestamp{f}#875, client_ip{f}#876, event_duration..]
     */
    public void testUnionAllWithConflictingTypesFromSubqueries() {
        LogicalPlan plan = analyzer().addSampleData().query("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1) | keep client_ip
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Project[[!client_ip]] — client_ip is UnsupportedAttribute due to type conflict (ip vs integer)
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        assertUnsupportedAttribute(projections.getFirst(), "client_ip", List.of(IP.esType(), INTEGER.esType()));

        // UnionAll[[@timestamp, !client_ip, event_duration, message]]
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Left leg: Project → Eval[null[KEYWORD] AS client_ip] → Subquery → EsRelation[sample_data]
        Project leftProject = as(unionAll.children().get(0), Project.class);
        Eval leftEval = as(leftProject.child(), Eval.class);
        List<Alias> leftAliases = leftEval.fields();
        assertThat(leftAliases, hasSize(1));
        Alias leftAlias = leftAliases.getFirst();
        assertEquals("client_ip", leftAlias.name());
        Literal leftNull = as(leftAlias.child(), Literal.class);
        assertNull(leftNull.value());
        assertEquals(KEYWORD, leftNull.dataType());

        Subquery leftSubquery = as(leftEval.child(), Subquery.class);
        EsRelation leftRelation = as(leftSubquery.child(), EsRelation.class);

        // Right leg: Project → Eval[null[KEYWORD] AS client_ip] → Subquery → Eval[1[INTEGER] AS client_ip] → EsRelation[sample_data]
        Project rightProject = as(unionAll.children().get(1), Project.class);
        Eval rightEval = as(rightProject.child(), Eval.class);
        List<Alias> rightAliases = rightEval.fields();
        assertThat(rightAliases, hasSize(1));
        Alias rightAlias = rightAliases.getFirst();
        assertEquals("client_ip", rightAlias.name());
        Literal rightNull = as(rightAlias.child(), Literal.class);
        assertNull(rightNull.value());
        assertEquals(KEYWORD, rightNull.dataType());

        Subquery rightSubquery = as(rightEval.child(), Subquery.class);
        Eval innerEval = as(rightSubquery.child(), Eval.class);
        List<Alias> innerAliases = innerEval.fields();
        assertThat(innerAliases, hasSize(1));
        Alias innerAlias = innerAliases.getFirst();
        assertEquals("client_ip", innerAlias.name());
        Literal one = as(innerAlias.child(), Literal.class);
        assertEquals(1, one.value());
        assertEquals(INTEGER, one.dataType());
        EsRelation rightRelation = as(innerEval.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#2464, !client_ip, event_duration{r}#2466, message{r}#2467]]
     *   |_Project[[@timestamp{f}#2456, client_ip{r}#2468, event_duration{f}#2458, message{f}#2459]]
     *   | \_Eval[[null[KEYWORD] AS client_ip#2468]]
     *   |   \_Subquery[]
     *   |     \_EsRelation[sample_data][@timestamp{f}#2456, client_ip{f}#2457, event_durati..]
     *   \_Project[[@timestamp{f}#2460, client_ip{r}#2469, event_duration{f}#2462, message{f}#2463]]
     *     \_Eval[[null[KEYWORD] AS client_ip#2469]]
     *       \_Subquery[]
     *         \_Eval[[1[INTEGER] AS client_ip#2455]]
     *           \_EsRelation[sample_data][@timestamp{f}#2460, client_ip{f}#2461, event_durati..]
     */
    public void testUnionAllWithConflictingTypesFromSubqueriesWithoutUsageInMainQuery() {
        LogicalPlan plan = analyzer().addSampleData().query("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1)
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Limit directly over UnionAll since there is no keep/project
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        Attribute clientIpAttr = output.stream().filter(a -> "client_ip".equals(a.name())).findFirst().orElseThrow();
        assertUnsupportedAttribute(clientIpAttr, "client_ip", List.of(IP.esType(), INTEGER.esType()));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!emp_no]]
     *   \_UnionAll[[!avg_worked_seconds, birth_date{r}#739, !emp_no, !first_name, !gender, height{r}#743, height.float{r}#744,
     *               height.half_float{r}#745, height.scaled_float{r}#746, hire_date{r}#747, !is_rehired, !job_positions,
     *               languages{r}#750, languages.byte{r}#751, languages.long{r}#752, languages.short{r}#753, !last_name, !salary,
     *               salary_change{r}#756, salary_change.int{r}#757, salary_change.keyword{r}#758, salary_change.long{r}#759,
     *               !still_hired, height.double{r}#761, languages.int{r}#762]]
     *     |_Project[[avg_worked_seconds{r}#763, birth_date{f}#692, emp_no{r}#764, first_name{r}#765, gender{r}#766, height{f}#699,
     *                height.float{f}#700, height.half_float{f}#702, height.scaled_float{f}#701, hire_date{r}#767, is_rehired{r}#768,
     *                job_positions{r}#769, languages{f}#695, languages.byte{f}#698, languages.long{f}#696, languages.short{f}#697,
     *                last_name{r}#770, salary{r}#771, salary_change{f}#707, salary_change.int{f}#708, salary_change.keyword{f}#710,
     *                salary_change.long{f}#709, still_hired{r}#772, height.double{r}#731, languages.int{r}#732]]
     *     | \_Eval[[null[KEYWORD] AS avg_worked_seconds#763, null[KEYWORD] AS emp_no#764, null[KEYWORD] AS first_name#765,
     *               null[KEYWORD] AS gender#766, TODATENANOS(hire_date{f}#693) AS hire_date#767, null[KEYWORD] AS is_rehired#768,
     *               null[KEYWORD] AS job_positions#769, null[KEYWORD] AS last_name#770, null[KEYWORD] AS salary#771,
     *               null[KEYWORD] AS still_hired#772]]
     *     |   \_Eval[[null[DOUBLE] AS height.double#731, null[INTEGER] AS languages.int#732]]
     *     |     \_EsRelation[test][avg_worked_seconds{f}#704, birth_date{f}#692, emp_n..]
     *     \_Project[[avg_worked_seconds{r}#773, birth_date{f}#715, emp_no{r}#774, first_name{r}#775, gender{r}#776, height{f}#722,
     *                height.float{r}#733, height.half_float{f}#725, height.scaled_float{f}#724, hire_date{f}#716, is_rehired{r}#777,
     *                job_positions{r}#778, languages{f}#718, languages.byte{r}#734, languages.long{f}#719, languages.short{f}#720,
     *                last_name{r}#779, salary{r}#780, salary_change{f}#730, salary_change.int{r}#735, salary_change.keyword{r}#736,
     *                salary_change.long{r}#737, still_hired{r}#781, height.double{f}#723, languages.int{f}#721]]
     *       \_Eval[[null[KEYWORD] AS avg_worked_seconds#773, null[KEYWORD] AS emp_no#774, null[KEYWORD] AS first_name#775,
     *               null[KEYWORD] AS gender#776, null[KEYWORD] AS is_rehired#777, null[KEYWORD] AS job_positions#778,
     *               null[KEYWORD] AS last_name#779, null[KEYWORD] AS salary#780, null[KEYWORD] AS still_hired#781]]
     *         \_Eval[[null[DOUBLE] AS height.float#733, null[INTEGER] AS languages.byte#734, null[INTEGER] AS salary_change.int#735,
     *                 null[KEYWORD] AS salary_change.keyword#736, null[LONG] AS salary_change.long#737]]
     *           \_Subquery[]
     *             \_EsRelation[test_mixed_types][avg_worked_seconds{f}#727, birth_date{f}#715, emp_n..]
     */
    public void testUnionAllWithConflictingNumericTypesFromSubqueries() {
        LogicalPlan plan = analyzer().addDefaultIndex().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types) | keep emp_no
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Project[[!emp_no]]
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        assertUnsupportedAttribute(projections.getFirst(), "emp_no", List.of(INTEGER.esType(), LONG.esType()));
    }

    // mixed data types across subquery branches sourced from external datasets --

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!salary]]
     *   \_UnionAll[[emp_no{r}#10, name{r}#11, !salary]]
     *     |_Project[[emp_no{r}#3, name{r}#4, salary{r}#13]]
     *     | \_Eval[[null[KEYWORD] AS salary#13]]
     *     |   \_Subquery[]
     *     |     \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#3, name{r}#4, salary{r}#5]
     *     \_Project[[emp_no{r}#6, name{r}#7, salary{r}#14]]
     *       \_Eval[[null[KEYWORD] AS salary#14]]
     *         \_Subquery[]
     *           \_ExternalRelation[s3://bucket/salaries_long.parquet][parquet][emp_no{r}#6, name{r}#7, salary{r}#8]
     */
    public void testUnionAllWithConflictingTypesFromExternalDatasetSubqueries() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeExternalDatasetSubquery("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | KEEP salary
            """);

        Limit limit = as(plan, Limit.class);
        Project project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(1));
        assertUnsupportedAttribute(project.projections().getFirst(), "salary", List.of(INTEGER.esType(), LONG.esType()));

        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        // both branches are genuinely external dataset relations
        List<ExternalRelation> externalRelations = new ArrayList<>();
        unionAll.forEachDown(ExternalRelation.class, externalRelations::add);
        assertThat(externalRelations, hasSize(2));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[emp_no{r}#9, name{r}#10, !salary]]
     *   |_Project[[emp_no{r}#2, name{r}#3, salary{r}#12]]
     *   | \_Eval[[null[KEYWORD] AS salary#12]]
     *   |   \_Subquery[]
     *   |     \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     *   \_Project[[emp_no{r}#5, name{r}#6, salary{r}#13]]
     *     \_Eval[[null[KEYWORD] AS salary#13]]
     *       \_Subquery[]
     *         \_ExternalRelation[s3://bucket/salaries_long.parquet][parquet][emp_no{r}#5, name{r}#6, salary{r}#7]
     */
    public void testUnionAllWithConflictingTypesFromExternalDatasetSubqueriesWithoutUsage() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeExternalDatasetSubquery("""
            FROM (FROM salaries_int), (FROM salaries_long)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Attribute salary = unionAll.output().stream().filter(a -> "salary".equals(a.name())).findFirst().orElseThrow();
        assertUnsupportedAttribute(salary, "salary", List.of(INTEGER.esType(), LONG.esType()));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!x]]
     *   \_UnionAll[[!x]]
     *     |_Project[[x{r}#9]]
     *     | \_Eval[[null[KEYWORD] AS x#9]]
     *     |   \_Subquery[]
     *     |     \_Row[[1[INTEGER] AS x#4]]
     *     \_Project[[x{r}#10]]
     *       \_Eval[[null[KEYWORD] AS x#10]]
     *         \_Subquery[]
     *           \_Row[[abc[KEYWORD] AS x#6]]
     */
    public void testUnionAllWithConflictingTypesFromRowSubqueries() {
        LogicalPlan plan = analyzer().query("""
            FROM (ROW x = 1), (ROW x = "abc")
            | keep x
            """);

        Limit limit = as(plan, Limit.class);

        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        assertUnsupportedAttribute(projections.getFirst(), "x", List.of(INTEGER.esType(), KEYWORD.esType()));

        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Both legs share the structure Project → Eval[null[KEYWORD] AS x] → Subquery → Row
        // The Eval is the conflict-resolution null injected by the analyzer to align the leg
        // schema with the (UNSUPPORTED) union output.
        for (int i = 0; i < 2; i++) {
            Project legProject = as(unionAll.children().get(i), Project.class);
            Eval legConflictEval = as(legProject.child(), Eval.class);
            assertEquals(1, legConflictEval.fields().size());
            assertEquals("x", legConflictEval.fields().get(0).name());
            Literal legNull = as(legConflictEval.fields().get(0).child(), Literal.class);
            assertNull(legNull.value());
            assertEquals(KEYWORD, legNull.dataType());

            Subquery legSubquery = as(legConflictEval.child(), Subquery.class);
            Row row = as(legSubquery.child(), Row.class);
            assertEquals(1, row.fields().size());
            assertEquals("x", row.fields().get(0).name());
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[!x]]
     *   |_Project[[x{r}#8]]
     *   | \_Eval[[null[KEYWORD] AS x#8]]
     *   |   \_Subquery[]
     *   |     \_Row[[1[INTEGER] AS x#4]]
     *   \_Project[[x{r}#9]]
     *     \_Eval[[null[KEYWORD] AS x#9]]
     *       \_Subquery[]
     *         \_Row[[abc[KEYWORD] AS x#6]]
     */
    public void testUnionAllWithConflictingTypesFromRowSubqueriesWithoutUsageInMainQuery() {
        LogicalPlan plan = analyzer().query("""
            FROM (ROW x = 1), (ROW x = "abc")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        Attribute xAttr = output.stream().filter(a -> "x".equals(a.name())).findFirst().orElseThrow();
        assertUnsupportedAttribute(xAttr, "x", List.of(INTEGER.esType(), KEYWORD.esType()));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!client_ip]]
     *   \_UnionAll[[@timestamp{r}#13, !client_ip, event_duration{r}#15, message{r}#16]]
     *     |_Project[[@timestamp{f}#6, client_ip{r}#17, event_duration{f}#8, message{f}#9]]
     *     | \_Eval[[null[KEYWORD] AS client_ip#17]]
     *     |   \_Subquery[]
     *     |     \_EsRelation[sample_data][@timestamp{f}#6, client_ip{f}#7, event_duration{f}#..]
     *     \_Project[[@timestamp{r}#10, client_ip{r}#18, event_duration{r}#11, message{r}#12]]
     *       \_Eval[[null[KEYWORD] AS client_ip#18]]
     *         \_Eval[[null[DATETIME] AS @timestamp#10, null[LONG] AS event_duration#11, null[KEYWORD] AS message#12]]
     *           \_Subquery[]
     *             \_Row[[1[INTEGER] AS client_ip#4]]
     */
    public void testUnionAllWithConflictingTypesFromMixedRowAndFromSubqueries() {
        LogicalPlan plan = analyzer().addSampleData().query("""
            FROM (FROM sample_data), (ROW client_ip = 1)
            | keep client_ip
            """);

        Limit limit = as(plan, Limit.class);

        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        assertUnsupportedAttribute(projections.getFirst(), "client_ip", List.of(IP.esType(), INTEGER.esType()));

        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // FROM leg: Project → Eval[null[KEYWORD] AS client_ip] → Subquery → EsRelation[sample_data]
        // The single Eval is the conflict-resolution null override; sample_data already provides
        // every other column the union exposes, so there are no "missing field" nullEvals here.
        Project fromProject = as(unionAll.children().get(0), Project.class);
        Eval fromConflictEval = as(fromProject.child(), Eval.class);
        assertEquals(1, fromConflictEval.fields().size());
        assertEquals("client_ip", fromConflictEval.fields().get(0).name());
        Literal fromNull = as(fromConflictEval.fields().get(0).child(), Literal.class);
        assertNull(fromNull.value());
        assertEquals(KEYWORD, fromNull.dataType());
        Subquery fromSubquery = as(fromConflictEval.child(), Subquery.class);
        EsRelation fromRelation = as(fromSubquery.child(), EsRelation.class);
        assertEquals("sample_data", fromRelation.indexPattern());

        // ROW leg: Project → Eval[null[KEYWORD] AS client_ip] → Eval[3 nullEvals for missing
        // sample_data columns] → Subquery → Row[client_ip = 1]
        Project rowProject = as(unionAll.children().get(1), Project.class);
        Eval rowConflictEval = as(rowProject.child(), Eval.class);
        assertEquals(1, rowConflictEval.fields().size());
        assertEquals("client_ip", rowConflictEval.fields().get(0).name());
        Literal rowNull = as(rowConflictEval.fields().get(0).child(), Literal.class);
        assertNull(rowNull.value());
        assertEquals(KEYWORD, rowNull.dataType());
        Eval rowMissingEval = as(rowConflictEval.child(), Eval.class);
        assertEquals(3, rowMissingEval.fields().size()); // @timestamp, event_duration, message
        Subquery rowSubquery = as(rowMissingEval.child(), Subquery.class);
        Row row = as(rowSubquery.child(), Row.class);
        assertEquals("client_ip", row.fields().get(0).name());
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(1, rowLiteral.value());
        assertEquals(INTEGER, rowLiteral.dataType());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!emp_no]]
     *   \_UnionAll[[_meta_field{r}#27, !emp_no, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *               languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *     |_Project[[_meta_field{f}#12, emp_no{r}#38, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *     | \_Eval[[null[KEYWORD] AS emp_no#38]]
     *     |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *     \_Project[[_meta_field{r}#17, emp_no{r}#39, first_name{r}#18, gender{r}#19, hire_date{r}#20, job{r}#21, job.raw{r}#22,
     *                languages{r}#23, last_name{r}#24, long_noidx{r}#25, salary{r}#26]]
     *       \_Eval[[null[KEYWORD] AS emp_no#39]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#17, null[KEYWORD] AS first_name#18, null[TEXT] AS gender#19,
     *                 null[DATETIME] AS hire_date#20, null[TEXT] AS job#21, null[KEYWORD] AS job.raw#22, null[INTEGER] AS languages#23,
     *                 null[KEYWORD] AS last_name#24, null[LONG] AS long_noidx#25, null[INTEGER] AS salary#26]]
     *           \_Subquery[]
     *             \_Row[[abc[KEYWORD] AS emp_no#4]]
     */
    public void testUnionAllWithConflictingTypesFromRowSubqueryAndMainIndex() {
        LogicalPlan plan = analyzer().addEmployees().query("""
            FROM employees, (ROW emp_no = "abc")
            | keep emp_no
            """);

        Limit limit = as(plan, Limit.class);

        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        assertUnsupportedAttribute(projections.getFirst(), "emp_no", List.of(INTEGER.esType(), KEYWORD.esType()));

        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // index leg: Project → Eval[null[KEYWORD] AS emp_no] → EsRelation[test]
        // No "missing field" Eval is needed because the only ROW-introduced column (emp_no) is
        // already in the index and is the conflicting one (nulled out above).
        Project indexProject = as(unionAll.children().get(0), Project.class);
        Eval indexConflictEval = as(indexProject.child(), Eval.class);
        assertEquals(1, indexConflictEval.fields().size());
        assertEquals("emp_no", indexConflictEval.fields().get(0).name());
        Literal indexNull = as(indexConflictEval.fields().get(0).child(), Literal.class);
        assertNull(indexNull.value());
        assertEquals(KEYWORD, indexNull.dataType());
        EsRelation indexRelation = as(indexConflictEval.child(), EsRelation.class);
        assertEquals("employees", indexRelation.indexPattern());

        // ROW leg: Project → Eval[null[KEYWORD] AS emp_no] → Eval[10 nullEvals for missing
        // test fields] → Subquery → Row[emp_no = "abc"]
        Project rowProject = as(unionAll.children().get(1), Project.class);
        Eval rowConflictEval = as(rowProject.child(), Eval.class);
        assertEquals(1, rowConflictEval.fields().size());
        assertEquals("emp_no", rowConflictEval.fields().get(0).name());
        Literal rowNull = as(rowConflictEval.fields().get(0).child(), Literal.class);
        assertNull(rowNull.value());
        assertEquals(KEYWORD, rowNull.dataType());
        Eval rowMissingEval = as(rowConflictEval.child(), Eval.class);
        assertEquals(10, rowMissingEval.fields().size()); // every test field except emp_no
        Subquery rowSubquery = as(rowMissingEval.child(), Subquery.class);
        Row row = as(rowSubquery.child(), Row.class);
        assertEquals("emp_no", row.fields().get(0).name());
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(BytesRefs.toBytesRef("abc"), rowLiteral.value());
        assertEquals(KEYWORD, rowLiteral.dataType());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[!a, !b]]
     *   \_UnionAll[[!a, !b]]
     *     |_Project[[a{r}#15, b{r}#16]]
     *     | \_Eval[[null[KEYWORD] AS a#15, null[KEYWORD] AS b#16]]
     *     |   \_Subquery[]
     *     |     \_Row[[1[INTEGER] AS a#4, [[63 61 74], [64 6f 67]][KEYWORD] AS b#6]]
     *     \_Project[[a{r}#17, b{r}#18]]
     *       \_Eval[[null[KEYWORD] AS a#17, null[KEYWORD] AS b#18]]
     *         \_Subquery[]
     *           \_Row[[[1.5, 2.5][DOUBLE] AS a#8, true[BOOLEAN] AS b#10]]
     */
    public void testTwoRowSubqueriesEachWithMixedScalarAndMultivalueFieldsConflictingTypes() {
        LogicalPlan plan = analyzer().query("""
            FROM (ROW a = 1, b = ["cat", "dog"]), (ROW a = [1.5, 2.5], b = true)
            | KEEP a, b
            """);

        Limit limit = as(plan, Limit.class);
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(2));
        // Original types are reported in leg order: leg 0 declares INTEGER, leg 1 declares DOUBLE.
        assertUnsupportedAttribute(projections.get(0), "a", List.of(INTEGER.esType(), DOUBLE.esType()));
        assertUnsupportedAttribute(projections.get(1), "b", List.of(KEYWORD.esType(), BOOLEAN.esType()));

        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Both legs share the structure Project → Eval[null AS a, null AS b] → Subquery → Row.
        // The Eval is the conflict-resolution null injected by the analyzer to align each leg's
        // schema with the (UNSUPPORTED) union output.
        for (int i = 0; i < 2; i++) {
            Project legProject = as(unionAll.children().get(i), Project.class);
            Eval legConflictEval = as(legProject.child(), Eval.class);
            assertEquals(2, legConflictEval.fields().size());
            for (Alias alias : legConflictEval.fields()) {
                Literal nullLit = as(alias.child(), Literal.class);
                assertNull(nullLit.value());
            }

            Subquery legSubquery = as(legConflictEval.child(), Subquery.class);
            Row row = as(legSubquery.child(), Row.class);
            assertEquals(2, row.fields().size());
            assertEquals("a", row.fields().get(0).name());
            assertEquals("b", row.fields().get(1).name());
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *             languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, !x, !y]]
     *   |_Project[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20,
     *              languages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16, x{r}#59, y{r}#60]]
     *   | \_Eval[[null[KEYWORD] AS x#59, null[KEYWORD] AS y#60]]
     *   |   \_Eval[[null[INTEGER] AS x#22, null[KEYWORD] AS y#23]]
     *   |     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *   |_Project[[_meta_field{r}#24, emp_no{r}#25, first_name{r}#26, gender{r}#27, hire_date{r}#28, job{r}#29, job.raw{r}#30,
     *              languages{r}#31, last_name{r}#32, long_noidx{r}#33, salary{r}#34, x{r}#61, y{r}#62]]
     *   | \_Eval[[null[KEYWORD] AS x#61, null[KEYWORD] AS y#62]]
     *   |   \_Eval[[null[KEYWORD] AS _meta_field#24, null[INTEGER] AS emp_no#25, null[KEYWORD] AS first_name#26, null[TEXT] AS gender#27,
     *               null[DATETIME] AS hire_date#28, null[TEXT] AS job#29, null[KEYWORD] AS job.raw#30, null[INTEGER] AS languages#31,
     *               null[KEYWORD] AS last_name#32, null[LONG] AS long_noidx#33, null[INTEGER] AS salary#34]]
     *   |     \_Subquery[]
     *   |       \_Row[[1[INTEGER] AS x#4, [[63 61 74], [64 6f 67]][KEYWORD] AS y#6]]
     *   \_Project[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41,
     *              languages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, x{r}#63, y{r}#64]]
     *     \_Eval[[null[KEYWORD] AS x#63, null[KEYWORD] AS y#64]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#35, null[INTEGER] AS emp_no#36, null[KEYWORD] AS first_name#37, null[TEXT] AS gender#38,
     *               null[DATETIME] AS hire_date#39, null[TEXT] AS job#40, null[KEYWORD] AS job.raw#41, null[INTEGER] AS languages#42,
     *               null[KEYWORD] AS last_name#43, null[LONG] AS long_noidx#44, null[INTEGER] AS salary#45]]
     *         \_Subquery[]
     *           \_Row[[[1.5, -2.5][DOUBLE] AS x#8, true[BOOLEAN] AS y#10]]
     */
    public void testIndexPatternWithMixedRowSubqueriesAndConflictingTypes() {
        LogicalPlan plan = analyzer().addEmployees().query("""
            FROM employees, (ROW x = 1, y = ["cat", "dog"]), (ROW x = [1.5, -2.5], y = true)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // Schema: 11 test fields + x + y = 13. x and y are UnsupportedAttribute due to type
        // conflicts between the two ROWs (INTEGER vs DOUBLE for x, KEYWORD vs BOOLEAN for y).
        List<Attribute> output = unionAll.output();
        assertEquals(13, output.size());
        // Reported in leg order: the index leg synthesises a null x of the first ROW's type
        // (INTEGER), then ROW 1 contributes INTEGER and ROW 2 contributes DOUBLE.
        Attribute xAttr = output.stream().filter(a -> "x".equals(a.name())).findFirst().orElseThrow();
        assertUnsupportedAttribute(xAttr, "x", List.of(INTEGER.esType(), INTEGER.esType(), DOUBLE.esType()));
        // Same shape for y: KEYWORD null on the index leg, KEYWORD from ROW 1, BOOLEAN from ROW 2.
        Attribute yAttr = output.stream().filter(a -> "y".equals(a.name())).findFirst().orElseThrow();
        assertUnsupportedAttribute(yAttr, "y", List.of(KEYWORD.esType(), KEYWORD.esType(), BOOLEAN.esType()));

        // Index leg: Project → Eval[null[KEYWORD] AS x, null[KEYWORD] AS y] ← conflict-resolution
        // → Eval[null[INTEGER] AS x, null[KEYWORD] AS y] ← missing-field fill
        // → EsRelation[test].
        Project indexProject = as(unionAll.children().get(0), Project.class);
        Eval indexConflictEval = as(indexProject.child(), Eval.class);
        assertEquals(2, indexConflictEval.fields().size());
        Eval indexMissingEval = as(indexConflictEval.child(), Eval.class);
        assertEquals(2, indexMissingEval.fields().size());
        EsRelation indexRelation = as(indexMissingEval.child(), EsRelation.class);
        assertEquals("employees", indexRelation.indexPattern());

        // ROW legs: Project → Eval[conflict-resolution null for x, y]
        // → Eval[11 nullEvals for the test-only fields]
        // → Subquery → Row.
        for (int i = 1; i < 3; i++) {
            Project rowLegProject = as(unionAll.children().get(i), Project.class);
            Eval rowConflictEval = as(rowLegProject.child(), Eval.class);
            assertEquals(2, rowConflictEval.fields().size()); // x, y conflict-resolution nulls
            Eval rowMissingEval = as(rowConflictEval.child(), Eval.class);
            assertEquals(11, rowMissingEval.fields().size()); // 11 test-index fields nulled
            Subquery rowLegSubquery = as(rowMissingEval.child(), Subquery.class);
            Row row = as(rowLegSubquery.child(), Row.class);
            assertEquals(2, row.fields().size());
            assertEquals("x", row.fields().get(0).name());
            assertEquals("y", row.fields().get(1).name());
        }
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_UnionAll[[!m, cluster{r}#2360, @timestamp{r}#2361, client_ip{r}#2362, event_duration{r}#2363, message{r}#2364]]
     *   |_Project[[m{r}#2365, cluster{f}#2326, @timestamp{r}#2354, client_ip{r}#2355, event_duration{r}#2356, message{r}#2357]]
     *   | \_Eval[[null[KEYWORD] AS m#2365]]
     *   |   \_Eval[[null[DATETIME] AS @timestamp#2354, null[IP] AS client_ip#2355, null[LONG] AS event_duration#2356,
     *               null[KEYWORD] AS message#2357]]
     *   |     \_Subquery[]
     *   |       \_Project[[m{r}#6, cluster{r}#11]]
     *   |         \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#56) AS cluster#11]]
     *   |           \_Aggregate[[pack_cluster_$1{r}#55 AS group_cluster_$1#56],
     *                           [MAX(RATE_$1{r}#53,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#6, group_cluster_$1{r}#56]]
     *   |             \_Eval[[PACKDIMENSION(cluster{r}#54) AS pack_cluster_$1#55]]
     *   |               \_TimeSeriesAggregate[[_tsid{m}#52],[RATE(network.total_bytes_in{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                         @timestamp{f}#10) AS RATE_$1#53, DIMENSIONVALUES(cluster{f}#11,true[BOOLEAN],
     *                                         PT0S[TIME_DURATION]) AS cluster#54, _tsid{m}#52],null,null,@timestamp{f}#10,TS_COMMAND]
     *   |                 \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#10, client.ip{f}#14, cluster{f}#11, e..]
     *   \_Project[[m{r}#2366, cluster{r}#2358, @timestamp{f}#2349, client_ip{f}#2350, event_duration{f}#2351, message{f}#2352]]
     *     \_Eval[[null[KEYWORD] AS m#2366]]
     *       \_Eval[[null[KEYWORD] AS cluster#2358]]
     *         \_Subquery[]
     *           \_Eval[[abc[KEYWORD] AS m#2324]]
     *             \_EsRelation[sample_data][@timestamp{f}#2349, client_ip{f}#2350, event_durati..]
     */
    public void testTSSubqueryWithConflictingTypesInUnionAll() {
        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | EVAL m = "abc")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        assertEquals(6, output.size());
        assertUnsupportedAttribute(output.get(0), "m", List.of(DOUBLE.esType(), KEYWORD.esType()));

        // Branch 0: TS leg
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsNullM = as(tsProject.child(), Eval.class);
        assertEquals(1, tsNullM.fields().size());
        Alias tsNullMAlias = tsNullM.fields().get(0);
        assertEquals("m", tsNullMAlias.name());
        Literal tsNullMLit = as(tsNullMAlias.child(), Literal.class);
        assertNull(tsNullMLit.value());
        assertEquals(KEYWORD, tsNullMLit.dataType());

        Eval tsNullSampleFields = as(tsNullM.child(), Eval.class);
        assertEquals(4, tsNullSampleFields.fields().size());
        Subquery tsSubquery = as(tsNullSampleFields.child(), Subquery.class);
        // The TS STATS BY clause is now expanded: Project -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TimeSeriesAggregate
        Project tsInnerProject = as(tsSubquery.child(), Project.class);
        Eval tsUnpackEval = as(tsInnerProject.child(), Eval.class);
        assertEquals(1, tsUnpackEval.fields().size());
        Aggregate tsOuterAggregate = as(tsUnpackEval.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        assertEquals(1, tsOuterAggregate.groupings().size());
        Eval tsPackEval = as(tsOuterAggregate.child(), Eval.class);
        TimeSeriesAggregate tsAggregate = as(tsPackEval.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullM = as(sampleProject.child(), Eval.class);
        assertEquals(1, sampleNullM.fields().size());
        Alias sampleNullMAlias = sampleNullM.fields().get(0);
        assertEquals("m", sampleNullMAlias.name());
        Literal sampleNullMLit = as(sampleNullMAlias.child(), Literal.class);
        assertNull(sampleNullMLit.value());
        assertEquals(KEYWORD, sampleNullMLit.dataType());

        Eval sampleNullCluster = as(sampleNullM.child(), Eval.class);
        assertEquals(1, sampleNullCluster.fields().size());
        assertEquals("cluster", sampleNullCluster.fields().get(0).name());
        Subquery sampleSubquery = as(sampleNullCluster.child(), Subquery.class);
        Eval innerEval = as(sampleSubquery.child(), Eval.class);
        assertEquals(1, innerEval.fields().size());
        Alias innerMAlias = innerEval.fields().get(0);
        assertEquals("m", innerMAlias.name());
        Literal innerMLit = as(innerMAlias.child(), Literal.class);
        assertEquals(KEYWORD, innerMLit.dataType());
        EsRelation sampleRelation = as(innerEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /*
     * Project[[m{r}#12]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Project[[m{r}#12, $$m$converted_to$keyword{r$}#56]]
     *     \_Eval[[$$m$converted_to$keyword{r$}#56 AS m#12]]
     *       \_UnionAll[[!m, $$m$converted_to$keyword{r$}#56, cluster{r}#49, @timestamp{r}#50, client_ip{r}#51, event_duration{r}#52,
     *                   message{r}#53]]
     *         |_Project[[m{r}#57, $$m$converted_to$keyword{r$}#54, cluster{f}#15, @timestamp{r}#43, client_ip{r}#44, event_duration{r}#45,
     *                    message{r}#46]]
     *         | \_Eval[[null[KEYWORD] AS m#57]]
     *         |   \_Eval[[TOSTRING(m{r}#6) AS $$m$converted_to$keyword#54]]
     *         |     \_Eval[[null[DATETIME] AS @timestamp#43, null[IP] AS client_ip#44, null[LONG] AS event_duration#45,
     *                       null[KEYWORD] AS message#46]]
     *         |       \_Subquery[]
     *         |         \_Project[[m{r}#6, cluster{r}#15]]
     *         |           \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#63) AS cluster#15]]
     *         |             \_Aggregate[[pack_cluster_$1{r}#62 AS group_cluster_$1#63],
     *                                   [MAX(RATE_$1{r}#60,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#6, group_cluster_$1{r}#63]]
     *         |               \_Eval[[PACKDIMENSION(cluster{r}#61) AS pack_cluster_$1#62]]
     *         |                 \_TimeSeriesAggregate[[_tsid{m}#59],[RATE(network.total_bytes_in{f}#28,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                                  @timestamp{f}#14) AS RATE_$1#60, VALUES(cluster{f}#15,true[BOOLEAN],
     *                                                  PT0S[TIME_DURATION]) AS cluster#61, _tsid{m}#59],null,null,@timestamp{f}#14,
     *                                                  TS_COMMAND]
     *         |                   \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#14, client.ip{f}#18, cluster{f}#15, e..]
     *         \_Project[[m{r}#58, $$m$converted_to$keyword{r$}#55, cluster{r}#47, @timestamp{f}#38, client_ip{f}#39,
     *                    event_duration{f}#40, message{f}#41]]
     *           \_Eval[[null[KEYWORD] AS m#58]]
     *             \_Eval[[TOSTRING(m{r}#9) AS $$m$converted_to$keyword#55]]
     *               \_Eval[[null[KEYWORD] AS cluster#47]]
     *                 \_Subquery[]
     *                   \_Eval[[abc[KEYWORD] AS m#9]]
     *                     \_EsRelation[sample_data][@timestamp{f}#38, client_ip{f}#39, event_duration{f..]
     */
    public void testTSSubqueryWithConflictingTypesAndExplicitCast() {
        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | EVAL m = "abc")
            | EVAL m = m::string
            | KEEP m
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(1, topProject.projections().size());
        Limit limit = as(topProject.child(), Limit.class);
        topProject = as(limit.child(), Project.class);
        assertEquals(2, topProject.projections().size());
        assertEquals("m", topProject.projections().get(0).name());

        Eval topEval = as(topProject.child(), Eval.class);
        assertEquals(1, topEval.fields().size());
        Alias mFromCast = topEval.fields().get(0);
        assertEquals("m", mFromCast.name());
        ReferenceAttribute castRef = as(mFromCast.child(), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$keyword", castRef.name());
        assertEquals(KEYWORD, castRef.dataType());

        UnionAll unionAll = as(topEval.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        assertEquals(7, output.size());
        UnsupportedAttribute mUnsupported = as(output.get(0), UnsupportedAttribute.class);
        assertEquals("m", mUnsupported.name());
        assertEquals(UNSUPPORTED, mUnsupported.dataType());
        ReferenceAttribute castedAttr = as(output.get(1), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$keyword", castedAttr.name());
        assertEquals(KEYWORD, castedAttr.dataType());

        // Branch 0: TS leg — TOSTRING(m) eval is inserted above the null sample-data evals.
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsNullM = as(tsProject.child(), Eval.class);
        assertEquals(1, tsNullM.fields().size());
        assertEquals("m", tsNullM.fields().get(0).name());
        Eval tsCastEval = as(tsNullM.child(), Eval.class);
        assertEquals(1, tsCastEval.fields().size());
        Alias tsCastAlias = tsCastEval.fields().get(0);
        assertEquals("$$m$converted_to$keyword", tsCastAlias.name());
        Eval tsNullSampleFields = as(tsCastEval.child(), Eval.class);
        assertEquals(4, tsNullSampleFields.fields().size());
        Subquery tsSubquery = as(tsNullSampleFields.child(), Subquery.class);
        // The TS STATS BY clause is now expanded: Project -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TimeSeriesAggregate
        Project tsInnerProject = as(tsSubquery.child(), Project.class);
        Eval tsUnpackEval = as(tsInnerProject.child(), Eval.class);
        Aggregate tsOuterAggregate = as(tsUnpackEval.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        Eval tsPackEval = as(tsOuterAggregate.child(), Eval.class);
        TimeSeriesAggregate tsAggregate = as(tsPackEval.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data — TOSTRING(m) is inserted above the cluster null and inner literal eval.
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullM = as(sampleProject.child(), Eval.class);
        assertEquals("m", sampleNullM.fields().get(0).name());
        Eval sampleCastEval = as(sampleNullM.child(), Eval.class);
        assertEquals(1, sampleCastEval.fields().size());
        assertEquals("$$m$converted_to$keyword", sampleCastEval.fields().get(0).name());
        Eval sampleNullCluster = as(sampleCastEval.child(), Eval.class);
        assertEquals("cluster", sampleNullCluster.fields().get(0).name());
        Subquery sampleSubquery = as(sampleNullCluster.child(), Subquery.class);
        Eval innerEval = as(sampleSubquery.child(), Eval.class);
        Alias innerMAlias = innerEval.fields().get(0);
        assertEquals("m", innerMAlias.name());
        assertEquals(KEYWORD, as(innerMAlias.child(), Literal.class).dataType());
        EsRelation sampleRelation = as(innerEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /*
     * planNoCast:
     * Limit[10000[INTEGER],false,false]
     * \_UnionAll[[!m, @timestamp{r}#1799, client_ip{r}#1800, event_duration{r}#1801, message{r}#1802]]
     *   |_Project[[m{r}#1803, @timestamp{r}#1794, client_ip{r}#1795, event_duration{r}#1796, message{r}#1797]]
     *   | \_Eval[[null[KEYWORD] AS m#1803]]
     *   |   \_Eval[[null[DATETIME] AS @timestamp#1794, null[IP] AS client_ip#1795, null[LONG] AS event_duration#1796,
     *               null[KEYWORD] AS message#1797]]
     *   |     \_Subquery[]
     *   |       \_Aggregate[[],[SUM(LASTOVERTIME_$1{r}#50,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD],
     *                           long_overflow_throw[KEYWORD]) AS m#5]]
     *   |         \_TimeSeriesAggregate[[_tsid{m}#49],[LASTOVERTIME(network.bytes_in{f}#22,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                    @timestamp{f}#9) AS LASTOVERTIME_$1#50, _tsid{m}#49],null,null,@timestamp{f}#9,TS_COMMAND]
     *   |         \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#1765, client.ip{f}#1769, cluster{f}#1..]
     *   \_Project[[m{r}#1804, @timestamp{f}#1789, client_ip{f}#1790, event_duration{f}#1791, message{f}#1792]]
     *     \_Eval[[null[KEYWORD] AS m#1804]]
     *       \_Subquery[]
     *         \_Eval[[1.5[DOUBLE] AS m#1764]]
     *           \_EsRelation[sample_data][@timestamp{f}#1789, client_ip{f}#1790, event_durati..]
     *
     * planCast:
     * Project[[m{r}#57]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Project[[m{r}#57, $$m$converted_to$double{r$}#99]]
     *     \_Eval[[$$m$converted_to$double{r$}#99 AS m#57]]
     *       \_UnionAll[[!m, $$m$converted_to$double{r$}#99, @timestamp{r}#93, client_ip{r}#94, event_duration{r}#95, message{r}#96]]
     *         |_Project[[m{r}#100, $$m$converted_to$double{r$}#97, @timestamp{r}#88, client_ip{r}#89, event_duration{r}#90,
     *                    message{r}#91]]
     *         | \_Eval[[null[KEYWORD] AS m#100]]
     *         |   \_Eval[[TODOUBLE(m{r}#51) AS $$m$converted_to$double#97]]
     *         |     \_Eval[[null[DATETIME] AS @timestamp#88, null[IP] AS client_ip#89, null[LONG] AS event_duration#90,
     *                       null[KEYWORD] AS message#91]]
     *         |       \_Subquery[]
     *         |         \_Aggregate[[],[SUM(LASTOVERTIME_$1{r}#105,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD],
     *                                   long_overflow_throw[KEYWORD]) AS m#53]]
     *         |           \_TimeSeriesAggregate[[_tsid{m}#104],[LASTOVERTIME(network.bytes_in{f}#74,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                            @timestamp{f}#61) AS LASTOVERTIME_$1#105, _tsid{m}#104],null,null,@timestamp{f}#61,
     *                                            TS_COMMAND]
     *         |             \_EsRelation[k8s][@timestamp{f}#61, client.ip{f}#65, cluster{f}#62, e..]
     *         \_Project[[m{r}#101, $$m$converted_to$double{r$}#98, @timestamp{f}#83, client_ip{f}#84, event_duration{f}#85,
     *                    message{f}#86]]
     *           \_Eval[[null[KEYWORD] AS m#101]]
     *             \_Eval[[TODOUBLE(m{r}#54) AS $$m$converted_to$double#98]]
     *               \_Subquery[]
     *                 \_Eval[[1.5[DOUBLE] AS m#54]]
     *                   \_EsRelation[sample_data][@timestamp{f}#83, client_ip{f}#84, event_duration{f..]
     */
    public void testTSSubqueryWithNumericConflictAndExplicitCast() {
        // (1) Without explicit cast: LONG vs DOUBLE → UNSUPPORTED.
        LogicalPlan planNoCast = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = sum(last_over_time(network.bytes_in))),
              (FROM sample_data | EVAL m = 1.5)
            """);

        Limit noCastLimit = as(planNoCast, Limit.class);
        UnionAll noCastUnion = as(noCastLimit.child(), UnionAll.class);
        assertEquals(2, noCastUnion.children().size());
        List<Attribute> noCastOutput = noCastUnion.output();
        assertEquals(5, noCastOutput.size());
        assertUnsupportedAttribute(noCastOutput.get(0), "m", List.of(LONG.esType(), DOUBLE.esType()));

        Project tsProject = as(noCastUnion.children().get(0), Project.class);
        Eval tsNullM = as(tsProject.child(), Eval.class);
        assertEquals("m", tsNullM.fields().get(0).name());
        assertEquals(KEYWORD, as(tsNullM.fields().get(0).child(), Literal.class).dataType());
        Eval tsNullSampleFields = as(tsNullM.child(), Eval.class);
        assertEquals(4, tsNullSampleFields.fields().size());
        Subquery tsSubquery = as(tsNullSampleFields.child(), Subquery.class);
        // No BY clause: outer Aggregate -> inner TimeSeriesAggregate (no PACK/UNPACK layers)
        Aggregate tsOuterAggregate = as(tsSubquery.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        assertTrue(tsOuterAggregate.groupings().isEmpty());
        TimeSeriesAggregate tsAggregate = as(tsOuterAggregate.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals(IndexMode.STANDARD, tsRelation.indexMode());

        Project sampleProject = as(noCastUnion.children().get(1), Project.class);
        Eval sampleNullM = as(sampleProject.child(), Eval.class);
        assertEquals("m", sampleNullM.fields().get(0).name());
        Subquery sampleSubquery = as(sampleNullM.child(), Subquery.class);
        Eval innerEval = as(sampleSubquery.child(), Eval.class);
        Literal oneAndAHalf = as(innerEval.fields().get(0).child(), Literal.class);
        assertEquals(DOUBLE, oneAndAHalf.dataType());
        assertEquals(1.5, oneAndAHalf.value());
        EsRelation sampleRelation = as(innerEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        // (2) With explicit cast m::double: TODOUBLE is pushed into each branch.
        LogicalPlan planCast = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = sum(last_over_time(network.bytes_in))),
              (FROM sample_data | EVAL m = 1.5)
            | EVAL m = m::double
            | KEEP m
            """);

        Project castTopProject = as(planCast, Project.class);
        assertEquals(1, castTopProject.projections().size());
        Limit castLimit = as(castTopProject.child(), Limit.class);
        castTopProject = as(castLimit.child(), Project.class);
        assertEquals(2, castTopProject.projections().size());
        Eval castTopEval = as(castTopProject.child(), Eval.class);
        Alias mFromCast = castTopEval.fields().get(0);
        assertEquals("m", mFromCast.name());
        ReferenceAttribute castRef = as(mFromCast.child(), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$double", castRef.name());
        assertEquals(DOUBLE, castRef.dataType());

        UnionAll castUnion = as(castTopEval.child(), UnionAll.class);
        assertEquals(2, castUnion.children().size());
        ReferenceAttribute castedOutput = as(castUnion.output().get(1), ReferenceAttribute.class);
        assertEquals("$$m$converted_to$double", castedOutput.name());
        assertEquals(DOUBLE, castedOutput.dataType());

        Project castTsProject = as(castUnion.children().get(0), Project.class);
        Eval castTsNullM = as(castTsProject.child(), Eval.class);
        assertEquals("m", castTsNullM.fields().get(0).name());
        Eval castTsConvertEval = as(castTsNullM.child(), Eval.class);
        assertEquals("$$m$converted_to$double", castTsConvertEval.fields().get(0).name());
        Eval castTsNullSamples = as(castTsConvertEval.child(), Eval.class);
        assertEquals(4, castTsNullSamples.fields().size());
        Subquery castTsSubquery = as(castTsNullSamples.child(), Subquery.class);
        // No BY clause: outer Aggregate -> inner TimeSeriesAggregate (no PACK/UNPACK layers)
        Aggregate castTsOuterAggregate = as(castTsSubquery.child(), Aggregate.class);
        assertFalse(castTsOuterAggregate instanceof TimeSeriesAggregate);
        TimeSeriesAggregate castTsAggregate = as(castTsOuterAggregate.child(), TimeSeriesAggregate.class);
        EsRelation castTsRelation = as(castTsAggregate.child(), EsRelation.class);
        assertEquals(IndexMode.STANDARD, castTsRelation.indexMode());

        Project castSampleProject = as(castUnion.children().get(1), Project.class);
        Eval castSampleNullM = as(castSampleProject.child(), Eval.class);
        assertEquals("m", castSampleNullM.fields().get(0).name());
        Eval castSampleConvertEval = as(castSampleNullM.child(), Eval.class);
        assertEquals("$$m$converted_to$double", castSampleConvertEval.fields().get(0).name());
        Subquery castSampleSubquery = as(castSampleConvertEval.child(), Subquery.class);
        Eval castSampleInnerEval = as(castSampleSubquery.child(), Eval.class);
        assertEquals(DOUBLE, as(castSampleInnerEval.fields().get(0).child(), Literal.class).dataType());
        EsRelation castSampleRelation = as(castSampleInnerEval.child(), EsRelation.class);
        assertEquals("sample_data", castSampleRelation.indexPattern());
    }

    /*
     * Project[[x{r}#?]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[x{r}#?,ASC,LAST]]]
     *     \_Project[[x{r}#?, $$@timestamp$converted_to$long{r$}#?]]
     *       \_Project[[@timestamp{r}#? AS x#?, $$@timestamp$converted_to$long{r$}#?]]
     *         \_Project[[@timestamp{r}#?, $$@timestamp$converted_to$long{r$}#?]]
     *           \_Eval[[$$@timestamp$converted_to$long{r$}#? AS @timestamp#?]]
     *             \_UnionAll[[!@timestamp, $$@timestamp$converted_to$long{r$}#?, client_ip{r}#?, event_duration{r}#?, message{r}#?]]
     *               |_Project[[@timestamp{r}#?, $$@timestamp$converted_to$long{r$}#?, client_ip{f}#?, event_duration{f}#?, message{f}#?]]
     *               | \_Eval[[null[KEYWORD] AS @timestamp#?]]
     *               |   \_Eval[[TOLONG(@timestamp{f}#?) AS $$@timestamp$converted_to$long#?]]
     *               |     \_EsRelation[sample_data][@timestamp{f}#?(date), client_ip{f}#?, event_duration..]
     *               \_Project[...same shape, sample_data_ts_long...]
     *                 \_Eval[[null[KEYWORD] AS @timestamp#?]]
     *                   \_Eval[[TOLONG(@timestamp{f}#?) AS $$@timestamp$converted_to$long#?]]
     *                     \_Subquery[]
     *                       \_EsRelation[sample_data_ts_long][@timestamp{f}#?(long), client_ip{f}#?, event_duration..]
     *
     * Mixed {@code date}/{@code long} {@code @timestamp} variant of {@link #testSubqueryRenameKeepOnDateAndDateNanosTimestamp()}.
     */
    public void testSubqueryRenameKeepOnDateAndLongTimestampWithExplicitCast() {
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsLongIndex()).query("""
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """);

        // Outer pruning Project that hides the synthetic $$@timestamp$converted_to$long attribute.
        Project outer = as(plan, Project.class);
        assertEquals(1, outer.projections().size());
        ReferenceAttribute xOut = as(outer.projections().get(0), ReferenceAttribute.class);
        assertEquals("x", xOut.name());
        assertEquals(LONG, xOut.dataType());

        Limit limit = as(outer.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute xOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("x", xOrder.name());
        assertEquals(LONG, xOrder.dataType());

        // KEEP * — projects x (LONG) plus the carried-over synthetic LONG attribute.
        Project project = as(orderBy.child(), Project.class);
        assertProjectionHasLong(project, "x", ReferenceAttribute.class);
        assertProjectionHasSyntheticTimestampLong(project);

        // RENAME @timestamp AS x — alias x is LONG (cascaded from the EVAL).
        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "x", Alias.class);

        // KEEP @timestamp — reference to the rebound @timestamp (LONG via EVAL).
        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "@timestamp", ReferenceAttribute.class);

        // EVAL @timestamp = @timestamp::long — replaced with synthetic LONG attribute.
        Eval eval = as(project.child(), Eval.class);
        Alias timestampEval = as(eval.fields().stream().filter(f -> "@timestamp".equals(f.name())).findFirst().orElseThrow(), Alias.class);
        assertEquals(LONG, timestampEval.dataType());
        ReferenceAttribute syntheticRef = as(timestampEval.child(), ReferenceAttribute.class);
        assertEquals(LONG, syntheticRef.dataType());

        UnionAll unionAll = as(eval.child(), UnionAll.class);
        // The original @timestamp in the UnionAll output is UnsupportedAttribute (date + long).
        Attribute timestampAttr = unionAll.output().stream().filter(a -> "@timestamp".equals(a.name())).findFirst().orElseThrow();
        as(timestampAttr, UnsupportedAttribute.class);
        // The synthetic $$@timestamp$converted_to$long carries the LONG cast.
        assertTrue(unionAll.output().stream().anyMatch(a -> isSyntheticTimestampLong(a) && LONG.equals(a.dataType())));
    }

    /*
     * Project[[y{r}#?]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[y{r}#?,ASC,LAST]]]
     *     \_Project[[y{r}#?, $$@timestamp$converted_to$long{r$}#?]]
     *       \_Project[[@timestamp{r}#? AS y#?, $$@timestamp$converted_to$long{r$}#?]]
     *         \_Project[[@timestamp{r}#?, $$@timestamp$converted_to$long{r$}#?]]
     *           \_Eval[[$$@timestamp$converted_to$long{r$}#? AS @timestamp#?]]
     *             \_UnionAll[...]
     *
     * Chained {@code RENAME @timestamp AS x, x AS y} variant of the explicit-cast date/long test.
     */
    public void testSubqueryRenameChainKeepOnDateAndLongTimestampWithExplicitCast() {
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsLongIndex()).query("""
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x, x AS y
            | KEEP y
            | SORT y
            """);

        Project outer = as(plan, Project.class);
        assertEquals(1, outer.projections().size());
        ReferenceAttribute yOut = as(outer.projections().get(0), ReferenceAttribute.class);
        assertEquals("y", yOut.name());
        assertEquals(LONG, yOut.dataType());

        Limit limit = as(outer.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        ReferenceAttribute yOrder = as(orderBy.order().get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(LONG, yOrder.dataType());

        Project project = as(orderBy.child(), Project.class);
        assertProjectionHasLong(project, "y", ReferenceAttribute.class);
        assertProjectionHasSyntheticTimestampLong(project);

        // The chain rename collapses to a single Project: @timestamp AS y.
        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "y", Alias.class);

        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "@timestamp", ReferenceAttribute.class);

        Eval eval = as(project.child(), Eval.class);
        Alias timestampEval = as(eval.fields().stream().filter(f -> "@timestamp".equals(f.name())).findFirst().orElseThrow(), Alias.class);
        assertEquals(LONG, timestampEval.dataType());

        UnionAll unionAll = as(eval.child(), UnionAll.class);
        as(unionAll.output().stream().filter(a -> "@timestamp".equals(a.name())).findFirst().orElseThrow(), UnsupportedAttribute.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> isSyntheticTimestampLong(a) && LONG.equals(a.dataType())));
    }

    /*
     * Project[[y{r}#?]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[y{r}#?,ASC,LAST]]]
     *     \_Project[[y{r}#?, $$@timestamp$converted_to$long{r$}#?]]
     *       \_Project[[x{r}#? AS y#?, $$@timestamp$converted_to$long{r$}#?]]
     *         \_Project[[@timestamp{r}#? AS x#?, $$@timestamp$converted_to$long{r$}#?]]
     *           \_Project[[@timestamp{r}#?, $$@timestamp$converted_to$long{r$}#?]]
     *             \_Eval[[$$@timestamp$converted_to$long{r$}#? AS @timestamp#?]]
     *               \_UnionAll[...]
     *
     * Two separate {@code RENAME} commands variant of the explicit-cast date/long test.
     */
    public void testSubqueryDoubleRenameKeepStarOnDateAndLongTimestampWithExplicitCast() {
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsLongIndex()).query("""
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | RENAME x AS y
            | KEEP *
            | SORT y
            """);

        Project outer = as(plan, Project.class);
        assertEquals(1, outer.projections().size());
        ReferenceAttribute yOut = as(outer.projections().get(0), ReferenceAttribute.class);
        assertEquals("y", yOut.name());
        assertEquals(LONG, yOut.dataType());

        Limit limit = as(outer.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        ReferenceAttribute yOrder = as(orderBy.order().get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(LONG, yOrder.dataType());

        // KEEP * with y (LONG) plus the carried synthetic.
        Project project = as(orderBy.child(), Project.class);
        assertProjectionHasLong(project, "y", ReferenceAttribute.class);
        assertProjectionHasSyntheticTimestampLong(project);

        // RENAME x AS y (second rename).
        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "y", Alias.class);

        // RENAME @timestamp AS x (first rename).
        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "x", Alias.class);

        // KEEP @timestamp.
        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "@timestamp", ReferenceAttribute.class);

        Eval eval = as(project.child(), Eval.class);
        Alias timestampEval = as(eval.fields().stream().filter(f -> "@timestamp".equals(f.name())).findFirst().orElseThrow(), Alias.class);
        assertEquals(LONG, timestampEval.dataType());

        UnionAll unionAll = as(eval.child(), UnionAll.class);
        as(unionAll.output().stream().filter(a -> "@timestamp".equals(a.name())).findFirst().orElseThrow(), UnsupportedAttribute.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> isSyntheticTimestampLong(a) && LONG.equals(a.dataType())));
    }

    /*
     * Project[[x{r}#9]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[x{r}#9,ASC,LAST]]]
     *     \_Project[[x{r}#9, $$@timestamp$converted_to$long{r$}#27]]
     *       \_Project[[@timestamp{r}#5 AS x#9, $$@timestamp$converted_to$long{r$}#27]]
     *         \_Project[[@timestamp{r}#5, $$@timestamp$converted_to$long{r$}#27]]
     *           \_Eval[[$$@timestamp$converted_to$long{r$}#27 AS @timestamp#5]]
     *             \_UnionAll[[!@timestamp, $$@timestamp$converted_to$long{r$}#27, client_ip{r}#22, event_duration{r}#23, message{r}#24]]
     *               |_Project[[@timestamp{r}#28, $$@timestamp$converted_to$long{r$}#25, client_ip{f}#14, event_duration{f}#15,
     *                          message{f}#16]]
     *               | \_Eval[[null[KEYWORD] AS @timestamp#28]]
     *               |   \_Eval[[TOLONG(@timestamp{f}#13) AS $$@timestamp$converted_to$long#25]]
     *               |     \_EsRelation[sample_data][@timestamp{f}#13, client_ip{f}#14, event_duration{f..]
     *               \_Project[[@timestamp{r}#29, $$@timestamp$converted_to$long{r$}#26, client_ip{f}#19, event_duration{f}#20,
     *                          message{f}#17]]
     *                 \_Eval[[null[KEYWORD] AS @timestamp#29]]
     *                   \_Eval[[TOLONG(@timestamp{f}#18) AS $$@timestamp$converted_to$long#26]]
     *                     \_Subquery[]
     *                       \_EsRelation[sample_data_ts_long][@timestamp{f}#18, client_ip{f}#19, event_duration{f..]
     *
     * Same shape as {@code testSubqueryRenameKeepOnDateAndLongTimestampWithExplicitCast()} with {@code SET unmapped_fields="nullify"}.
     */
    public void testSubqueryRenameKeepOnDateAndLongTimestampWithExplicitCastAndNullify() {
        requireNullifySupport();
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsLongIndex()).statement("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """);

        Project outer = as(plan, Project.class);
        assertEquals(1, outer.projections().size());
        ReferenceAttribute xOut = as(outer.projections().get(0), ReferenceAttribute.class);
        assertEquals("x", xOut.name());
        assertEquals(LONG, xOut.dataType());

        Limit limit = as(outer.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        ReferenceAttribute xOrder = as(orderBy.order().get(0).child(), ReferenceAttribute.class);
        assertEquals("x", xOrder.name());
        assertEquals(LONG, xOrder.dataType());

        Project project = as(orderBy.child(), Project.class);
        assertProjectionHasLong(project, "x", ReferenceAttribute.class);
        assertProjectionHasSyntheticTimestampLong(project);

        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "x", Alias.class);

        project = as(project.child(), Project.class);
        assertProjectionHasLong(project, "@timestamp", ReferenceAttribute.class);

        Eval eval = as(project.child(), Eval.class);
        Alias timestampEval = as(eval.fields().stream().filter(f -> "@timestamp".equals(f.name())).findFirst().orElseThrow(), Alias.class);
        assertEquals(LONG, timestampEval.dataType());

        UnionAll unionAll = as(eval.child(), UnionAll.class);
        as(unionAll.output().stream().filter(a -> "@timestamp".equals(a.name())).findFirst().orElseThrow(), UnsupportedAttribute.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> isSyntheticTimestampLong(a) && LONG.equals(a.dataType())));
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
     * Analyzes a subquery query over two external datasets ({@code salaries_int}/{@code salaries_long}) that share
     * {@code emp_no}/{@code name} but type {@code salary} differently ({@code integer} vs {@code long}). Mirrors the
     * production pipeline: {@link DatasetRewriter} turns each {@code FROM <dataset>} into the
     * {@code UnresolvedExternalRelation} the {@code EXTERNAL} command produces, which the analyzer resolves against the
     * configured external source schemas — so a dataset branch is backed by an {@link ExternalRelation}, exactly like a
     * real dataset subquery. The plan is analyzed (not optimized) to match the neighbouring tests.
     */
    private static LogicalPlan analyzeExternalDatasetSubquery(String query) {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset intDataset = new Dataset("salaries_int", new DataSourceReference("external_ds"), SALARIES_INT_RESOURCE, null, Map.of());
        Dataset longDataset = new Dataset("salaries_long", new DataSourceReference("external_ds"), SALARIES_LONG_RESOURCE, null, Map.of());
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of("salaries_int", intDataset, "salaries_long", longDataset))
            .build();
        LogicalPlan rewritten = DatasetRewriter.rewriteUnsecured(
            TEST_PARSER.parseQuery(query),
            projectMetadata,
            TestIndexNameExpressionResolver.newInstance()
        );
        ExternalSourceResolution resolution = new ExternalSourceResolution(
            Map.of(
                SALARIES_INT_RESOURCE,
                externalSource(SALARIES_INT_RESOURCE, INTEGER),
                SALARIES_LONG_RESOURCE,
                externalSource(SALARIES_LONG_RESOURCE, LONG)
            )
        );
        return analyzer().externalSourceResolution(resolution).buildAnalyzer().analyze(rewritten);
    }

    /** A resolved external source named {@code emp_no}/{@code name}/{@code salary} with the given salary type. */
    private static ExternalSourceResolution.ResolvedSource externalSource(String path, DataType salaryType) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", INTEGER),
            referenceAttribute("name", KEYWORD),
            referenceAttribute("salary", salaryType)
        );
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
        return new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of());
    }

    /**
     * Asserts that {@code attr} is an {@link UnsupportedAttribute} named {@code name} whose union-typed
     * {@link UnsupportedAttribute#originalTypes() original types} equal {@code originalTypes} (in leg order).
     */
    private static void assertUnsupportedAttribute(NamedExpression attr, String name, List<String> originalTypes) {
        UnsupportedAttribute ua = as(attr, UnsupportedAttribute.class);
        assertEquals(name, ua.name());
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(originalTypes));
    }

    /**
     * A {@link UnionAll} branch over a no-field index: the empty-projection wrapper {@code Project([]) -> EsRelation}
     * (a leading bare index) or {@code Project([]) -> Subquery -> EsRelation} (an explicit {@code (FROM ...)} subquery).
     */
    private record UnionBranch(String index, boolean subquery) {}

    private static UnionBranch directBranch(String index) {
        return new UnionBranch(index, false);
    }

    private static UnionBranch subqueryBranch(String index) {
        return new UnionBranch(index, true);
    }

    /**
     * Asserts the analyzed {@code query} is {@code Limit -> UnionAll} whose branches match {@code branches} (in order),
     * each an empty-projection wrapper over a no-field relation as described by {@link UnionBranch}.
     */
    private static void assertNoFieldUnionAll(TestAnalyzer analyzer, String query, UnionBranch... branches) {
        Limit limit = as(analyzer.query(query), Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(0, unionAll.output().size());
        assertEquals(branches.length, unionAll.children().size());
        for (int i = 0; i < branches.length; i++) {
            Project project = as(unionAll.children().get(i), Project.class);
            assertTrue(project.projections().isEmpty());
            LogicalPlan relationChild = branches[i].subquery() ? as(project.child(), Subquery.class).child() : project.child();
            EsRelation relation = as(relationChild, EsRelation.class);
            assertEquals(branches[i].index(), relation.indexPattern());
        }
    }

    /**
     * Asserts that {@code STATS <count>} over subqueries of the given no-field {@code indices} analyzes to
     * {@code Limit -> Aggregate -> UnionAll}, with one empty {@code Project([]) -> Subquery -> EsRelation} branch per
     * index. Runs the check for {@code count()}, {@code count(*)} and {@code count(1)}.
     */
    private static void assertCountOverNoFieldSubqueries(TestAnalyzer analyzer, String... indices) {
        List<String> subqueries = new ArrayList<>();
        for (String index : indices) {
            subqueries.add("(FROM " + index + ")");
        }
        String from = String.join(", ", subqueries);
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            LogicalPlan plan = analyzer.query(LoggerMessageFormat.format(null, "FROM {} | STATS {}", from, count));
            Limit limit = as(plan, Limit.class);
            Aggregate aggregate = as(limit.child(), Aggregate.class);
            UnionAll unionAll = as(aggregate.child(), UnionAll.class);
            assertEquals(0, unionAll.output().size());
            assertEquals(indices.length, unionAll.children().size());
            for (int i = 0; i < indices.length; i++) {
                Project project = as(unionAll.children().get(i), Project.class);
                assertEquals(0, project.projections().size());
                Subquery subquery = as(project.child(), Subquery.class);
                EsRelation relation = as(subquery.child(), EsRelation.class);
                assertEquals(indices[i], relation.indexPattern());
            }
        }
    }

    private static void assertProjectionHasLong(Project project, String name, Class<? extends NamedExpression> kind) {
        NamedExpression match = project.projections().stream().filter(p -> name.equals(p.name())).findFirst().orElseThrow();
        NamedExpression typed = as(match, kind);
        assertEquals(LONG, typed.dataType());
    }

    private static void assertProjectionHasSyntheticTimestampLong(Project project) {
        assertTrue(
            "expected synthetic $$@timestamp$converted_to$long attribute in projections",
            project.projections().stream().anyMatch(p -> isSyntheticTimestampLong(p) && LONG.equals(p.dataType()))
        );
    }

    private static boolean isSyntheticTimestampLong(NamedExpression e) {
        // The push-down name is built by Attribute#rawTemporaryName which uses $$ delimiters and
        // a stable suffix encoding the target type (see ResolveUnionTypesInUnionAll).
        return e.name().contains("@timestamp") && e.name().contains("converted_to") && e.name().endsWith("long");
    }

    /**
     * The {@code sample_data_ts_long} index: the shared {@code sample_data} mapping with {@code @timestamp} overridden to
     * {@code long}. This mirrors how {@link org.elasticsearch.xpack.esql.CsvTestsDataLoader} builds the real index
     * ({@code new TestDataset("sample_data").withIndex("sample_data_ts_long").withTypeMapping(Map.of("@timestamp", "long"))}),
     * so we reuse {@code mapping-sample_data.json} (as {@link TestAnalyzer#addSampleData()} does) instead of re-declaring the fields.
     */
    private static EsIndex sampleDataTsLongIndex() {
        Map<String, EsField> mapping = new LinkedHashMap<>(loadMapping("mapping-sample_data.json"));
        mapping.put("@timestamp", new EsField("@timestamp", LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        return new EsIndex("sample_data_ts_long", mapping, Map.of("sample_data_ts_long", IndexMode.STANDARD), Map.of(), Map.of());
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
