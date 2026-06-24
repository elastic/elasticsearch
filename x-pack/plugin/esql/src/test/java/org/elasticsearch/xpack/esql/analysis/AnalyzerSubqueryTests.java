/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for subquery analysis in {@code FROM} (and the related {@code ViewUnionAll}/{@code UnionAll} planning).
 * All subquery in {@code FROM} command related analyzer tests belong here.
 */
public class AnalyzerSubqueryTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    @Before
    public void requireSubqueryInFromCommand() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{r}#675,ASC,LAST], Order[language_code{r}#685,ASC,LAST]]]
     *   \_Filter[emp_no{r}#675 > 10000[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#674, emp_no{r}#675, first_name{r}#676, gender{r}#677, hire_date{r}#678, job{r}#679, job.raw{r}#680,
     *                 languages{r}#681, last_name{r}#682, long_noidx{r}#683, salary{r}#684, language_code{r}#685, language_name{r}#686]]
     *       |_Project[[_meta_field{f}#654, emp_no{f}#648, first_name{f}#649, gender{f}#650, hire_date{f}#655, job{f}#656, job.raw{f}#657,
     *                  languages{f}#651, last_name{f}#652, long_noidx{f}#658, salary{f}#653, language_code{r}#661, language_name{r}#662]]
     *       | \_Eval[[null[INTEGER] AS language_code#661, null[KEYWORD] AS language_name#662]]
     *       |   \_EsRelation[test][_meta_field{f}#654, emp_no{f}#648, first_name{f}#64..]
     *       \_Project[[_meta_field{r}#663, emp_no{r}#664, first_name{r}#665, gender{r}#666, hire_date{r}#667, job{r}#668, job.raw{r}#669,
     *                  languages{r}#670, last_name{r}#671, long_noidx{r}#672, salary{r}#673, language_code{f}#659, language_name{f}#660]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#663, null[INTEGER] AS emp_no#664, null[KEYWORD] AS first_name#665,
     *                 null[TEXT] AS gender#666, null[DATETIME] AS hire_date#667, null[TEXT] AS job#668, null[KEYWORD] AS job.raw#669,
     *                 null[INTEGER] AS languages#670, null[KEYWORD] AS last_name#671, null[LONG] AS long_noidx#672,
     *                 null[INTEGER] AS salary#673
     * ]]
     *           \_Subquery[]
     *             \_Filter[language_code{f}#659 > 1[INTEGER]]
     *               \_EsRelation[languages][language_code{f}#659, language_name{f}#660]
     */
    public void testSubqueryInFrom() {
        LogicalPlan plan = basic().addLanguages().query("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(2, order.size());
        ReferenceAttribute empNo = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        ReferenceAttribute languageCode = as(order.get(1).child(), ReferenceAttribute.class);
        assertEquals("language_code", languageCode.name());
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        List<? extends NamedExpression> projections = subqueryProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from languages index
        assertEquals(2, aliases.size());
        assertEquals("language_code", aliases.get(0).name());
        Literal nullLiteral = as(aliases.get(0).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(INTEGER, nullLiteral.dataType());
        assertEquals("language_name", aliases.get(1).name());
        nullLiteral = as(aliases.get(1).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(KEYWORD, nullLiteral.dataType());
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        projections = subqueryProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from test index
        assertEquals(11, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{r}#632,ASC,LAST], Order[language_code{r}#642,ASC,LAST]]]
     *   \_Filter[emp_no{r}#632 > 10000[INTEGER]]
     *     \_ViewUnionAll[[null, view]]
     *       |_Project[[_meta_field{f}#611, emp_no{f}#605, first_name{f}#606, gender{f}#607, hire_date{f}#612, job{f}#613, job.raw{f}#614,
     *                  languages{f}#608, last_name{f}#609, long_noidx{f}#615, salary{f}#610, language_code{r}#618, language_name{r}#619]]
     *       | \_Eval[[null[INTEGER] AS language_code#618, null[KEYWORD] AS language_name#619]]
     *       |   \_EsRelation[test][_meta_field{f}#611, emp_no{f}#605, first_name{f}#60..]
     *       \_Project[[_meta_field{r}#620, emp_no{r}#621, first_name{r}#622, gender{r}#623, hire_date{r}#624, job{r}#625, job.raw{r}#626,
     *                  languages{r}#627, last_name{r}#628, long_noidx{r}#629, salary{r}#630, language_code{f}#616, language_name{f}#617]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#620, null[INTEGER] AS emp_no#621, null[KEYWORD] AS first_name#622,
     *                 null[TEXT] AS gender#623, null[DATETIME] AS hire_date#624, null[TEXT] AS job#625, null[KEYWORD] AS job.raw#626,
     *                 null[INTEGER] AS languages#627, null[KEYWORD] AS last_name#628, null[LONG] AS long_noidx#629,
     *                 null[INTEGER] AS salary#630]]
     *           \_Filter[language_code{f}#616 > 1[INTEGER]]
     *             \_EsRelation[languages][language_code{f}#616, language_name{f}#617]
     */
    public void testViewInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        LogicalPlan plan = basic().addLanguages().addView("view", "FROM languages | WHERE language_code > 1").query("""
            FROM test, view
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(2, order.size());
        ReferenceAttribute empNo = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        ReferenceAttribute languageCode = as(order.get(1).child(), ReferenceAttribute.class);
        assertEquals("language_code", languageCode.name());
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        ViewUnionAll viewUnionAll = as(filter.child(), ViewUnionAll.class);
        assertEquals(2, viewUnionAll.children().size());

        Project viewProject = as(viewUnionAll.children().get(0), Project.class);
        List<? extends NamedExpression> projections = viewProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        Eval viewEval = as(viewProject.child(), Eval.class);
        List<Alias> aliases = viewEval.fields(); // nullEvals from languages index
        assertEquals(2, aliases.size());
        assertEquals("language_code", aliases.get(0).name());
        Literal nullLiteral = as(aliases.get(0).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(INTEGER, nullLiteral.dataType());
        assertEquals("language_name", aliases.get(1).name());
        nullLiteral = as(aliases.get(1).child(), Literal.class);
        assertNull(nullLiteral.value());
        assertEquals(KEYWORD, nullLiteral.dataType());
        EsRelation subqueryIndex = as(viewEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        viewProject = as(viewUnionAll.children().get(1), Project.class);
        projections = viewProject.projections();
        assertEquals(13, projections.size()); // all fields from the two indices
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from test index
        assertEquals(11, aliases.size());
        Filter subqueryFilter = as(viewEval.child(), Filter.class);
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[ISNOTNULL(language_name{f}#2473)]
     *   \_Filter[language_code{f}#2472 > 1[INTEGER]]
     *     \_EsRelation[languages][language_code{f}#2472, language_name{f}#2473]
     */
    public void testSubqueryInFromWithoutMainIndexPattern() {
        LogicalPlan plan = basic().addLanguages().query("""
            FROM (FROM languages | WHERE language_code > 1)
            | WHERE language_name is not null
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        FieldAttribute language_name = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[ISNOTNULL(language_name{f}#2146)]
     *   \_Filter[language_code{f}#2145 > 1[INTEGER]]
     *     \_EsRelation[languages][language_code{f}#2145, language_name{f}#2146]
     */
    public void testViewInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        LogicalPlan plan = basic().addLanguages().addView("view", "FROM languages | WHERE language_code > 1").query("""
            FROM view
            | WHERE language_name is not null
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        FieldAttribute language_name = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[languageCode{r}#1877,languageCode{r}#1958]
     *   \_Project[[count(*){r}#1871, emp_no{r}#1944 AS empNo#1874, language_code{r}#1954 AS languageCode#1877]]
     *     \_Aggregate[[emp_no{r}#1944, language_code{r}#1954],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#1871,
     *                  emp_no{r}#1944, language_code{r}#1954]]
     *       \_Filter[emp_no{r}#1944 > 10000[INTEGER]]
     *         \_UnionAll[[_meta_field{r}#1943, emp_no{r}#1944, first_name{r}#1945, gender{r}#1946, hire_date{r}#1947, job{r}#1948,
     *                     job.raw{r}#1949, languages{r}#1950, last_name{r}#1951, long_noidx{r}#1952, salary{r}#1953,
     *                     language_code{r}#1954, languageName{r}#1955, max(@timestamp){r}#1956, language_name{r}#1957]]
     *           |_Project[[_meta_field{f}#1886, emp_no{f}#1880, first_name{f}#1881, gender{f}#1882, hire_date{f}#1887, job{f}#1888,
     *                      job.raw{f}#1889, languages{f}#1883, last_name{f}#1884, long_noidx{f}#1890, salary{f}#1885,
     *                      language_code{r}#1910, languageName{r}#1911, max(@timestamp){r}#1912, language_name{r}#1913]]
     *           | \_Eval[[null[INTEGER] AS language_code#1910, null[KEYWORD] AS languageName#1911,
     *                     null[DATETIME] AS max(@timestamp)#1912, null[KEYWORD] AS language_name#1913]]
     *           |   \_EsRelation[test][_meta_field{f}#1886, emp_no{f}#1880, first_name{f}#..]
     *           |_Project[[_meta_field{r}#1914, emp_no{r}#1915, first_name{r}#1916, gender{r}#1917, hire_date{r}#1918, job{r}#1919,
     *                      job.raw{r}#1920, languages{r}#1921, last_name{r}#1922, long_noidx{r}#1923, salary{r}#1924,
     *                      language_code{f}#1891, languageName{r}#1861, max(@timestamp){r}#1925, language_name{r}#1926]]
     *           | \_Eval[[null[KEYWORD] AS _meta_field#1914, null[INTEGER] AS emp_no#1915, null[KEYWORD] AS first_name#1916,
     *                     null[TEXT] AS gender#1917, null[DATETIME] AS hire_date#1918, null[TEXT] AS job#1919,
     *                     null[KEYWORD] AS job.raw#1920, null[INTEGER] AS languages#1921, null[KEYWORD] AS last_name#1922,
     *                     null[LONG] AS long_noidx#1923, null[INTEGER] AS salary#1924, null[DATETIME] AS max(@timestamp)#1925,
     *                     null[KEYWORD] AS language_name#1926]]
     *           |   \_Subquery[]
     *           |     \_Project[[language_code{f}#1891, language_name{f}#1892 AS languageName#1861]]
     *           |       \_Filter[language_code{f}#1891 > 10[INTEGER]]
     *           |         \_EsRelation[languages][language_code{f}#1891, language_name{f}#1892]
     *           |_Project[[_meta_field{r}#1927, emp_no{r}#1928, first_name{r}#1929, gender{r}#1930, hire_date{r}#1931, job{r}#1932,
     *                      job.raw{r}#1933, languages{r}#1934, last_name{r}#1935, long_noidx{r}#1936, salary{r}#1937,
     *                      language_code{r}#1938, languageName{r}#1939, max(@timestamp){r}#1863, language_name{r}#1940]]
     *           | \_Eval[[null[KEYWORD] AS _meta_field#1927, null[INTEGER] AS emp_no#1928, null[KEYWORD] AS first_name#1929,
     *                     null[TEXT] AS gender#1930, null[DATETIME] AS hire_date#1931, null[TEXT] AS job#1932,
     *                     null[KEYWORD] AS job.raw#1933, null[INTEGER] AS languages#1934, null[KEYWORD] AS last_name#1935,
     *                     null[LONG] AS long_noidx#1936, null[INTEGER] AS salary#1937, null[INTEGER] AS language_code#1938,
     *                     null[KEYWORD] AS languageName#1939, null[KEYWORD] AS language_name#1940]]
     *           |   \_Subquery[]
     *           |     \_Aggregate[[],[MAX(@timestamp{f}#1893,true[BOOLEAN],PT0S[TIME_DURATION]) AS max(@timestamp)#1863]]
     *           |       \_EsRelation[sample_data][@timestamp{f}#1893, client_ip{f}#1894, event_durati..]
     *           \_Project[[_meta_field{f}#1903, emp_no{f}#1897, first_name{f}#1898, gender{f}#1899, hire_date{f}#1904, job{f}#1905,
     *                      job.raw{f}#1906, languages{f}#1900, last_name{f}#1901, long_noidx{f}#1907, salary{f}#1902,
     *                      language_code{r}#1867, languageName{r}#1941, max(@timestamp){r}#1942, language_name{f}#1909]]
     *             \_Eval[[null[KEYWORD] AS languageName#1941, null[DATETIME] AS max(@timestamp)#1942]]
     *               \_Subquery[]
     *                 \_LookupJoin[LEFT,[language_code{r}#1867],[language_code{f}#1908],false,null]
     *                   |_Eval[[languages{f}#1900 AS language_code#1867]]
     *                   | \_EsRelation[test][_meta_field{f}#1903, emp_no{f}#1897, first_name{f}#..]
     *                   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1908, language_name{f}#1909]
     */
    public void testMultipleSubqueriesInFrom() {
        LogicalPlan plan = basic().addLanguages().addSampleData().addLanguagesLookup().query("""
            FROM test
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            , (FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        Project rename = as(mvExpand.child(), Project.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(4, aliases.size());
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        rename = as(subquery.child(), Project.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(3), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        LookupJoin lookupJoin = as(subquery.child(), LookupJoin.class);
        subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        subqueryEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[languageCode{r}#1133,languageCode{r}#1224]
     *   \_Project[[count(*){r}#1127, emp_no{r}#1210 AS empNo#1130, language_code{r}#1220 AS languageCode#1133]]
     *     \_Aggregate[[emp_no{r}#1210, language_code{r}#1220],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#1127,
     *                  emp_no{r}#1210, language_code{r}#1220]]
     *       \_Filter[emp_no{r}#1210 > 10000[INTEGER]]
     *         \_ViewUnionAll[[null, view1, view2, view3]]
     *           |_Project[[_meta_field{f}#1152, emp_no{f}#1146, first_name{f}#1147, gender{f}#1148, hire_date{f}#1153, job{f}#1154,
     *                      job.raw{f}#1155, languages{f}#1149, last_name{f}#1150, long_noidx{f}#1156, salary{f}#1151,
     *                      language_code{r}#1176, languageName{r}#1177, max(@timestamp){r}#1178, language_name{r}#1179]]
     *           | \_Eval[[null[INTEGER] AS language_code#1176, null[KEYWORD] AS languageName#1177,
     *                     null[DATETIME] AS max(@timestamp)#1178, null[KEYWORD] AS language_name#1179]]
     *           |   \_EsRelation[test][_meta_field{f}#1152, emp_no{f}#1146, first_name{f}#..]
     *           |_Project[[_meta_field{r}#1180, emp_no{r}#1181, first_name{r}#1182, gender{r}#1183, hire_date{r}#1184, job{r}#1185,
     *                      job.raw{r}#1186, languages{r}#1187, last_name{r}#1188, long_noidx{r}#1189, salary{r}#1190,
     *                      language_code{f}#1157, languageName{r}#1139, max(@timestamp){r}#1191, language_name{r}#1192]]
     *           | \_Eval[[null[KEYWORD] AS _meta_field#1180, null[INTEGER] AS emp_no#1181, null[KEYWORD] AS first_name#1182,
     *                     null[TEXT] AS gender#1183, null[DATETIME] AS hire_date#1184, null[TEXT] AS job#1185,
     *                     null[KEYWORD] AS job.raw#1186, null [INTEGER] AS languages#1187, null[KEYWORD] AS last_name#1188,
     *                     null[LONG] AS long_noidx#1189, null[INTEGER] AS salary#1190, null[DATETIME] AS max(@timestamp)#1191,
     *                     null[KEYWORD] AS language_name#1192]]
     *           |   \_Project[[language_code{f}#1157, language_name{f}#1158 AS languageName#1139]]
     *           |     \_Filter[language_code{f}#1157 > 10[INTEGER]]
     *           |       \_EsRelation[languages][language_code{f}#1157, language_name{f}#1158]
     *           |_Project[[_meta_field{r}#1193, emp_no{r}#1194, first_name{r}#1195, gender{r}#1196, hire_date{r}#1197, job{r}#1198,
     *                      job.raw{r}#1199, languages{r}#1200, last_name{r}#1201, long_noidx{r}#1202, salary{r}#1203,
     *                      language_code{r}#1204, languageName{r}#1205, max(@timestamp){r}#1141, language_name{r}#1206]]
     *           | \_Eval[[null[KEYWORD] AS _meta_field#1193, null[INTEGER] AS emp_no#1194, null[KEYWORD] AS first_name#1195,
     *                     null[TEXT] AS gender#1196, null[DATETIME] AS hire_date#1197, null[TEXT] AS job#1198,
     *                     null[KEYWORD] AS job.raw#1199, null [INTEGER] AS languages#1200, null[KEYWORD] AS last_name#1201,
     *                     null[LONG] AS long_noidx#1202, null[INTEGER] AS salary#1203, null[INTEGER] AS language_code#1204,
     *                     null[KEYWORD] AS languageName#1205, null[KEYWORD] AS language_name#1206]]
     *           |   \_Aggregate[[],[MAX(@timestamp{f}#1159,true[BOOLEAN],PT0S[TIME_DURATION]) AS max(@timestamp)#1141]]
     *           |     \_EsRelation[sample_data][@timestamp{f}#1159, client_ip{f}#1160, event_durati..]
     *           \_Project[[_meta_field{f}#1169, emp_no{f}#1163, first_name{f}#1164, gender{f}#1165, hire_date{f}#1170, job{f}#1171,
     *                      job.raw{f}#1172, languages{f}#1166, last_name{f}#1167, long_noidx{f}#1173, salary{f}#1168,
     *                      language_code{r}#1144, languageName{r}#1207, max(@timestamp){r}#1208, language_name{f}#1175]]
     *             \_Eval[[null[KEYWORD] AS languageName#1207, null[DATETIME] AS max(@timestamp)#1208]]
     *               \_LookupJoin[LEFT,[language_code{r}#1144],[language_code{f}#1174],false,null]
     *                 |_Eval[[languages{f}#1166 AS language_code#1144]]
     *                 | \_EsRelation[test][_meta_field{f}#1169, emp_no{f}#1163, first_name{f}#..]
     *                 \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1174, language_name{f}#1175]
     */
    public void testMultipleViewsInFrom() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        LogicalPlan plan = basic().addLanguages()
            .addSampleData()
            .addLanguagesLookup()
            .addView("view1", "FROM languages | WHERE language_code > 10 | RENAME language_name as languageName")
            .addView("view2", "FROM sample_data | STATS max(@timestamp)")
            .addView("view3", "FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code")
            .query("""
                FROM test, view1, view2, view3
                | WHERE emp_no > 10000
                | STATS count(*) by emp_no, language_code
                | RENAME emp_no AS empNo, language_code AS languageCode
                | MV_EXPAND languageCode
                """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        Project rename = as(mvExpand.child(), Project.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        ViewUnionAll viewUninAll = as(filter.child(), ViewUnionAll.class);
        assertEquals(4, viewUninAll.children().size());

        Project viewProject = as(viewUninAll.children().get(0), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval viewEval = as(viewProject.child(), Eval.class);
        List<Alias> aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(4, aliases.size());
        EsRelation subqueryIndex = as(viewEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        viewProject = as(viewUninAll.children().get(1), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        rename = as(viewEval.child(), Project.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        viewProject = as(viewUninAll.children().get(2), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        Aggregate subqueryAggregate = as(viewEval.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        viewProject = as(viewUninAll.children().get(3), Project.class);
        projections = viewProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        viewEval = as(viewProject.child(), Eval.class);
        aliases = viewEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        LookupJoin lookupJoin = as(viewEval.child(), LookupJoin.class);
        subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        viewEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(viewEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_MvExpand[languageCode{r}#801,languageCode{r}#867]
     *   \_Project[[count(*){r}#795, emp_no{r}#853 AS empNo#798, language_code{r}#863 AS languageCode#801]]
     *     \_Aggregate[[emp_no{r}#853, language_code{r}#863],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#795,
     *                  emp_no{r}#853, language_code{r}#863]]
     *       \_Filter[emp_no{r}#853 > 10000[INTEGER]]
     *         \_UnionAll[[_meta_field{r}#852, emp_no{r}#853, first_name{r}#854, gender{r}#855, hire_date{r}#856, job{r}#857,
     *                     job.raw{r}#858, languages{r}#859, last_name{r}#860, long_noidx{r}#861, salary{r}#862, language_code{r}#863,
     *                     language_name{r}#864, languageName{r}#865, max(@timestamp){r}#866]]
     *           |_Project[[_meta_field{f}#810, emp_no{f}#804, first_name{f}#805, gender{f}#806, hire_date{f}#811, job{f}#812,
     *                      job.raw{f}#813, languages{f}#807, last_name{f}#808, long_noidx{f}#814, salary{f}#809, language_code{r}#785,
     *                      language_name{f}#816, languageName{r}#823, max(@timestamp){r}#824]]
     *           | \_Eval[[null[KEYWORD] AS languageName#823, null[DATETIME] AS max(@timestamp)#824]]
     *           |   \_Subquery[]
     *           |     \_LookupJoin[LEFT,[language_code{r}#785],[language_code{f}#815],false,null]
     *           |       |_Eval[[languages{f}#807 AS language_code#785]]
     *           |       | \_EsRelation[test][_meta_field{f}#810, emp_no{f}#804, first_name{f}#80..]
     *           |       \_EsRelation[languages_lookup][LOOKUP][language_code{f}#815, language_name{f}#816]
     *           |_Project[[_meta_field{r}#825, emp_no{r}#826, first_name{r}#827, gender{r}#828, hire_date{r}#829, job{r}#830,
     *                      job.raw{r}#831, languages{r}#832, last_name{r}#833, long_noidx{r}#834, salary{r}#835, language_code{f}#817,
     *                      language_name{r}#836, languageName{r}#789, max(@timestamp){r}#837]]
     *           | \_Eval[[null[KEYWORD] AS _meta_field#825, null[INTEGER] AS emp_no#826, null[KEYWORD] AS first_name#827,
     *                     null[TEXT] AS gender#828, null[DATETIME] AS hire_date#829, null[TEXT] AS job#830, null[KEYWORD] AS job.raw#831,
     *                     null[INTEGER] AS languages#832, null[KEYWORD] AS last_name#833, null[LONG] AS long_noidx#834,
     *                     null[INTEGER] AS salary#835, null[KEYWORD] AS language_name#836, null[DATETIME] AS max(@timestamp)#837]]
     *           |   \_Subquery[]
     *           |     \_Project[[language_code{f}#817, language_name{f}#818 AS languageName#789]]
     *           |       \_Filter[language_code{f}#817 > 10[INTEGER]]
     *           |         \_EsRelation[languages][language_code{f}#817, language_name{f}#818]
     *           \_Project[[_meta_field{r}#838, emp_no{r}#839, first_name{r}#840, gender{r}#841, hire_date{r}#842, job{r}#843,
     *                      job.raw{r}#844, languages{r}#845, last_name{r}#846, long_noidx{r}#847, salary{r}#848, language_code{r}#849,
     *                      language_name{r}#850, languageName{r}#851, max(@timestamp){r}#791]]
     *             \_Eval[[null[KEYWORD] AS _meta_field#838, null[INTEGER] AS emp_no#839, null[KEYWORD] AS first_name#840,
     *                     null[TEXT] AS gender#841, null[DATETIME] AS hire_date#842, null[TEXT] AS job#843, null[KEYWORD] AS job.raw#844,
     *                     null[INTEGER] AS languages#845, null[KEYWORD] AS last_name#846, null[LONG] AS long_noidx#847,
     *                     null[INTEGER] AS salary#848, null[INTEGER] AS language_code#849, null[KEYWORD] AS language_name#850,
     *                     null[KEYWORD] AS languageName#851]]
     *               \_Subquery[]
     *                 \_Aggregate[[],[MAX(@timestamp{f}#819,true[BOOLEAN],PT0S[TIME_DURATION]) AS max(@timestamp)#791]]
     *                   \_EsRelation[sample_data][@timestamp{f}#819, client_ip{f}#820, event_duration..]
     */
    public void testMultipleSubqueryInFromWithoutMainIndexPattern() {
        LogicalPlan plan = basic().addLanguages().addSampleData().addLanguagesLookup().query("""
            FROM (FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """);

        Limit limit = as(plan, Limit.class);
        MvExpand mvExpand = as(limit.child(), MvExpand.class);
        NamedExpression mvExpandTarget = as(mvExpand.target(), NamedExpression.class);
        assertEquals("languageCode", mvExpandTarget.name());
        ReferenceAttribute mvExpandExpanded = as(mvExpand.expanded(), ReferenceAttribute.class);
        assertEquals("languageCode", mvExpandExpanded.name());
        Project rename = as(mvExpand.child(), Project.class);
        List<? extends NamedExpression> projections = rename.projections();
        assertEquals(3, projections.size());
        Alias a = as(projections.get(1), Alias.class);
        assertEquals("empNo", a.name());
        ReferenceAttribute ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        a = as(projections.get(2), Alias.class);
        assertEquals("languageCode", a.name());
        ra = as(a.child(), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Aggregate aggregate = as(rename.child(), Aggregate.class);
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(3, aggregates.size());
        a = as(aggregates.get(0), Alias.class);
        assertEquals("count(*)", a.name());
        List<Expression> groupings = aggregate.groupings();
        assertEquals(2, groupings.size());
        ra = as(groupings.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", ra.name());
        ra = as(groupings.get(1), ReferenceAttribute.class);
        assertEquals("language_code", ra.name());
        Filter filter = as(aggregate.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        List<Alias> aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(2, aliases.size());
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        LookupJoin lookupJoin = as(subquery.child(), LookupJoin.class);
        EsRelation subqueryIndex = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", subqueryIndex.indexPattern());
        subqueryEval = as(lookupJoin.left(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(13, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        rename = as(subquery.child(), Project.class);
        List<? extends NamedExpression> renameProjections = rename.projections();
        assertEquals(2, renameProjections.size());
        FieldAttribute language_code = as(renameProjections.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        a = as(renameProjections.get(1), Alias.class);
        assertEquals("languageName", a.name());
        FieldAttribute language_name = as(a.child(), FieldAttribute.class);
        assertEquals("language_name", language_name.name());
        Filter subqueryFilter = as(rename.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(10, literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        projections = subqueryProject.projections();
        assertEquals(15, projections.size()); // all fields from the other legs
        subqueryEval = as(subqueryProject.child(), Eval.class);
        aliases = subqueryEval.fields(); // nullEvals from the other legs
        assertEquals(14, aliases.size());
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{r}#2190,ASC,LAST], Order[language_code{r}#2200,ASC,LAST]]]
     *   \_Filter[emp_no{r}#2190 > 10000[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#2189, emp_no{r}#2190, first_name{r}#2191, gender{r}#2192, hire_date{r}#2193, job{r}#2194,
     *                 job.raw{r}#2195, languages{r}#2196, last_name{r}#2197, long_noidx{r}#2198, salary{r}#2199, language_code{r}#2200,
     *                 language_name{r}#2201, count(*){r}#2202]]
     *       |_Project[[_meta_field{f}#2158, emp_no{f}#2152, first_name{f}#2153, gender{f}#2154, hire_date{f}#2159, job{f}#2160,
     *                  job.raw{f}#2161, languages{f}#2155, last_name{f}#2156, long_noidx{f}#2162, salary{f}#2157, language_code{r}#2175,
     *                  language_name{r}#2176, count(*){r}#2177]]
     *       | \_Eval[[null[INTEGER] AS language_code#2175, null[KEYWORD] AS language_name#2176, null[LONG] AS count(*)#2177]]
     *       |   \_EsRelation[test][_meta_field{f}#2158, emp_no{f}#2152, first_name{f}#..]
     *       \_Project[[_meta_field{r}#2178, emp_no{r}#2179, first_name{r}#2180, gender{r}#2181, hire_date{r}#2182, job{r}#2183,
     *                  job.raw{r}#2184, languages{r}#2185, last_name{r}#2186, long_noidx{r}#2187, salary{r}#2188, language_code{r}#2172,
     *                  language_name{r}#2173, count(*){r}#2174]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#2178, null[INTEGER] AS emp_no#2179, null[KEYWORD] AS first_name#2180,
     *                 null[TEXT] AS gender#2181, null[DATETIME] AS hire_date#2182, null[TEXT] AS job#2183, null[KEYWORD] AS job.raw#2184,
     *                 null[INTEGER] AS languages#2185, null[KEYWORD] AS last_name#2186, null[LONG] AS long_noidx#2187,
     *                 null[INTEGER] AS salary#2188]]
     *           \_Subquery[]
     *             \_Filter[language_code{r}#2172 > 10[INTEGER]]
     *               \_UnionAll[[language_code{r}#2172, language_name{r}#2173, count(*){r}#2174]]
     *                 |_Project[[language_code{f}#2163, language_name{f}#2164, count(*){r}#2169]]
     *                 | \_Eval[[null[LONG] AS count(*)#2169]]
     *                 |   \_EsRelation[languages][language_code{f}#2163, language_name{f}#2164]
     *                 \_Project[[language_code{r}#2170, language_name{r}#2171, count(*){r}#2147]]
     *                   \_Eval[[null[INTEGER] AS language_code#2170, null[KEYWORD] AS language_name#2171]]
     *                     \_Subquery[]
     *                       \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#2147]]
     *                         \_EsRelation[sample_data][@timestamp{f}#2165, client_ip{f}#2166, event_durati..]
     */
    public void testNestedSubqueryInFrom() {
        LogicalPlan plan = basic().addLanguages().addSampleData().query("""
            FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        unionAll = as(subqueryFilter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        subqueryProject = as(unionAll.children().get(0), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{r}#2415,ASC,LAST], Order[language_code{r}#2426,ASC,LAST]]]
     *   \_Filter[emp_no{r}#2415 > 10000[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#2414, emp_no{r}#2415, first_name{r}#2416, gender{r}#2417, hire_date{r}#2418, job{r}#2419,
     *                 job.raw{r}#2420, languages{r}#2421, last_name{r}#2422, long_noidx{r}#2423, salary{r}#2424, _index{r}#2425,
     *                 language_code{r}#2426, language_name{r}#2427, count(*){r}#2428]]
     *       |_Project[[_meta_field{f}#2382, emp_no{f}#2376, first_name{f}#2377, gender{f}#2378, hire_date{f}#2383, job{f}#2384,
     *                  job.raw{f}#2385, languages{f}#2379, last_name{f}#2380, long_noidx{f}#2386, salary{f}#2381, _index{m}#2372,
     *                  language_code{r}#2399, language_name{r}#2400, count(*){r}#2401]]
     *       | \_Eval[[null[INTEGER] AS language_code#2399, null[KEYWORD] AS language_name#2400, null[LONG] AS count(*)#2401]]
     *       |   \_EsRelation[test][_meta_field{f}#2382, emp_no{f}#2376, first_name{f}#..]
     *       \_Project[[_meta_field{r}#2402, emp_no{r}#2403, first_name{r}#2404, gender{r}#2405, hire_date{r}#2406, job{r}#2407,
     *                  job.raw{r}#2408, languages{r}#2409, last_name{r}#2410, long_noidx{r}#2411, salary{r}#2412, _index{r}#2413,
     *                  language_code{r}#2396, language_name{r}#2397, count(*){r}#2398]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#2402, null[INTEGER] AS emp_no#2403, null[KEYWORD] AS first_name#2404,
     *                 null[TEXT] AS gender#2405, null[DATETIME] AS hire_date#2406, null[TEXT] AS job#2407, null[KEYWORD] AS job.raw#2408,
     *                 null[INTEGER] AS languages#2409, null[KEYWORD] AS last_name#2410, null[LONG] AS long_noidx#2411,
     *                 null[INTEGER] AS salary#2412, null[KEYWORD] AS _index#2413]]
     *           \_Subquery[]
     *             \_Filter[language_code{r}#2396 > 10[INTEGER]]
     *               \_UnionAll[[language_code{r}#2396, language_name{r}#2397, count(*){r}#2398]]
     *                 |_Project[[language_code{f}#2387, language_name{f}#2388, count(*){r}#2393]]
     *                 | \_Eval[[null[LONG] AS count(*)#2393]]
     *                 |   \_EsRelation[languages][language_code{f}#2387, language_name{f}#2388]
     *                 \_Project[[language_code{r}#2394, language_name{r}#2395, count(*){r}#2370]]
     *                   \_Eval[[null[INTEGER] AS language_code#2394, null[KEYWORD] AS language_name#2395]]
     *                     \_Subquery[]
     *                       \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#2370]]
     *                         \_EsRelation[sample_data][@timestamp{f}#2389, client_ip{f}#2390, event_durati..]
     */
    public void testNestedSubqueryInFromWithMetadata() {
        LogicalPlan plan = basic().addLanguages().addSampleData().query("""
            FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10) metadata _index
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());
        List<Attribute> output = subqueryIndex.output();
        assertEquals(12, output.size());
        MetadataAttribute metadataAttribute = as(output.get(11), MetadataAttribute.class);
        assertEquals("_index", metadataAttribute.name());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        unionAll = as(subqueryFilter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        subqueryProject = as(unionAll.children().get(0), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("languages", subqueryIndex.indexPattern());
        output = subqueryIndex.output();
        assertEquals(2, output.size());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
        output = subqueryIndex.output();
        assertEquals(4, output.size());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[emp_no{r}#1645,ASC,LAST], Order[languages{r}#1651,ASC,LAST]]]
     *   \_Filter[ISNOTNULL(languages{r}#1651)]
     *     \_Filter[emp_no{r}#1645 > 10[INTEGER]]
     *       \_UnionAll[[_meta_field{r}#1644, emp_no{r}#1645, first_name{r}#1646, gender{r}#1647, hire_date{r}#1648, job{r}#1649,
     *                   job.raw{r}#1650, languages{r}#1651, last_name{r}#1652, long_noidx{r}#1653, salary{r}#1654, count(*){r}#1655]]
     *         |_Project[[_meta_field{f}#1623, emp_no{f}#1617, first_name{f}#1618, gender{f}#1619, hire_date{f}#1624, job{f}#1625,
     *                    job.raw{f}#1626, languages{f}#1620, last_name{f}#1621, long_noidx{f}#1627, salary{f}#1622, count(*){r}#1632]]
     *         | \_Eval[[null[LONG] AS count(*)#1632]]
     *         |   \_EsRelation[test][_meta_field{f}#1623, emp_no{f}#1617, first_name{f}#..]
     *         \_Project[[_meta_field{r}#1633, emp_no{r}#1634, first_name{r}#1635, gender{r}#1636, hire_date{r}#1637, job{r}#1638,
     *                    job.raw{r}#1639, languages{r}#1640, last_name{r}#1641, long_noidx{r}#1642, salary{r}#1643, count(*){r}#1612]]
     *           \_Eval[[null[KEYWORD] AS _meta_field#1633, null[INTEGER] AS emp_no#1634, null[KEYWORD] AS first_name#1635,
     *                   null[TEXT] AS gender#1636, null[DATETIME] AS hire_date#1637, null[TEXT] AS job#1638,
     *                   null[KEYWORD] AS job.raw#1639, null[INTEGER] AS languages#1640, null[KEYWORD] AS last_name#1641,
     *                   null[LONG] AS long_noidx#1642, null[INTEGER] AS salary#1643]]
     *             \_Subquery[]
     *               \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#1612]]
     *                 \_EsRelation[sample_data][@timestamp{f}#1628, client_ip{f}#1629, event_durati..]
     */
    public void testNestedSubqueriesInFromWithoutMainIndexPattern() {
        LogicalPlan plan = basic().addSampleData().query("""
            FROM (FROM test, (FROM sample_data | STATS count(*)) | WHERE emp_no > 10)
            | WHERE languages is not null
            | SORT emp_no, languages
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> orderKeys = orderBy.order();
        assertEquals(2, orderKeys.size());
        ReferenceAttribute emp_no = as(orderKeys.get(0).child(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        ReferenceAttribute languages = as(orderKeys.get(1).child(), ReferenceAttribute.class);
        assertEquals("languages", languages.name());
        Filter filter = as(orderBy.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        languages = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("languages", languages.name());
        filter = as(filter.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval subqueryEval = as(subqueryProject.child(), Eval.class);
        EsRelation subqueryIndex = as(subqueryEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        subqueryEval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(subqueryEval.child(), Subquery.class);
        Aggregate subqueryAggregate = as(subquery.child(), Aggregate.class);
        subqueryIndex = as(subqueryAggregate.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
    }

    /*
     * Project[[!avg_worked_seconds, birth_date{r}#1450, !first_name, !gender, height{r}#1454, height.float{r}#1455,
     *          height.half_float{r}#1456, height.scaled_float{r}#1457, hire_date{r}#1458, !is_rehired, !job_positions, languages{r}#1461,
     *          languages.byte{r}#1462, languages.long{r}#1463, languages.short{r}#1464, !last_name, !salary, salary_change{r}#1467,
     *          salary_change.int{r}#1468, salary_change.keyword{r}#1469, salary_change.long{r}#1470, !still_hired, height.double{r}#1472,
     *          languages.int{r}#1473, emp_no{r}#1396]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[emp_no{r}#1396,ASC,LAST]]]
     *     \_Filter[emp_no{r}#1396 > 10000[INTEGER]]
     *       \_Eval[[$$emp_no$converted_to$long{r$}#1476 AS emp_no#1396]]
     *         \_UnionAll[[!avg_worked_seconds, birth_date{r}#1450, !emp_no, $$emp_no$converted_to$long{r$}#1476, !first_name, !gender,
     *                     height{r}#1454, height.float{r}#1455, height.half_float{r}#1456, height.scaled_float{r}#1457, hire_date{r}#1458,
     *                     !is_rehired, !job_positions, languages{r}#1461, languages.byte{r}#1462, languages.long{r}#1463,
     *                     languages.short{r}#1464, !last_name, !salary, salary_change{r}#1467, salary_change.int{r}#1468,
     *                     salary_change.keyword{r}#1469, salary_change.long{r}#1470, !still_hired, height.double{r}#1472,
     *                     languages.int{r}#1473]]
     *           |_Project[[avg_worked_seconds{r}#1477, birth_date{f}#1403, emp_no{r}#1478, $$emp_no$converted_to$long{r$}#1474,
     *                      first_name{r}#1479, gender{r}#1480, height{f}#1410, height.float{f}#1411, height.half_float{f}#1413,
     *                      height.scaled_float{f}#1412, hire_date{r}#1481, is_rehired{r}#1482, job_positions{r}#1483, languages{f}#1406,
     *                      languages.byte{f}#1409, languages.long{f}#1407, languages.short{f}#1408, last_name{r}#1484, salary{r}#1485,
     *                      salary_change{f}#1418, salary_change.int{f}#1419, salary_change.keyword{f}#1421, salary_change.long{f}#1420,
     *                      still_hired{r}#1486, height.double{r}#1442, languages.int{r}#1443]]
     *           | \_Eval[[null[KEYWORD] AS avg_worked_seconds#1477, null[KEYWORD] AS emp_no#1478, null[KEYWORD] AS first_name#1479,
     *                     null[KEYWORD] AS gender#1480, TODATENANOS(hire_date{f}#1404) AS hire_date#1481, null[KEYWORD] AS is_rehired#1482,
     *                     null[KEYWORD] AS job_positions#1483, null[KEYWORD] AS last_name#1484, null[KEYWORD] AS salary#1485,
     *                     null[KEYWORD] AS still_hired#1486]]
     *           |   \_Eval[[TOLONG(emp_no{f}#1399) AS $$emp_no$converted_to$long#1474]]
     *           |     \_Eval[[null[DOUBLE] AS height.double#1442, null[INTEGER] AS languages.int#1443]]
     *           |       \_EsRelation[test][avg_worked_seconds{f}#1415, birth_date{f}#1403, emp..]
     *           \_Project[[avg_worked_seconds{r}#1487, birth_date{f}#1426, emp_no{r}#1488, $$emp_no$converted_to$long{r$}#1475,
     *                      first_name{r}#1489, gender{r}#1490, height{f}#1433, height.float{r}#1444, height.half_float{f}#1436,
     *                      height.scaled_float{f}#1435, hire_date{f}#1427, is_rehired{r}#1491, job_positions{r}#1492, languages{f}#1429,
     *                      languages.byte{r}#1445, languages.long{f}#1430, languages.short{f}#1431, last_name{r}#1493, salary{r}#1494,
     *                      salary_change{f}#1441, salary_change.int{r}#1446, salary_change.keyword{r}#1447, salary_change.long{r}#1448,
     *                      still_hired{r}#1495, height.double{f}#1434, languages.int{f}#1432]]
     *             \_Eval[[null[KEYWORD] AS avg_worked_seconds#1487, null[KEYWORD] AS emp_no#1488, null[KEYWORD] AS first_name#1489,
     *                     null[KEYWORD] AS gender#1490, null[KEYWORD] AS is_rehired#1491, null[KEYWORD] AS job_positions#1492,
     *                     null[KEYWORD] AS last_name#1493, null[KEYWORD] AS salary#1494, null[KEYWORD] AS still_hired#1495]]
     *               \_Eval[[TOLONG(emp_no{f}#1422) AS $$emp_no$converted_to$long#1475]]
     *                 \_Eval[[null[DOUBLE] AS height.float#1444, null[INTEGER] AS languages.byte#1445,
     *                         null[INTEGER] AS salary_change.int#1446, null[KEYWORD] AS salary_change.keyword#1447,
     *                         null[LONG] AS salary_change.long#1448]]
     *                   \_Subquery[]
     *                     \_Filter[languages{f}#1429 > 0[INTEGER]]
     *                       \_EsRelation[test_mixed_types][avg_worked_seconds{f}#1438, birth_date{f}#1426, emp..]
     */
    public void testMixedDataTypesInSubquery() {
        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | SORT emp_no
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(25, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(1, aliases.size());
        Alias alias = aliases.get(0);
        assertEquals("emp_no", alias.name());
        ReferenceAttribute emp_no = as(alias.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(1, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(1, explicitCastingEval.fields().size());
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
     * Project[[!avg_worked_seconds, birth_date{r}#1560, !first_name, !gender, height{r}#1564, height.float{r}#1565,
     *          height.half_float{r}#1566, height.scaled_float{r}#1567, hire_date{r}#1568, !job_positions, languages{r}#1571,
     *          languages.byte{r}#1572, languages.long{r}#1573, languages.short{r}#1574, !last_name, !salary, salary_change{r}#1577,
     *          salary_change.int{r}#1578, salary_change.keyword{r}#1579, salary_change.long{r}#1580, height.double{r}#1582,
     *          languages.int{r}#1583, emp_no{r}#1499, still_hired{r}#1503, is_rehired{r}#1506]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[still_hired{r}#1503,ASC,LAST], Order[is_rehired{r}#1506,ASC,LAST]]]
     *     \_Eval[[$$still_hired$converted_to$keyword{r$}#1592 AS still_hired#1503,
     *             $$is_rehired$converted_to$keyword{r$}#1591 AS is_rehired#1506]]
     *       \_Filter[emp_no{r}#1499 > 10000[INTEGER]]
     *         \_Eval[[$$emp_no$converted_to$long{r$}#1590 AS emp_no#1499]]
     *           \_UnionAll[[!avg_worked_seconds, birth_date{r}#1560, !emp_no, $$emp_no$converted_to$long{r$}#1590, !first_name, !gender,
     *                       height{r}#1564, height.float{r}#1565, height.half_float{r}#1566, height.scaled_float{r}#1567,
     *                       hire_date{r}#1568, !is_rehired, $$is_rehired$converted_to$keyword{r$}#1591, !job_positions, languages{r}#1571,
     *                       languages.byte{r}#1572, languages.long{r}#1573, languages.short{r}#1574, !last_name, !salary,
     *                       salary_change{r}#1577, salary_change.int{r}#1578, salary_change.keyword{r}#1579, salary_change.long{r}#1580,
     *                       !still_hired, $$still_hired$converted_to$keyword{r$}#1592, height.double{r}#1582, languages.int{r}#1583]]
     *             |_Project[[avg_worked_seconds{r}#1593, birth_date{f}#1513, emp_no{r}#1594, $$emp_no$converted_to$long{r$}#1584,
     *                        first_name{r}#1595, gender{r}#1596, height{f}#1520, height.float{f}#1521, height.half_float{f}#1523,
     *                        height.scaled_float{f}#1522, hire_date{r}#1597, is_rehired{r}#1598,
     *                        $$is_rehired$converted_to$keyword{r$}#1585, job_positions{r}#1599, languages{f}#1516, languages.byte{f}#1519,
     *                        languages.long{f}#1517, languages.short{f}#1518, last_name{r}#1600, salary{r}#1601, salary_change{f}#1528,
     *                        salary_change.int{f}#1529, salary_change.keyword{f}#1531, salary_change.long{f}#1530, still_hired{r}#1602,
     *                        $$still_hired$converted_to$keyword{r$}#1586, height.double{r}#1552, languages.int{r}#1553]]
     *             | \_Eval[[null[KEYWORD] AS avg_worked_seconds#1593, null[KEYWORD] AS emp_no#1594, null[KEYWORD] AS first_name#1595,
     *                       null[KEYWORD] AS gender#1596, TODATENANOS(hire_date{f}#1514) AS hire_date#1597,
     *                       null[KEYWORD] AS is_rehired#1598, null[KEYWORD] AS job_positions#1599, null[KEYWORD] AS last_name#1600,
     *                       null[KEYWORD] AS salary#1601, null[KEYWORD] AS still_hired#1602]]
     *             |   \_Eval[[TOLONG(emp_no{f}#1509) AS $$emp_no$converted_to$long#1584,
     *                         TOSTRING(is_rehired{f}#1527) AS $$is_rehired$converted_to$keyword#1585,
     *                         TOSTRING(still_hired{f}#1524) AS $$still_hired$converted_to$keyword#1586]]
     *             |     \_Eval[[null[DOUBLE] AS height.double#1552, null[INTEGER] AS languages.int#1553]]
     *             |       \_EsRelation[test][avg_worked_seconds{f}#1525, birth_date{f}#1513, emp..]
     *             \_Project[[avg_worked_seconds{r}#1603, birth_date{f}#1536, emp_no{r}#1604, $$emp_no$converted_to$long{r$}#1587,
     *                        first_name{r}#1605, gender{r}#1606, height{f}#1543, height.float{r}#1554, height.half_float{f}#1546,
     *                        height.scaled_float{f}#1545, hire_date{f}#1537, is_rehired{r}#1607,
     *                        $$is_rehired$converted_to$keyword{r$}#1588, job_positions{r}#1608, languages{f}#1539, languages.byte{r}#1555,
     *                        languages.long{f}#1540, languages.short{f}#1541, last_name{r}#1609, salary{r}#1610, salary_change{f}#1551,
     *                        salary_change.int{r}#1556, salary_change.keyword{r}#1557, salary_change.long{r}#1558, still_hired{r}#1611,
     *                        $$still_hired$converted_to$keyword{r$}#1589, height.double{f}#1544, languages.int{f}#1542]]
     *               \_Eval[[null[KEYWORD] AS avg_worked_seconds#1603, null[KEYWORD] AS emp_no#1604, null[KEYWORD] AS first_name#1605,
     *                       null[KEYWORD] AS gender#1606, null[KEYWORD] AS is_rehired#1607, null[KEYWORD] AS job_positions#1608,
     *                       null[KEYWORD] AS last_name#1609, null[KEYWORD] AS salary#1610, null[KEYWORD] AS still_hired#1611]]
     *                 \_Eval[[TOLONG(emp_no{f}#1532) AS $$emp_no$converted_to$long#1587,
     *                         TOSTRING(is_rehired{f}#1550) AS $$is_rehired$converted_to$keyword#1588,
     *                         TOSTRING(still_hired{f}#1547) AS $$still_hired$converted_to$keyword#1589]]
     *                   \_Eval[[null[DOUBLE] AS height.float#1554, null[INTEGER] AS languages.byte#1555,
     *                           null[INTEGER] AS salary_change.int#1556, null[KEYWORD] AS salary_change.keyword#1557,
     *                           null[LONG] AS salary_change.long#1558]]
     *                     \_Subquery[]
     *                       \_Filter[languages{f}#1539 > 0[INTEGER]]
     *                         \_EsRelation[test_mixed_types][avg_worked_seconds{f}#1548, birth_date{f}#1536, emp..]
     */
    public void testMixedDataTypesWithExplicitCastingInSubquery() {
        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(25, projections.size());
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
        assertEquals(1, aliases.size());
        a = aliases.get(0);
        assertEquals("emp_no", a.name());
        ReferenceAttribute emp_no = as(a.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(28, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(10, implicitCastingEval.fields().size());
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(3, explicitCastingEval.fields().size());
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        assertEquals(2, missingFieldEval.fields().size());
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        assertEquals(9, implicitCastingEval.fields().size());
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        assertEquals(3, explicitCastingEval.fields().size());
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
        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
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
     * Project[[_meta_field{r}#135, !first_name, gender{r}#138, hire_date{r}#139, job{r}#140, job.raw{r}#141, languages{r}#142, !last_name,
     *          long_noidx{r}#144, !salary, avg_worked_seconds{r}#146, birth_date{r}#147, height{r}#148, height.double{r}#149,
     *          height.half_float{r}#150, height.scaled_float{r}#151, is_rehired{r}#152, job_positions{r}#153, languages.int{r}#154,
     *          languages.long{r}#155, languages.short{r}#156, salary_change{r}#157, still_hired{r}#158, emp_no{r}#84]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_OrderBy[[Order[emp_no{r}#84,ASC,LAST]]]
     *     \_Filter[emp_no{r}#84 > 1[INTEGER]]
     *       \_Eval[[$$languages$converted_to$long{r$}#161 AS emp_no#84]]
     *         \_UnionAll[[_meta_field{r}#135, !emp_no, !first_name, gender{r}#138, hire_date{r}#139, job{r}#140, job.raw{r}#141,
     *                     languages{r}#142, $$languages$converted_to$long{r$}#161, !last_name, long_noidx{r}#144, !salary,
     *                     avg_worked_seconds{r}#146, birth_date{r}#147, height{r}#148, height.double{r}#149, height.half_float{r}#150,
     *                     height.scaled_float{r}#151, is_rehired{r}#152, job_positions{r}#153, languages.int{r}#154,
     *                     languages.long{r}#155, languages.short{r}#156, salary_change{r}#157, still_hired{r}#158]]
     *           |_Project[[_meta_field{f}#93, emp_no{r}#162, first_name{r}#163, gender{f}#89, hire_date{r}#164, job{f}#95,
     *                      job.raw{f}#96, languages{f}#90, $$languages$converted_to$long{r$}#159, last_name{r}#165, long_noidx{f}#97,
     *                      salary{r}#166, avg_worked_seconds{r}#118, birth_date{r}#119, height{r}#120, height.double{r}#121,
     *                      height.half_float{r}#122, height.scaled_float{r}#123, is_rehired{r}#124, job_positions{r}#125,
     *                      languages.int{r}#126, languages.long{r}#127, languages.short{r}#128, salary_change{r}#129, still_hired{r}#130]]
     *           | \_Eval[[null[KEYWORD] AS emp_no#162, null[KEYWORD] AS first_name#163, TODATENANOS(hire_date{f}#94) AS hire_date#164,
     *                     null[KEYWORD] AS last_name#165, null[KEYWORD] AS salary#166]]
     *           |   \_Eval[[TOLONG(languages{f}#90) AS $$languages$converted_to$long#159]]
     *           |     \_Eval[[null[UNSIGNED_LONG] AS avg_worked_seconds#118, null[DATETIME] AS birth_date#119,
     *                         null[DOUBLE] AS height#120, null[DOUBLE] AS height.double#121, null[DOUBLE] AS height.half_float#122,
     *                         null[DOUBLE] AS height.scaled_float#123, null[KEYWORD] AS is_rehired#124, null[TEXT] AS job_positions#125,
     *                         null[INTEGER] AS languages.int#126, null[LONG] AS languages.long#127,
     *                         null[INTEGER] AS languages.short#128, null[DOUBLE] AS salary_change#129, null[KEYWORD] AS still_hired#130]]
     *           |       \_EsRelation[test][_meta_field{f}#93, emp_no{f}#87, first_name{f}#88, ..]
     *           \_Project[[_meta_field{r}#131, emp_no{r}#167, first_name{r}#168, gender{f}#101, hire_date{f}#103, job{r}#132,
     *                      job.raw{r}#133, languages{f}#105, $$languages$converted_to$long{r$}#160, last_name{r}#169, long_noidx{r}#134,
     *                      salary{r}#170, avg_worked_seconds{f}#114, birth_date{f}#102, height{f}#109, height.double{f}#110,
     *                      height.half_float{f}#112, height.scaled_float{f}#111, is_rehired{f}#116, job_positions{f}#115,
     *                      languages.int{f}#108, languages.long{f}#106, languages.short{f}#107, salary_change{f}#117, still_hired{f}#113]]
     *             \_Eval[[null[KEYWORD] AS emp_no#167, null[KEYWORD] AS first_name#168, null[KEYWORD] AS last_name#169, null[KEYWORD] A
     * S salary#170]]
     *               \_Eval[[TOLONG(languages{f}#105) AS $$languages$converted_to$long#160]]
     *                 \_Eval[[null[KEYWORD] AS _meta_field#131, null[TEXT] AS job#132, null[KEYWORD] AS job.raw#133,
     *                         null[LONG] AS long_noidx#134]]
     *                   \_Subquery[]
     *                     \_Filter[languages{f}#105 > 1[INTEGER]]
     *                       \_EsRelation[test_mixed_types][avg_worked_seconds{f}#114, birth_date{f}#102, emp_n..]
     */
    public void testSubqueryWithUnionAllOutputOverwritten() {
        LogicalPlan plan = basic().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types | WHERE languages > 1)
            | EVAL emp_no = languages::long
            | WHERE emp_no > 1
            | SORT emp_no
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(24, projections.size());
        Limit limit = as(project.child(), Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(1, aliases.size());
        Alias alias = aliases.get(0);
        assertEquals("emp_no", alias.name());
        ReferenceAttribute language_code = as(alias.child(), ReferenceAttribute.class);
        assertEquals("$$languages$converted_to$long", language_code.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(25, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval implicitCastingEval = as(subqueryProject.child(), Eval.class);
        Eval explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        Eval missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        EsRelation subqueryIndex = as(missingFieldEval.child(), EsRelation.class);
        assertEquals("test", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        implicitCastingEval = as(subqueryProject.child(), Eval.class);
        explicitCastingEval = as(implicitCastingEval.child(), Eval.class);
        missingFieldEval = as(explicitCastingEval.child(), Eval.class);
        Subquery subquery = as(missingFieldEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        greaterThan = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute fa = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", fa.name());
        literal = as(greaterThan.right(), Literal.class);
        assertEquals(1, literal.value());
        assertEquals(INTEGER, literal.dataType());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", subqueryIndex.indexPattern());
    }

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

        LogicalPlan plan = sampleData().query("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1) | keep client_ip
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Project[[!client_ip]] — client_ip is UnsupportedAttribute due to type conflict (ip vs integer)
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        List<String> originalTypes = ua.originalTypes();
        assertThat(originalTypes, hasSize(2));
        assertThat(originalTypes, is(List.of(IP.esType(), INTEGER.esType())));
        assertEquals("client_ip", ua.name());

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

        LogicalPlan plan = sampleData().query("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1)
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Limit directly over UnionAll since there is no keep/project
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        Attribute clientIpAttr = output.stream().filter(a -> "client_ip".equals(a.name())).findFirst().orElseThrow();
        UnsupportedAttribute ua = as(clientIpAttr, UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(IP.esType(), INTEGER.esType())));
        assertEquals("client_ip", ua.name());
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

        LogicalPlan plan = defaultMapping().addDefaultIncompatible().query("""
            FROM test, (FROM test_mixed_types) | keep emp_no
            """);

        // Limit[1000]
        Limit limit = as(plan, Limit.class);

        // Project[[!emp_no]]
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), LONG.esType())));
        assertEquals("emp_no", ua.name());
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
        UnsupportedAttribute ua = as(project.projections().getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), LONG.esType())));
        assertEquals("salary", ua.name());

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
        UnsupportedAttribute ua = as(salary, UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), LONG.esType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[salary{r}#15]]
     *   \_UnionAll[[emp_no{r}#13, name{r}#14, salary{r}#15]]
     *     |_Project[[emp_no{r}#6, name{r}#7, salary{r}#4]]
     *     | \_Subquery[]
     *     |   \_Eval[[TOLONG(salary{r}#8) AS salary#4]]
     *     |     \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#6, name{r}#7, salary{r}#8]
     *     \_Project[[emp_no{r}#9, name{r}#10, salary{r}#11]]
     *       \_Subquery[]
     *         \_ExternalRelation[s3://bucket/salaries_long.parquet][parquet][emp_no{r}#9, name{r}#10, salary{r}#11]
     */
    public void testExternalDatasetSubqueryConflictResolvedByCastInSubqueries() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeExternalDatasetSubquery("""
            FROM (FROM salaries_int | EVAL salary = salary::long), (FROM salaries_long)
            | KEEP salary
            """);

        Limit limit = as(plan, Limit.class);
        Project project = as(limit.child(), Project.class);
        assertThat(project.projections(), hasSize(1));
        NamedExpression salary = project.projections().getFirst();
        assertEquals("salary", salary.name());
        assertEquals(LONG, salary.dataType());
        assertFalse("salary should be resolved, not a type conflict", salary instanceof UnsupportedAttribute);

        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    /*
     * Project[[salary{r}#4]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Project[[salary{r}#4, $$salary$converted_to$long{r$}#18]]
     *     \_Eval[[$$salary$converted_to$long{r$}#18 AS salary#4]]
     *       \_UnionAll[[emp_no{r}#13, name{r}#14, !salary, $$salary$converted_to$long{r$}#18]]
     *         |_Project[[emp_no{r}#6, name{r}#7, salary{r}#19, $$salary$converted_to$long{r$}#16]]
     *         | \_Eval[[null[KEYWORD] AS salary#19]]
     *         |   \_Eval[[TOLONG(salary{r}#8) AS $$salary$converted_to$long#16]]
     *         |     \_Subquery[]
     *         |       \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#6, name{r}#7, salary{r}#8]
     *         \_Project[[emp_no{r}#9, name{r}#10, salary{r}#20, $$salary$converted_to$long{r$}#17]]
     *           \_Eval[[null[KEYWORD] AS salary#20]]
     *             \_Eval[[TOLONG(salary{r}#11) AS $$salary$converted_to$long#17]]
     *               \_Subquery[]
     *                 \_ExternalRelation[s3://bucket/salaries_long.parquet][parquet][emp_no{r}#9, name{r}#10, salary{r}#11]
     */
    public void testExternalDatasetSubqueryConflictResolvedByCastInMainQuery() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeExternalDatasetSubquery("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | EVAL salary = salary::long
            | KEEP salary
            """);

        Attribute salary = plan.output().stream().filter(a -> "salary".equals(a.name())).findFirst().orElseThrow();
        assertEquals(LONG, salary.dataType());
        assertFalse("salary should be resolved via the pushed-down cast", salary instanceof UnsupportedAttribute);

        List<UnionAll> unionAlls = new ArrayList<>();
        plan.forEachDown(UnionAll.class, unionAlls::add);
        assertThat(unionAlls, hasSize(1));
        assertEquals(2, unionAlls.getFirst().children().size());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[@timestamp{r}#1733 > 1759795200000[DATETIME]]
     *   \_UnionAll[[@timestamp{r}#1733, client.ip{r}#1734, cluster{r}#1735, event{r}#1736, event_city{r}#1737,
     *               event_city_boundary{r}#1738, event_location{r}#1739, event_log{r}#1740, event_shape{r}#1741, events_received{r}#1742,
     *               network.bytes_in{r}#1743, network.cost{r}#1744, network.eth0.currently_connected_clients{r}#1745,
     *               network.eth0.firmware_version{r}#1746, network.eth0.last_up{r}#1747, network.eth0.rx{r}#1748,
     *               network.eth0.tx{r}#1749, network.eth0.up{r}#1750, network.total_bytes_in{r}#1751, network.total_cost{r}#1752,
     *               pod{r}#1753, client_ip{r}#1754, event_duration{r}#1755, message{r}#1756]]
     *     |_Project[[@timestamp{f}#1658, client.ip{f}#1662, cluster{f}#1659, event{f}#1663, event_city{f}#1666,
     *                event_city_boundary{f}#1667, event_location{f}#1669, event_log{f}#1664, event_shape{f}#1668,
     *                events_received{f}#1665, network.bytes_in{f}#1671, network.cost{f}#1673,
     *                network.eth0.currently_connected_clients{f}#1681, network.eth0.firmware_version{f}#1680,
     *                network.eth0.last_up{f}#1679, network.eth0.rx{f}#1678, network.eth0.tx{f}#1677, network.eth0.up{f}#1676,
     *                network.total_bytes_in{r}#1757, network.total_cost{r}#1758, pod{f}#1660, client_ip{r}#1690,
     *                event_duration{r}#1691, message{r}#1692]]
     *     | \_Eval[[TOLONG(network.total_bytes_in{f}#1672) AS network.total_bytes_in#1757,
     *               TODOUBLE(network.total_cost{f}#1674) AS network.total_cost#1758]]
     *     |   \_Eval[[null[IP] AS client_ip#1690, null[LONG] AS event_duration#1691, null[KEYWORD] AS message#1692]]
     *     |     \_EsRelation[k8s][@timestamp{f}#1658, client.ip{f}#1662, cluster{f}#1..]
     *     |_Project[[@timestamp{f}#1682, client.ip{r}#1693, cluster{r}#1694, event{r}#1695, event_city{r}#1696,
     *                event_city_boundary{r}#1697, event_location{r}#1698, event_log{r}#1699, event_shape{r}#1700, events_received{r}#1701,
     *                network.bytes_in{r}#1702, network.cost{r}#1703, network.eth0.currently_connected_clients{r}#1704,
     *                network.eth0.firmware_version{r}#1705, network.eth0.last_up{r}#1706, network.eth0.rx{r}#1707,
     *                network.eth0.tx{r}#1708, network.eth0.up{r}#1709, network.total_bytes_in{r}#1710, network.total_cost{r}#1711,
     *                pod{r}#1712, client_ip{f}#1683, event_duration{f}#1684, message{f}#1685]]
     *     | \_Eval[[null[IP] AS client.ip#1693, null[KEYWORD] AS cluster#1694, null[KEYWORD] AS event#1695,
     *               null[GEO_POINT] AS event_city#1696, null[GEO_SHAPE] AS event_city_boundary#1697,
     *               null[CARTESIAN_POINT] AS event_location#1698, null[TEXT] AS event_log#1699,
     *               null[CARTESIAN_SHAPE] AS event_shape#1700, null[LONG] AS events_received#1701,
     *               null[LONG] AS network.bytes_in#1702, null[DOUBLE] AS network.cost#1703,
     *               null[INTEGER] AS network.eth0.currently_connected_clients#1704,
     *               null[VERSION] AS network.eth0.firmware_version#1705, null[DATE_NANOS] AS network.eth0.last_up#1706,
     *               null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#1707, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#1708,
     *               null[BOOLEAN] AS network.eth0.up#1709, null[LONG] AS network.total_bytes_in#1710,
     *               null[DOUBLE] AS network.total_cost#1711, null[KEYWORD] AS pod#1712]]
     *     |   \_Subquery[]
     *     |     \_EsRelation[sample_data][@timestamp{f}#1682, client_ip{f}#1683, event_durati..]
     *     \_Project[[@timestamp{f}#1686, client.ip{r}#1713, cluster{r}#1714, event{r}#1715, event_city{r}#1716,
     *                event_city_boundary{r}#1717, event_location{r}#1718, event_log{r}#1719, event_shape{r}#1720,
     *                events_received{r}#1721, network.bytes_in{r}#1722, network.cost{r}#1723,
     *                network.eth0.currently_connected_clients{r}#1724, network.eth0.firmware_version{r}#1725,
     *                network.eth0.last_up{r}#1726, network.eth0.rx{r}#1727, network.eth0.tx{r}#1728, network.eth0.up{r}#1729,
     *                network.total_bytes_in{r}#1730, network.total_cost{r}#1731, pod{r}#1732, client_ip{f}#1687,
     *                event_duration{f}#1688, message{f}#1689]]
     *       \_Eval[[null[IP] AS client.ip#1713, null[KEYWORD] AS cluster#1714, null[KEYWORD] AS event#1715,
     *               null[GEO_POINT] AS event_city#1716, null[GEO_SHAPE] AS event_city_boundary#1717,
     *               null[CARTESIAN_POINT] AS event_location#1718, null[TEXT] AS event_log#1719,
     *               null[CARTESIAN_SHAPE] AS event_shape#1720, null[LONG] AS events_received#1721,
     *               null[LONG] AS network.bytes_in#1722, null[DOUBLE] AS network.cost#1723,
     *               null[INTEGER] AS network.eth0.currently_connected_clients#1724, null[VERSION] AS network.eth0.firmware_version#1725,
     *               null[DATE_NANOS] AS network.eth0.last_up#1726, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#1727,
     *               null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#1728, null[BOOLEAN] AS network.eth0.up#1729,
     *               null[LONG] AS network.total_bytes_in#1730, null[DOUBLE] AS network.total_cost#1731, null[KEYWORD] AS pod#1732]]
     *         \_Subquery[]
     *           \_Filter[client_ip{f}#1687 == [0 0 0 0 0 0 0 0 0 0 ff ff 7f 0 0 1][IP]]
     *             \_EsRelation[sample_data][@timestamp{f}#1686, client_ip{f}#1687, event_durati..]
     */
    public void testSubqueryWithTimeSeriesIndexInMainQuery() {
        LogicalPlan plan = k8s().addSampleData().query("""
            FROM k8s, (FROM sample_data), (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(24, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        relation = as(subquery.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[@timestamp{r}#2290 > 1759795200000[DATETIME]]
     *   \_UnionAll[[@timestamp{r}#2290, client_ip{r}#2291, event_duration{r}#2292, message{r}#2293, client.ip{r}#2294, cluster{r}#2295,
     *               event{r}#2296, event_city{r}#2297, event_city_boundary{r}#2298, event_location{r}#2299, event_log{r}#2300,
     *               event_shape{r}#2301, events_received{r}#2302, network.bytes_in{r}#2303, network.cost{r}#2304,
     *               network.eth0.currently_connected_clients{r}#2305, network.eth0.firmware_version{r}#2306, network.eth0.last_up{r}#2307,
     *               network.eth0.rx{r}#2308, network.eth0.tx{r}#2309, network.eth0.up{r}#2310, network.total_bytes_in{r}#2311,
     *               network.total_cost{r}#2312, a{r}#2313, tx_max{r}#2314, pod{r}#2315]]
     *     |_Project[[@timestamp{f}#2211, client_ip{f}#2212, event_duration{f}#2213, message{f}#2214, client.ip{r}#2243, cluster{r}#2244,
     *                event{r}#2245, event_city{r}#2246, event_city_boundary{r}#2247, event_location{r}#2248, event_log{r}#2249,
     *                event_shape{r}#2250, events_received{r}#2251, network.bytes_in{r}#2252, network.cost{r}#2253,
     *                network.eth0.currently_connected_clients{r}#2254, network.eth0.firmware_version{r}#2255,
     *                network.eth0.last_up{r}#2256, network.eth0.rx{r}#2257, network.eth0.tx{r}#2258, network.eth0.up{r}#2259,
     *                network.total_bytes_in{r}#2260, network.total_cost{r}#2261, a{r}#2262, tx_max{r}#2263, pod{r}#2264]]
     *     | \_Eval[[null[IP] AS client.ip#2243, null[KEYWORD] AS cluster#2244, null[KEYWORD] AS event#2245,
     *               null[GEO_POINT] AS event_city#2246, null[GEO_SHAPE] AS event_city_boundary#2247,
     *               null[CARTESIAN_POINT] AS event_location#2248, null[TEXT] AS event_log#2249,
     *               null[CARTESIAN_SHAPE] AS event_shape#2250, null[LONG] AS events_received#2251, null[LONG] AS network.bytes_in#2252,
     *               null[DOUBLE] AS network.cost#2253, null[INTEGER] AS network.eth0.currently_connected_clients#2254,
     *               null[VERSION] AS network.eth0.firmware_version#2255, null[DATE_NANOS] AS network.eth0.last_up#2256,
     *               null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#2257, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#2258,
     *               null[BOOLEAN] AS network.eth0.up#2259, null[LONG] AS network.total_bytes_in#2260,
     *               null[DOUBLE] AS network.total_cost#2261, null[AGGREGATE_METRIC_DOUBLE] AS a#2262, null[DOUBLE] AS tx_max#2263,
     *               null[KEYWORD] AS pod#2264]]
     *     |   \_EsRelation[sample_data][@timestamp{f}#2211, client_ip{f}#2212, event_durati..]
     *     |_Project[[@timestamp{f}#2215, client_ip{r}#2265, event_duration{r}#2266, message{r}#2267, client.ip{f}#2219, cluster{f}#2216,
     *                event{f}#2220, event_city{f}#2223, event_city_boundary{f}#2224, event_location{f}#2226, event_log{f}#2221,
     *                event_shape{f}#2225, events_received{f}#2222, network.bytes_in{f}#2228, network.cost{f}#2230,
     *                network.eth0.currently_connected_clients{f}#2238, network.eth0.firmware_version{f}#2237,
     *                network.eth0.last_up{f}#2236, network.eth0.rx{f}#2235, network.eth0.tx{f}#2234, network.eth0.up{f}#2233,
     *                network.total_bytes_in{r}#2316, network.total_cost{r}#2317, a{r}#2208, tx_max{r}#2205, pod{f}#2217]]
     *     | \_Eval[[TOLONG(network.total_bytes_in{f}#2229) AS network.total_bytes_in#2316,
     *               TODOUBLE(network.total_cost{f}#2231) AS network.total_cost#2317]]
     *     |   \_Eval[[null[IP] AS client_ip#2265, null[LONG] AS event_duration#2266, null[KEYWORD] AS message#2267]]
     *     |     \_Subquery[]
     *     |       \_InlineStats[]
     *     |         \_Aggregate[[pod{f}#2217],[MAX(network.eth0.tx{f}#2234,true[BOOLEAN],PT0S[TIME_DURATION]) AS tx_max#2205, pod{f}#2217]]
     *     |           \_Eval[[TOAGGREGATEMETRICDOUBLE(1[INTEGER]) AS a#2208]]
     *     |             \_EsRelation[k8s][@timestamp{f}#2215, client.ip{f}#2219, cluster{f}#2..]
     *     \_Project[[@timestamp{f}#2239, client_ip{f}#2240, event_duration{f}#2241, message{f}#2242, client.ip{r}#2268, cluster{r}#2269,
     *                event{r}#2270, event_city{r}#2271, event_city_boundary{r}#2272, event_location{r}#2273, event_log{r}#2274,
     *                event_shape{r}#2275, events_received{r}#2276, network.bytes_in{r}#2277, network.cost{r}#2278,
     *                network.eth0.currently_connected_clients{r}#2279, network.eth0.firmware_version{r}#2280,
     *                network.eth0.last_up{r}#2281, network.eth0.rx{r}#2282, network.eth0.tx{r}#2283, network.eth0.up{r}#2284,
     *                network.total_bytes_in{r}#2285, network.total_cost{r}#2286, a{r}#2287, tx_max{r}#2288, pod{r}#2289]]
     *       \_Eval[[null[IP] AS client.ip#2268, null[KEYWORD] AS cluster#2269, null[KEYWORD] AS event#2270,
     *               null[GEO_POINT] AS event_city#2271, null[GEO_SHAPE] AS event_city_boundary#2272,
     *               null[CARTESIAN_POINT] AS event_location#2273, null[TEXT] AS event_log#2274,
     *               null[CARTESIAN_SHAPE] AS event_shape#2275, null[LONG] AS events_received#2276,
     *               null[LONG] AS network.bytes_in#2277, null[DOUBLE] AS network.cost#2278,
     *               null[INTEGER] AS network.eth0.currently_connected_clients#2279, null[VERSION] AS network.eth0.firmware_version#2280,
     *               null[DATE_NANOS] AS network.eth0.last_up#2281, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#2282,
     *               null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#2283, null[BOOLEAN] AS network.eth0.up#2284,
     *               null[LONG] AS network.total_bytes_in#2285, null[DOUBLE] AS network.total_cost#2286,
     *               null[AGGREGATE_METRIC_DOUBLE] AS a#2287, null[DOUBLE] AS tx_max#2288, null[KEYWORD] AS pod#2289]]
     *         \_Subquery[]
     *           \_Filter[client_ip{f}#2240 == [0 0 0 0 0 0 0 0 0 0 ff ff 7f 0 0 1][IP]]
     *             \_EsRelation[sample_data][@timestamp{f}#2239, client_ip{f}#2240, event_durati..]
     */
    public void testSubqueryWithTimeSeriesIndexInSubquery() {
        LogicalPlan plan = sampleData().addK8sDownsampled().query("""
            FROM sample_data,
                       (FROM k8s | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                       (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        InlineStats inlineStats = as(subquery.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[@timestamp{r}#320 > 1759795200000[DATETIME]]
     *   \_UnionAll[[@timestamp{r}#320, client.ip{r}#321, cluster{r}#322, event{r}#323, event_city{r}#324, event_city_boundary{r}#325,
     *               event_location{r}#326, event_log{r}#327, event_shape{r}#328, events_received{r}#329, network.bytes_in{r}#330,
     *               network.cost{r}#331, network.eth0.currently_connected_clients{r}#332, network.eth0.firmware_version{r}#333,
     *               network.eth0.last_up{r}#334, network.eth0.rx{r}#335, network.eth0.tx{r}#336, network.eth0.up{r}#337,
     *               network.total_bytes_in{r}#338, network.total_cost{r}#339, pod{r}#340, a{r}#341, tx_max{r}#342, client_ip{r}#343,
     *               event_duration{r}#344, message{r}#345]]
     *     |_Project[[@timestamp{f}#238, client.ip{f}#242, cluster{f}#239, event{f}#243, event_city{f}#246, event_city_boundary{f}#247,
     *                event_location{f}#249, event_log{f}#244, event_shape{f}#248, events_received{f}#245, network.bytes_in{f}#251,
     *                network.cost{f}#253, network.eth0.currently_connected_clients{f}#261, network.eth0.firmware_version{f}#260,
     *                network.eth0.last_up{f}#259, network.eth0.rx{f}#258, network.eth0.tx{f}#257, network.eth0.up{f}#256,
     *                network.total_bytes_in{r}#346, network.total_cost{r}#347, pod{f}#240, a{r}#290, tx_max{r}#291, client_ip{r}#292,
     *                event_duration{r}#293, message{r}#294]]
     *     | \_Eval[[TOLONG(network.total_bytes_in{f}#252) AS network.total_bytes_in#346,
     *               TODOUBLE(network.total_cost{f}#254) AS network.total_cost#347]]
     *     |   \_Eval[[null[AGGREGATE_METRIC_DOUBLE] AS a#290, null[DOUBLE] AS tx_max#291, null[IP] AS client_ip#292,
     *                 null[LONG] AS event_duration#293, null[KEYWORD] AS message#294]]
     *     |     \_EsRelation[k8s][@timestamp{f}#238, client.ip{f}#242, cluster{f}#239, ..]
     *     |_Project[[@timestamp{f}#262, client.ip{f}#266, cluster{f}#263, event{f}#267, event_city{f}#270,
     *                event_city_boundary{f}#271, event_location{f}#273, event_log{f}#268, event_shape{f}#272, events_received{f}#269,
     *                network.bytes_in{f}#275, network.cost{f}#277, network.eth0.currently_connected_clients{f}#285,
     *                network.eth0.firmware_version{f}#284, network.eth0.last_up{f}#283, network.eth0.rx{f}#282, network.eth0.tx{f}#281,
     *                network.eth0.up{f}#280, network.total_bytes_in{r}#348, network.total_cost{r}#349, pod{f}#264, a{r}#235,
     *                tx_max{r}#232, client_ip{r}#295, event_duration{r}#296, message{r}#297]]
     *     | \_Eval[[TOLONG(network.total_bytes_in{f}#276) AS network.total_bytes_in#348,
     *               TODOUBLE(network.total_cost{f}#278) AS network.total_cost#349]]
     *     |   \_Eval[[null[IP] AS client_ip#295, null[LONG] AS event_duration#296, null[KEYWORD] AS message#297]]
     *     |     \_Subquery[]
     *     |       \_InlineStats[]
     *     |         \_Aggregate[[pod{f}#264],[MAX(network.eth0.tx{f}#281,true[BOOLEAN],PT0S[TIME_DURATION]) AS tx_max#232, pod{f}#264]]
     *     |           \_Eval[[TOAGGREGATEMETRICDOUBLE(1[INTEGER]) AS a#235]]
     *     |             \_EsRelation[k8s][@timestamp{f}#262, client.ip{f}#266, cluster{f}#263, ..]
     *     \_Project[[@timestamp{f}#286, client.ip{r}#298, cluster{r}#299, event{r}#300, event_city{r}#301, event_city_boundary{r}#302,
     *                event_location{r}#303, event_log{r}#304, event_shape{r}#305, events_received{r}#306, network.bytes_in{r}#307,
     *                network.cost{r}#308, network.eth0.currently_connected_clients{r}#309, network.eth0.firmware_version{r}#310,
     *                network.eth0.last_up{r}#311, network.eth0.rx{r}#312, network.eth0.tx{r}#313, network.eth0.up{r}#314,
     *                network.total_bytes_in{r}#315, network.total_cost{r}#316, pod{r}#317, a{r}#318, tx_max{r}#319, client_ip{f}#287,
     *                event_duration{f}#288, message{f}#289]]
     *       \_Eval[[null[IP] AS client.ip#298, null[KEYWORD] AS cluster#299, null[KEYWORD] AS event#300,
     *               null[GEO_POINT] AS event_city#301, null[GEO_SHAPE] AS event_city_boundary#302,
     *               null[CARTESIAN_POINT] AS event_location#303, null[TEXT] AS event_log#304, null[CARTESIAN_SHAPE] AS event_shape#305,
     *               null[LONG] AS events_received#306, null[LONG] AS network.bytes_in#307, null[DOUBLE] AS network.cost#308,
     *               null[INTEGER] AS network.eth0.currently_connected_clients#309, null[VERSION] AS network.eth0.firmware_version#310,
     *               null[DATE_NANOS] AS network.eth0.last_up#311, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#312,
     *               null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#313, null[BOOLEAN] AS network.eth0.up#314,
     *               null[LONG] AS network.total_bytes_in#315, null[DOUBLE] AS network.total_cost#316, null[KEYWORD] AS pod#317,
     *               null[AGGREGATE_METRIC_DOUBLE] AS a#318, null[DOUBLE] AS tx_max#319]]
     *         \_Subquery[]
     *           \_Filter[client_ip{f}#287 == [0 0 0 0 0 0 0 0 0 0 ff ff 7f 0 0 1][IP]]
     *             \_EsRelation[sample_data][@timestamp{f}#286, client_ip{f}#287, event_duration..]
     */
    public void testSubqueryWithTimeSeriesIndexInMainQueryAndSubquery() {
        LogicalPlan plan = k8s().addSampleData().query("""
            FROM k8s,
                       (FROM k8s | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                       (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        assertEquals(26, output.size());
        assertEquals(3, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        eval = as(eval.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        InlineStats inlineStats = as(subquery.child(), InlineStats.class);
        Aggregate aggregate = as(inlineStats.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        relation = as(eval.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());

        subqueryProject = as(unionAll.children().get(2), Project.class);
        eval = as(subqueryProject.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        filter = as(subquery.child(), Filter.class);
        relation = as(filter.child(), EsRelation.class);
        assertEquals("sample_data", relation.indexPattern());
        assertEquals(IndexMode.STANDARD, relation.indexMode());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[MATCH(client_ip{r}#598,127.0.0.1[KEYWORD])]
     *   \_UnionAll[[@timestamp{r}#597, client_ip{r}#598, event_duration{r}#599, message{r}#600]]
     *     |_Project[[@timestamp{f}#589, client_ip{f}#590, event_duration{f}#591, message{f}#592]]
     *     | \_EsRelation[sample_data][@timestamp{f}#589, client_ip{f}#590, event_duration..]
     *     \_Project[[@timestamp{f}#593, client_ip{f}#594, event_duration{f}#595, message{f}#596]]
     *       \_Subquery[]
     *         \_Filter[:(message{f}#596,error[KEYWORD])]
     *           \_EsRelation[sample_data][@timestamp{f}#593, client_ip{f}#594, event_duration..]
     */
    public void testSubqueryWithFullTextFunctionInMainQuery() {
        LogicalPlan plan = basic().addSampleData().query("""
            FROM sample_data, (FROM sample_data | WHERE message:"error")
            | WHERE match(client_ip,"127.0.0.1")
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Match matchFunction = as(filter.condition(), Match.class);
        ReferenceAttribute clientIP = as(matchFunction.field(), ReferenceAttribute.class);
        assertEquals("client_ip", clientIP.name());
        Literal literal = as(matchFunction.query(), Literal.class);
        assertEquals(new BytesRef("127.0.0.1"), literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        List<Attribute> output = unionAll.output();
        // all fields from the two indices
        assertEquals(4, output.size());
        assertEquals(2, unionAll.children().size());

        Project subqueryProject = as(unionAll.children().get(0), Project.class);
        EsRelation subqueryIndex = as(subqueryProject.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());

        subqueryProject = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(subqueryProject.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        MatchOperator matchOperator = as(subqueryFilter.condition(), MatchOperator.class);
        FieldAttribute message = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("message", message.name());
        literal = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("error"), literal.value());
        subqueryIndex = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("sample_data", subqueryIndex.indexPattern());
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
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addSampleData().addRemoteMissingIndex().query("""
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
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addNoFieldsIndex().query("""
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
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addEmptyIndex().query("""
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
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );

        LogicalPlan plan = basic().addNoFieldsIndex().addEmptyIndex().query("""
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
        assumeTrue("Prune no-fields in subquery", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS.isEnabled());
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM no_fields_index), (FROM no_fields_index)
                | STATS {}
                """, count);
            var plan = basic().addNoFieldsIndex().query(query);

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
        assumeTrue("Prune no-fields in subquery", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS.isEnabled());
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM empty_index), (FROM empty_index)
                | STATS {}
                """, count);
            var plan = basic().addEmptyIndex().query(query);

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
        assumeTrue("Prune no-fields in subquery", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_PRUNE_NO_FIELDS.isEnabled());
        for (String count : List.of("count()", "count(*)", "count(1)")) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM no_fields_index), (FROM empty_index)
                | STATS {}
                """, count);
            var plan = basic().addEmptyIndex().addNoFieldsIndex().query(query);

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

    /*
     * Project[[_meta_field{r}#31, emp_no{r}#32, fname{r}#5, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37, languages{r}#38,
     *          last_name{r}#39, long_noidx{r}#40, salary{r}#41, emp_no_str{r}#8]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Eval[[$$emp_no$converted_to$keyword{r$}#44 AS emp_no_str#8]]
     *     \_Project[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33 AS fname#5, gender{r}#34, hire_date{r}#35, job{r}#36,
     *                job.raw{r}#37, languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41,
     *                $$emp_no$converted_to$keyword{r$}#44]]
     *       \_UnionAll[[_meta_field{r}#31, emp_no{r}#32, $$emp_no$converted_to$keyword{r$}#44, first_name{r}#33, gender{r}#34,
     *                   hire_date{r}#35, job{r}#36, job.raw{r}#37, languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41]]
     *         |_Project[[_meta_field{f}#15, emp_no{f}#9, $$emp_no$converted_to$keyword{r$}#42, first_name{f}#10, gender{f}#11,
     *                    hire_date{f}#16, job{f}#17, job.raw{f}#18, languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     *         | \_Eval[[TOSTRING(emp_no{f}#9) AS $$emp_no$converted_to$keyword#42]]
     *         |   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, ..]
     *         \_Project[[_meta_field{f}#26, emp_no{f}#20, $$emp_no$converted_to$keyword{r$}#43, first_name{f}#21, gender{f}#22,
     *                    hire_date{f}#27, job{f}#28, job.raw{f}#29, languages{f}#23, last_name{f}#24, long_noidx{f}#30, salary{f}#25]]
     *           \_Eval[[TOSTRING(emp_no{f}#20) AS $$emp_no$converted_to$keyword#43]]
     *             \_Subquery[]
     *               \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     */
    public void testSubqueryWithRenameAndEvalWithConversionFunction() {
        assumeTrue(
            "Require the fix of synthetic attributes carry over",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_CARRY_OVER_SYNTHETIC_CONVERT_ATTRIBUTES.isEnabled()
        );
        LogicalPlan plan = basic().query("""
            FROM test, (FROM test)
            | RENAME first_name AS fname
            | EVAL emp_no_str = to_string(emp_no)
            """);

        String syntheticName = "$$emp_no$converted_to$keyword";

        // The outer Project is the final user-facing projection added during finishing analysis.
        Project outerProject = as(plan, Project.class);
        // The synthetic attribute must NOT leak into the user-visible output.
        assertTrue(
            "User-visible projection should not expose [" + syntheticName + "]",
            outerProject.projections().stream().noneMatch(p -> syntheticName.equals(p.name()))
        );

        Limit limit = as(outerProject.child(), Limit.class);
        Eval outerEval = as(limit.child(), Eval.class);
        assertEquals(1, outerEval.fields().size());
        Alias outerAlias = outerEval.fields().get(0);
        assertEquals("emp_no_str", outerAlias.name());
        // ResolveUnionTypesInUnionAll's replaceConvertFunctions rewrote to_string(emp_no) to a direct ref.
        ReferenceAttribute outerRef = as(outerAlias.child(), ReferenceAttribute.class);
        assertEquals(syntheticName, outerRef.name());
        assertEquals(KEYWORD, outerRef.dataType());

        Project renameProject = as(outerEval.child(), Project.class);
        // The fix carries the synthetic attribute through the RENAME-Project so that the reference
        // inserted above (in outerEval) has a binding. Without the fix this assertion would fail.
        assertTrue(
            "Project above UnionAll must expose [" + syntheticName + "] in its projections",
            renameProject.projections().stream().anyMatch(p -> syntheticName.equals(p.name()))
        );
        // Confirm the RENAME's original alias is still present.
        assertTrue(
            "Project should still rename first_name to fname",
            renameProject.projections().stream().anyMatch(p -> p instanceof Alias a && "fname".equals(a.name()))
        );

        UnionAll unionAll = as(renameProject.child(), UnionAll.class);
        // Sanity: the synthetic must also be in the UnionAll's output (this part isn't new).
        assertTrue(
            "UnionAll output must contain [" + syntheticName + "]",
            unionAll.output().stream().anyMatch(a -> syntheticName.equals(a.name()))
        );
        assertEquals(2, unionAll.children().size());
        // Each branch must materialize the synthetic via TOSTRING(emp_no) AS $$emp_no$converted_to$keyword.
        for (int i = 0; i < 2; i++) {
            Project branchProject = as(unionAll.children().get(i), Project.class);
            assertTrue(
                "UnionAll branch [" + i + "] must expose [" + syntheticName + "]",
                branchProject.projections().stream().anyMatch(p -> syntheticName.equals(p.name()))
            );
            Eval branchEval = as(branchProject.child(), Eval.class);
            assertTrue(
                "UnionAll branch [" + i + "] Eval must define [" + syntheticName + "] via TOSTRING(emp_no)",
                branchEval.fields().stream().anyMatch(a -> syntheticName.equals(a.name()))
            );
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Eval[[TOSTRING(id{r}#5) AS id_str#8]]
     *   \_Project[[_meta_field{r}#31, emp_no{r}#32 AS id#5, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36,
     *              job.raw{r}#37, languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41]]
     *     \_UnionAll[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36,
     *                 job.raw{r}#37, languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41]]
     *       |_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, ..., salary{f}#14]]
     *       | \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, ..]
     *       \_Project[[_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..., salary{f}#25]]
     *         \_Subquery[]
     *           \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     */
    public void testSubqueryWithRenameOnSameFieldAsConvertFunction() {
        LogicalPlan plan = basic().query("""
            FROM test, (FROM test)
            | RENAME emp_no AS id
            | EVAL id_str = to_string(id)
            """);

        // No synthetic exists anywhere in the analyzed plan.
        plan.forEachUp(p -> p.output().forEach(attr -> {
            assertFalse(
                "Unexpected synthetic attribute [" + attr.name() + "] in plan node " + p.nodeName(),
                attr.synthetic() && attr.name().startsWith("$$") && attr.name().contains("$converted_to$")
            );
        }));

        // The EVAL contains the un-pushed-down TOSTRING(id) function call.
        Limit limit = as(plan, Limit.class);
        Eval outerEval = as(limit.child(), Eval.class);
        assertEquals(1, outerEval.fields().size());
        Alias idStrAlias = outerEval.fields().get(0);
        assertEquals("id_str", idStrAlias.name());
        ToString toStringFn = as(idStrAlias.child(), ToString.class);
        ReferenceAttribute idRef = as(toStringFn.field(), ReferenceAttribute.class);
        assertEquals("id", idRef.name());

        // RENAME-Project below the EVAL exposes the alias [emp_no AS id], and only the regular columns
        // (no synthetic carry-over because no synthetic was ever created).
        Project renameProject = as(outerEval.child(), Project.class);
        assertTrue(
            "RENAME-Project must rename emp_no to id",
            renameProject.projections().stream().anyMatch(p -> p instanceof Alias a && "id".equals(a.name()))
        );
        assertTrue(
            "RENAME-Project should not carry over any synthetic conversion attributes",
            renameProject.projections().stream().noneMatch(p -> p.name().startsWith("$$") && p.name().contains("$converted_to$"))
        );

        UnionAll unionAll = as(renameProject.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        assertTrue(
            "UnionAll output should not contain any synthetic conversion attributes",
            unionAll.output().stream().noneMatch(a -> a.name().startsWith("$$") && a.name().contains("$converted_to$"))
        );
    }

    /*
     * Project[[_meta_field{r}#31, id{r}#8, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37,
     *          languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, emp_no_str{r}#5]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Project[[_meta_field{r}#31, emp_no{r}#32 AS id#8, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36,
     *              job.raw{r}#37, languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, emp_no_str{r}#5,
     *              $$emp_no$converted_to$keyword{r$}#44]]
     *     \_Eval[[$$emp_no$converted_to$keyword{r$}#44 AS emp_no_str#5]]
     *       \_UnionAll[[_meta_field{r}#31, emp_no{r}#32, $$emp_no$converted_to$keyword{r$}#44, first_name{r}#33, gender{r}#34,
     *                   hire_date{r}#35, job{r}#36, job.raw{r}#37, languages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41]]
     *         |_Project[[_meta_field{f}#15, emp_no{f}#9, $$emp_no$converted_to$keyword{r$}#42, first_name{f}#10, gender{f}#11,
     *                    hire_date{f}#16, job{f}#17, job.raw{f}#18, languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     *         | \_Eval[[TOSTRING(emp_no{f}#9) AS $$emp_no$converted_to$keyword#42]]
     *         |   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, ..]
     *         \_Project[[_meta_field{f}#26, emp_no{f}#20, $$emp_no$converted_to$keyword{r$}#43, first_name{f}#21, gender{f}#22,
     *                    hire_date{f}#27, job{f}#28, job.raw{f}#29, languages{f}#23, last_name{f}#24, long_noidx{f}#30, salary{f}#25]]
     *           \_Eval[[TOSTRING(emp_no{f}#20) AS $$emp_no$converted_to$keyword#43]]
     *             \_Subquery[]
     *               \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     */
    public void testSubqueryWithConvertFunctionBeforeRenameOnSameField() {
        assumeTrue(
            "Require the fix of synthetic attributes carry over",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_CARRY_OVER_SYNTHETIC_CONVERT_ATTRIBUTES.isEnabled()
        );
        LogicalPlan plan = basic().query("""
            FROM test, (FROM test)
            | EVAL emp_no_str = to_string(emp_no)
            | RENAME emp_no AS id
            """);

        String syntheticName = "$$emp_no$converted_to$keyword";

        // The outer Project added by UnionTypesCleanup drops the synthetic from the user-visible output.
        Project outerProject = as(plan, Project.class);
        assertTrue(
            "User-visible projection should not expose [" + syntheticName + "]",
            outerProject.projections().stream().noneMatch(p -> syntheticName.equals(p.name()))
        );
        // ...but must expose the user-facing renamed column and the new EVAL column.
        assertTrue("User-visible projection must expose [id]", outerProject.projections().stream().anyMatch(p -> "id".equals(p.name())));
        assertTrue(
            "User-visible projection must expose [emp_no_str]",
            outerProject.projections().stream().anyMatch(p -> "emp_no_str".equals(p.name()))
        );

        Limit limit = as(outerProject.child(), Limit.class);
        Project renameProject = as(limit.child(), Project.class);
        // The RENAME-Project must alias emp_no to id and ALSO carry over the synthetic so it remains
        // visible above the EVAL until UnionTypesCleanup strips it. Without the fix, the synthetic would
        // be dropped here.
        assertTrue(
            "RENAME-Project must rename emp_no to id",
            renameProject.projections().stream().anyMatch(p -> p instanceof Alias a && "id".equals(a.name()))
        );
        assertTrue(
            "RENAME-Project must expose [emp_no_str] (the EVAL alias)",
            renameProject.projections().stream().anyMatch(p -> "emp_no_str".equals(p.name()))
        );
        assertTrue(
            "RENAME-Project must carry over the synthetic [" + syntheticName + "]",
            renameProject.projections().stream().anyMatch(p -> syntheticName.equals(p.name()))
        );

        Eval evalNode = as(renameProject.child(), Eval.class);
        assertEquals(1, evalNode.fields().size());
        Alias evalAlias = evalNode.fields().get(0);
        assertEquals("emp_no_str", evalAlias.name());
        ReferenceAttribute evalRef = as(evalAlias.child(), ReferenceAttribute.class);
        assertEquals(syntheticName, evalRef.name());

        UnionAll unionAll = as(evalNode.child(), UnionAll.class);
        assertTrue(
            "UnionAll output must contain [" + syntheticName + "]",
            unionAll.output().stream().anyMatch(a -> syntheticName.equals(a.name()))
        );
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[ABSENT($$id_int$converted_to$keyword{r$}#41,true[BOOLEAN],PT0S[TIME_DURATION])
     *               AS absent(to_string(id_int))#11]]
     *   \_MvExpand[other2{r}#5,other2{r}#38]
     *     \_LookupJoin[LEFT,[id_int{r}#34, other2{r}#5],[id_int{f}#24, other2{f}#31],false,null]
     *       |_Project[[extra1{r}#32, extra2{r}#33 AS other2#5, id_int{r}#34, ip_addr{r}#35, is_active_bool{r}#36,
     *                  name_str{r}#37, $$id_int$converted_to$keyword{r$}#41]]
     *       | \_UnionAll[[extra1{r}#32, extra2{r}#33, id_int{r}#34, $$id_int$converted_to$keyword{r$}#41,
     *                     ip_addr{r}#35, is_active_bool{r}#36, name_str{r}#37]]
     *       |   |_Project[[extra1{f}#16, extra2{f}#17, id_int{f}#12, $$id_int$converted_to$keyword{r$}#39,
     *       |   |          ip_addr{f}#15, is_active_bool{f}#14, name_str{f}#13]]
     *       |   | \_Eval[[TOSTRING(id_int{f}#12) AS $$id_int$converted_to$keyword#39]]
     *       |   |   \_EsRelation[multi_column_joinable][extra1{f}#16, extra2{f}#17, id_int{f}#12, ip_addr{f..]
     *       |   \_Project[[extra1{f}#22, extra2{f}#23, id_int{f}#18, $$id_int$converted_to$keyword{r$}#40,
     *       |              ip_addr{f}#21, is_active_bool{f}#20, name_str{f}#19]]
     *       |     \_Eval[[TOSTRING(id_int{f}#18) AS $$id_int$converted_to$keyword#40]]
     *       |       \_Subquery[]
     *       |         \_EsRelation[multi_column_joinable][extra1{f}#22, extra2{f}#23, id_int{f}#18, ip_addr{f..]
     *       \_EsRelation[multi_column_joinable_lookup][LOOKUP][date{f}#28, date_nanos{f}#29, id_int{f}#24, ip_addr..]
     */
    public void testSubqueryWithRenameAndOtherProcessingCommandsWithConversionFunction() {
        assumeTrue(
            "Require the fix of synthetic attributes carry over",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_CARRY_OVER_SYNTHETIC_CONVERT_ATTRIBUTES.isEnabled()
        );
        LogicalPlan plan = analyzer().addIndex("multi_column_joinable", "mapping-multi_column_joinable.json")
            .addLookupIndex("multi_column_joinable_lookup", "mapping-multi_column_joinable_lookup.json")
            .query("""
                FROM multi_column_joinable, (FROM multi_column_joinable)
                | RENAME extra2 AS other2
                | LOOKUP JOIN multi_column_joinable_lookup ON id_int, other2
                | MV_EXPAND other2
                | STATS absent(to_string(id_int))
                """);

        String syntheticName = "$$id_int$converted_to$keyword";

        // Limit -> Aggregate (STATS) -> MvExpand -> LookupJoin -> Project (RENAME) -> UnionAll.
        Limit limit = as(plan, Limit.class);
        Aggregate aggregate = as(limit.child(), Aggregate.class);
        // The aggregate's only expression is absent(to_string(id_int)), with to_string(id_int) already rewritten
        // by ResolveUnionTypesInUnionAll.replaceConvertFunctions to a reference to the synthetic attribute.
        assertEquals(1, aggregate.aggregates().size());
        Alias aggAlias = as(aggregate.aggregates().get(0), Alias.class);
        assertEquals("absent(to_string(id_int))", aggAlias.name());
        // The Aggregate's child input set must expose the synthetic so the rewritten reference resolves.
        assertTrue(
            "Aggregate's input must include [" + syntheticName + "] for absent(to_string(id_int)) to resolve",
            aggregate.inputSet().stream().anyMatch(a -> syntheticName.equals(a.name()))
        );

        MvExpand mvExpand = as(aggregate.child(), MvExpand.class);
        ReferenceAttribute mvExpandTarget = as(mvExpand.target(), ReferenceAttribute.class);
        assertEquals("other2", mvExpandTarget.name());

        LookupJoin lookupJoin = as(mvExpand.child(), LookupJoin.class);
        EsRelation lookupRight = as(lookupJoin.right(), EsRelation.class);
        assertEquals("multi_column_joinable_lookup", lookupRight.indexPattern());

        // The Project produced by RENAME extra2 AS other2 sits directly above the UnionAll on the LookupJoin's
        // left side. Its projections must include the synthetic carried over from the UnionAll - this is the
        // post-condition guaranteed by carryOverSyntheticAttributesThroughProjects.
        Project renameProject = as(lookupJoin.left(), Project.class);
        assertTrue(
            "RENAME-Project above UnionAll must alias extra2 to other2",
            renameProject.projections().stream().anyMatch(p -> p instanceof Alias a && "other2".equals(a.name()))
        );
        assertTrue(
            "RENAME-Project above UnionAll must expose [" + syntheticName + "] in its projections",
            renameProject.projections().stream().anyMatch(p -> syntheticName.equals(p.name()))
        );

        UnionAll unionAll = as(renameProject.child(), UnionAll.class);
        assertTrue(
            "UnionAll output must contain [" + syntheticName + "]",
            unionAll.output().stream().anyMatch(a -> syntheticName.equals(a.name()))
        );
        assertEquals(2, unionAll.children().size());
        // Each branch materializes the synthetic via TOSTRING(id_int) AS $$id_int$converted_to$keyword.
        for (int i = 0; i < 2; i++) {
            Project branchProject = as(unionAll.children().get(i), Project.class);
            assertTrue(
                "UnionAll branch [" + i + "] must expose [" + syntheticName + "]",
                branchProject.projections().stream().anyMatch(p -> syntheticName.equals(p.name()))
            );
            Eval branchEval = as(branchProject.child(), Eval.class);
            assertTrue(
                "UnionAll branch [" + i + "] Eval must define [" + syntheticName + "] via TOSTRING(id_int)",
                branchEval.fields().stream().anyMatch(a -> syntheticName.equals(a.name()))
            );
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[emp_no{r}#30 > 10000[INTEGER]]
     *   \_UnionAll[[_meta_field{r}#29, emp_no{r}#30, first_name{r}#31, gender{r}#32, hire_date{r}#33, job{r}#34, job.raw{r}#35,
     *               languages{r}#36, last_name{r}#37, long_noidx{r}#38, salary{r}#39, x{r}#40]]
     *     |_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, x{r}#17]]
     *     | \_Eval[[null[INTEGER] AS x#17]]
     *     |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *     \_Project[[_meta_field{r}#18, emp_no{r}#19, first_name{r}#20, gender{r}#21, hire_date{r}#22, job{r}#23, job.raw{r}#24,
     *                languages{r}#25, last_name{r}#26, long_noidx{r}#27, salary{r}#28, x{r}#4]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#18, null[INTEGER] AS emp_no#19, null[KEYWORD] AS first_name#20, null[TEXT] AS gender#21,
     *               null[DATETIME] AS hire_date#22, null[TEXT] AS job#23, null[KEYWORD] AS job.raw#24, null[INTEGER] AS languages#25,
     *               null[KEYWORD] AS last_name#26, null[LONG] AS long_noidx#27, null[INTEGER] AS salary#28]]
     *         \_Subquery[]
     *           \_Row[[1[INTEGER] AS x#4]]
     */
    public void testRowSubqueryInFrom() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW x = 1)
            | WHERE emp_no > 10000
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(10000, literal.value());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // index leg: nullEval for the ROW alias `x`
        Project indexProject = as(unionAll.children().get(0), Project.class);
        assertEquals(12, indexProject.projections().size()); // 11 test fields + x
        Eval indexEval = as(indexProject.child(), Eval.class);
        List<Alias> indexAliases = indexEval.fields();
        assertEquals(1, indexAliases.size());
        assertEquals("x", indexAliases.get(0).name());
        Literal indexNullLiteral = as(indexAliases.get(0).child(), Literal.class);
        assertNull(indexNullLiteral.value());
        assertEquals(INTEGER, indexNullLiteral.dataType());
        EsRelation indexRelation = as(indexEval.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg: nullEvals for the 11 test fields wrap a Subquery over Row[x = 1]
        Project rowProject = as(unionAll.children().get(1), Project.class);
        assertEquals(12, rowProject.projections().size());
        Eval rowEval = as(rowProject.child(), Eval.class);
        List<Alias> rowAliases = rowEval.fields();
        assertEquals(11, rowAliases.size());
        Subquery subquery = as(rowEval.child(), Subquery.class);
        Row row = as(subquery.child(), Row.class);
        assertEquals(1, row.fields().size());
        assertEquals("x", row.fields().get(0).name());
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(1, rowLiteral.value());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#33, emp_no{r}#34, first_name{r}#35, gender{r}#36, hire_date{r}#37, job{r}#38, job.raw{r}#39,
     *             languages{r}#40, last_name{r}#41, long_noidx{r}#42, salary{r}#43, x{r}#44, y{r}#45]]
     *   |_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *              languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14, x{r}#20, y{r}#21]]
     *   | \_Eval[[null[INTEGER] AS x#20, null[INTEGER] AS y#21]]
     *   |   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *   \_Project[[_meta_field{r}#22, emp_no{r}#23, first_name{r}#24, gender{r}#25, hire_date{r}#26, job{r}#27, job.raw{r}#28,
     *              languages{r}#29, last_name{r}#30, long_noidx{r}#31, salary{r}#32, x{r}#4, y{r}#8]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#22, null[INTEGER] AS emp_no#23, null[KEYWORD] AS first_name#24, null[TEXT] AS gender#25,
     *             null[DATETIME] AS hire_date#26, null[TEXT] AS job#27, null[KEYWORD] AS job.raw#28, null[INTEGER] AS languages#29,
     *             null[KEYWORD] AS last_name#30, null[LONG] AS long_noidx#31, null[INTEGER] AS salary#32]]
     *       \_Subquery[]
     *         \_Filter[y{r}#8 > 0[INTEGER]]
     *           \_Eval[[x{r}#4 + 1[INTEGER] AS y#8]]
     *             \_Row[[1[INTEGER] AS x#4]]
     */
    public void testRowSubqueryInFromWithProcessingCommandsInSubquery() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW x = 1 | EVAL y = x + 1 | WHERE y > 0)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // index leg: nullEvals for the two ROW columns x and y
        Project indexProject = as(unionAll.children().get(0), Project.class);
        assertEquals(13, indexProject.projections().size()); // 11 test fields + x + y
        Eval indexEval = as(indexProject.child(), Eval.class);
        List<Alias> indexAliases = indexEval.fields();
        assertEquals(2, indexAliases.size());
        assertEquals("x", indexAliases.get(0).name());
        assertEquals("y", indexAliases.get(1).name());
        EsRelation indexRelation = as(indexEval.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg: nullEvals for the 11 test fields wrap the analyzed subquery
        Project rowProject = as(unionAll.children().get(1), Project.class);
        assertEquals(13, rowProject.projections().size());
        Eval rowEval = as(rowProject.child(), Eval.class);
        List<Alias> rowAliases = rowEval.fields();
        assertEquals(11, rowAliases.size());
        Subquery subquery = as(rowEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan subqueryGt = as(subqueryFilter.condition(), GreaterThan.class);
        ReferenceAttribute y = as(subqueryGt.left(), ReferenceAttribute.class);
        assertEquals("y", y.name());
        Eval subqueryEval = as(subqueryFilter.child(), Eval.class);
        List<Alias> subqueryEvalFields = subqueryEval.fields();
        assertEquals(1, subqueryEvalFields.size());
        assertEquals("y", subqueryEvalFields.get(0).name());
        Add add = as(subqueryEvalFields.get(0).child(), Add.class);
        ReferenceAttribute addLeft = as(add.left(), ReferenceAttribute.class);
        assertEquals("x", addLeft.name());
        Row row = as(subqueryEval.child(), Row.class);
        assertEquals(1, row.fields().size());
        assertEquals("x", row.fields().get(0).name());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[y{r}#7 > 0[INTEGER]]
     *   \_Eval[[x{r}#4 + 1[INTEGER] AS y#7]]
     *     \_Row[[1[INTEGER] AS x#4]]
     */
    public void testRowSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM (ROW x = 1 | EVAL y = x + 1)
            | WHERE y > 0
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute y = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("y", y.name());
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> evalFields = eval.fields();
        assertEquals(1, evalFields.size());
        assertEquals("y", evalFields.get(0).name());
        Add add = as(evalFields.get(0).child(), Add.class);
        ReferenceAttribute addLeft = as(add.left(), ReferenceAttribute.class);
        assertEquals("x", addLeft.name());
        Row row = as(eval.child(), Row.class);
        assertEquals(1, row.fields().size());
        assertEquals("x", row.fields().get(0).name());
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(1, rowLiteral.value());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#47, emp_no{r}#48, first_name{r}#49, gender{r}#50, hire_date{r}#51, job{r}#52, job.raw{r}#53,
     *             languages{r}#54, last_name{r}#55, long_noidx{r}#56, salary{r}#57, x{r}#58, language_code{r}#59, language_name{r}#60]]
     *   |_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *              languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, x{r}#19, language_code{r}#20, language_name{r}#21]]
     *   | \_Eval[[null[INTEGER] AS x#19, null[INTEGER] AS language_code#20, null[KEYWORD] AS language_name#21]]
     *   |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_Project[[_meta_field{r}#22, emp_no{r}#23, first_name{r}#24, gender{r}#25, hire_date{r}#26, job{r}#27, job.raw{r}#28,
     *              languages{r}#29, last_name{r}#30, long_noidx{r}#31, salary{r}#32, x{r}#4, language_code{r}#33, language_name{r}#34]]
     *   | \_Eval[[null[KEYWORD] AS _meta_field#22, null[INTEGER] AS emp_no#23, null[KEYWORD] AS first_name#24, null[TEXT] AS gender#25,
     *             null[DATETIME] AS hire_date#26, null[TEXT] AS job#27, null[KEYWORD] AS job.raw#28, null[INTEGER] AS languages#29,
     *             null[KEYWORD] AS last_name#30, null[LONG] AS long_noidx#31, null[INTEGER] AS salary#32,
     *             null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Row[[1[INTEGER] AS x#4]]
     *   \_Project[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41,
     *              languages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, x{r}#46, language_code{f}#17, language_name{f}#18]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#35, null[INTEGER] AS emp_no#36, null[KEYWORD] AS first_name#37, null[TEXT] AS gender#38,
     *             null[DATETIME] AS hire_date#39, null[TEXT] AS job#40, null[KEYWORD] AS job.raw#41, null[INTEGER] AS languages#42,
     *             null[KEYWORD] AS last_name#43, null[LONG] AS long_noidx#44, null[INTEGER] AS salary#45, null[INTEGER] AS x#46]]
     *       \_Subquery[]
     *         \_Filter[language_code{f}#17 > 1[INTEGER]]
     *           \_EsRelation[languages][language_code{f}#17, language_name{f}#18]
     */
    public void testMixedRowAndFromSubqueriesInFrom() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().addLanguages().query("""
            FROM test
            , (ROW x = 1)
            , (FROM languages | WHERE language_code > 1)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());
        // schema across all three legs: 11 test fields + 2 languages fields + ROW alias x = 14
        int totalProjections = 14;

        // index leg
        Project indexProject = as(unionAll.children().get(0), Project.class);
        assertEquals(totalProjections, indexProject.projections().size());
        Eval indexEval = as(indexProject.child(), Eval.class);
        // nullEvals: language_code, language_name, x
        assertEquals(3, indexEval.fields().size());
        EsRelation indexRelation = as(indexEval.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg
        Project rowProject = as(unionAll.children().get(1), Project.class);
        assertEquals(totalProjections, rowProject.projections().size());
        Eval rowEval = as(rowProject.child(), Eval.class);
        // nullEvals: 11 test fields + 2 languages fields = 13
        assertEquals(13, rowEval.fields().size());
        Subquery rowSubquery = as(rowEval.child(), Subquery.class);
        Row row = as(rowSubquery.child(), Row.class);
        assertEquals("x", row.fields().get(0).name());

        // FROM-subquery leg
        Project fromProject = as(unionAll.children().get(2), Project.class);
        assertEquals(totalProjections, fromProject.projections().size());
        Eval fromEval = as(fromProject.child(), Eval.class);
        // nullEvals: 11 test fields + ROW alias x = 12
        assertEquals(12, fromEval.fields().size());
        Subquery fromSubquery = as(fromEval.child(), Subquery.class);
        Filter fromFilter = as(fromSubquery.child(), Filter.class);
        GreaterThan fromGt = as(fromFilter.condition(), GreaterThan.class);
        FieldAttribute languageCode = as(fromGt.left(), FieldAttribute.class);
        assertEquals("language_code", languageCode.name());
        EsRelation fromRelation = as(fromFilter.child(), EsRelation.class);
        assertEquals("languages", fromRelation.indexPattern());
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
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW x = 1), (ROW x = "abc")
            | keep x
            """);

        Limit limit = as(plan, Limit.class);

        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), KEYWORD.esType())));
        assertEquals("x", ua.name());

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
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW x = 1), (ROW x = "abc")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        Attribute xAttr = output.stream().filter(a -> "x".equals(a.name())).findFirst().orElseThrow();
        UnsupportedAttribute ua = as(xAttr, UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), KEYWORD.esType())));
        assertEquals("x", ua.name());
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
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = sampleData().query("""
            FROM (FROM sample_data), (ROW client_ip = 1)
            | keep client_ip
            """);

        Limit limit = as(plan, Limit.class);

        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(IP.esType(), INTEGER.esType())));
        assertEquals("client_ip", ua.name());

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
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW emp_no = "abc")
            | keep emp_no
            """);

        Limit limit = as(plan, Limit.class);

        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(1));
        UnsupportedAttribute ua = as(projections.getFirst(), UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, ua.dataType());
        assertThat(ua.originalTypes(), is(List.of(INTEGER.esType(), KEYWORD.esType())));
        assertEquals("emp_no", ua.name());

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
        assertEquals("test", indexRelation.indexPattern());

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
     * \_Filter[emp_no{r}#31 > 0[INTEGER]]
     *   \_UnionAll[[_meta_field{r}#30, emp_no{r}#31, first_name{r}#32, gender{r}#33, hire_date{r}#34, job{r}#35, job.raw{r}#36,
     *               languages{r}#37, last_name{r}#38, long_noidx{r}#39, salary{r}#40]]
     *     |_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *                languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     *     | \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_Project[[_meta_field{r}#20, emp_no{r}#7, first_name{r}#21, gender{r}#22, hire_date{r}#23, job{r}#24, job.raw{r}#25,
     *                languages{r}#26, last_name{r}#27, long_noidx{r}#28, salary{r}#29]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#20, null[KEYWORD] AS first_name#21, null[TEXT] AS gender#22,
     *               null[DATETIME] AS hire_date#23, null[TEXT] AS job#24, null[KEYWORD] AS job.raw#25,
     *               null[INTEGER] AS languages#26, null[KEYWORD] AS last_name#27, null[LONG] AS long_noidx#28,
     *               null[INTEGER] AS salary#29]]
     *         \_Subquery[]
     *           \_Eval[[TOINTEGER(emp_no{r}#4) AS emp_no#7]]
     *             \_Row[[1[KEYWORD] AS emp_no#4]]
     */
    public void testMixedDataTypesInRowSubqueryWithExplicitCastingInside() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW emp_no = "1" | EVAL emp_no = emp_no::integer)
            | WHERE emp_no > 0
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        assertEquals(INTEGER, empNo.dataType());

        UnionAll unionAll = as(filter.child(), UnionAll.class);
        // No UnsupportedAttribute in the union output: the inside cast aligned the ROW emp_no
        // type with the index emp_no type (INTEGER).
        for (Attribute attr : unionAll.output()) {
            assertFalse("Unexpected UnsupportedAttribute for [" + attr.name() + "]", attr instanceof UnsupportedAttribute);
        }
        Attribute empNoOut = unionAll.output().stream().filter(a -> "emp_no".equals(a.name())).findFirst().orElseThrow();
        assertEquals(INTEGER, empNoOut.dataType());

        assertEquals(2, unionAll.children().size());

        // index leg: no extra null/cast Evals (no missing columns introduced by the ROW)
        Project indexProject = as(unionAll.children().get(0), Project.class);
        EsRelation indexRelation = as(indexProject.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg: Project → Eval[10 nullEvals for missing test columns]
        // → Subquery → Eval[emp_no = emp_no::integer] → Row[emp_no = "1"]
        Project rowProject = as(unionAll.children().get(1), Project.class);
        Eval rowMissingEval = as(rowProject.child(), Eval.class);
        assertEquals(10, rowMissingEval.fields().size());
        Subquery rowSubquery = as(rowMissingEval.child(), Subquery.class);
        Eval rowExplicitCastEval = as(rowSubquery.child(), Eval.class);
        assertEquals(1, rowExplicitCastEval.fields().size());
        Alias castAlias = rowExplicitCastEval.fields().get(0);
        assertEquals("emp_no", castAlias.name());
        ToInteger toInteger = as(castAlias.child(), ToInteger.class);
        ReferenceAttribute toIntegerArg = as(toInteger.field(), ReferenceAttribute.class);
        assertEquals("emp_no", toIntegerArg.name());
        assertEquals(KEYWORD, toIntegerArg.dataType());
        Row row = as(rowExplicitCastEval.child(), Row.class);
        assertEquals(1, row.fields().size());
        assertEquals("emp_no", row.fields().get(0).name());
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(BytesRefs.toBytesRef("1"), rowLiteral.value());
        assertEquals(KEYWORD, rowLiteral.dataType());
    }

    /*
     * Project[[_meta_field{r}#30, first_name{r}#32, gender{r}#33, hire_date{r}#34, job{r}#35, job.raw{r}#36, languages{r}#37,
     *          last_name{r}#38, long_noidx{r}#39, salary{r}#40, emp_no{r}#7]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[emp_no{r}#7 > 1000[INTEGER]]
     *     \_Eval[[$$emp_no$converted_to$long{r$}#43 AS emp_no#7]]
     *       \_UnionAll[[_meta_field{r}#30, emp_no{r}#31, $$emp_no$converted_to$long{r$}#43, first_name{r}#32, gender{r}#33,
     *                   hire_date{r}#34, job{r}#35, job.raw{r}#36, languages{r}#37, last_name{r}#38, long_noidx{r}#39, salary{r}#40]]
     *         |_Project[[_meta_field{f}#15, emp_no{f}#9, $$emp_no$converted_to$long{r$}#41, first_name{f}#10, gender{f}#11,
     *                    hire_date{f}#16, job{f}#17, job.raw{f}#18, languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     *         | \_Eval[[TOLONG(emp_no{f}#9) AS $$emp_no$converted_to$long#41]]
     *         |   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *         \_Project[[_meta_field{r}#20, emp_no{r}#4, $$emp_no$converted_to$long{r$}#42, first_name{r}#21, gender{r}#22,
     *                    hire_date{r}#23, job{r}#24, job.raw{r}#25, languages{r}#26, last_name{r}#27, long_noidx{r}#28, salary{r}#29]]
     *           \_Eval[[TOLONG(emp_no{r}#4) AS $$emp_no$converted_to$long#42]]
     *             \_Eval[[null[KEYWORD] AS _meta_field#20, null[KEYWORD] AS first_name#21, null[TEXT] AS gender#22,
     *                     null[DATETIME] AS hire_date#23, null[TEXT] AS job#24, null[KEYWORD] AS job.raw#25,
     *                     null[INTEGER] AS languages#26, null[KEYWORD] AS last_name#27, null[LONG] AS long_noidx#28,
     *                     null[INTEGER] AS salary#29]]
     *               \_Subquery[]
     *                 \_Row[[1[INTEGER] AS emp_no#4]]
     */
    public void testMixedDataTypesInRowSubqueryWithExplicitCastingOutside() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW emp_no = 1)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 1000
            """);

        // Top-level Project drops the internal $$emp_no$converted_to$long reference from output.
        Project project = as(plan, Project.class);
        Limit limit = as(project.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNo = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no", empNo.name());
        assertEquals(LONG, empNo.dataType());

        // Outer Eval holds the user's `EVAL emp_no = emp_no::long` aliasing the pushed-down ref.
        Eval outerEval = as(filter.child(), Eval.class);
        assertEquals(1, outerEval.fields().size());
        Alias outerAlias = outerEval.fields().get(0);
        assertEquals("emp_no", outerAlias.name());
        ReferenceAttribute outerRef = as(outerAlias.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", outerRef.name());
        assertEquals(LONG, outerRef.dataType());

        UnionAll unionAll = as(outerEval.child(), UnionAll.class);
        for (Attribute attr : unionAll.output()) {
            assertFalse("Unexpected UnsupportedAttribute for [" + attr.name() + "]", attr instanceof UnsupportedAttribute);
        }
        assertEquals(2, unionAll.children().size());

        // index leg: Project → Eval[TOLONG(emp_no) AS $$converted] → EsRelation[test]
        Project indexProject = as(unionAll.children().get(0), Project.class);
        Eval indexCastEval = as(indexProject.child(), Eval.class);
        assertEquals(1, indexCastEval.fields().size());
        assertEquals("$$emp_no$converted_to$long", indexCastEval.fields().get(0).name());
        ToLong indexToLong = as(indexCastEval.fields().get(0).child(), ToLong.class);
        FieldAttribute indexEmpNo = as(indexToLong.field(), FieldAttribute.class);
        assertEquals("emp_no", indexEmpNo.name());
        assertEquals(INTEGER, indexEmpNo.dataType());
        EsRelation indexRelation = as(indexCastEval.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg: Project → Eval[TOLONG(emp_no) AS $$converted]
        // → Eval[10 nullEvals] → Subquery → Row[emp_no = 1]
        Project rowProject = as(unionAll.children().get(1), Project.class);
        Eval rowCastEval = as(rowProject.child(), Eval.class);
        assertEquals(1, rowCastEval.fields().size());
        assertEquals("$$emp_no$converted_to$long", rowCastEval.fields().get(0).name());
        ToLong rowToLong = as(rowCastEval.fields().get(0).child(), ToLong.class);
        ReferenceAttribute rowEmpNoRef = as(rowToLong.field(), ReferenceAttribute.class);
        assertEquals("emp_no", rowEmpNoRef.name());
        assertEquals(INTEGER, rowEmpNoRef.dataType());
        Eval rowMissingEval = as(rowCastEval.child(), Eval.class);
        assertEquals(10, rowMissingEval.fields().size());
        Subquery rowSubquery = as(rowMissingEval.child(), Subquery.class);
        Row row = as(rowSubquery.child(), Row.class);
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(1, rowLiteral.value());
        assertEquals(INTEGER, rowLiteral.dataType());
    }

    /*
     * Project[[_meta_field{r}#33, emp_no{r}#34, first_name{r}#35, gender{r}#36, hire_date{r}#37, job{r}#38, job.raw{r}#39,
     *          languages{r}#40, last_name{r}#41, long_noidx{r}#42, salary{r}#43, emp_no_long{r}#10]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[emp_no_long{r}#10 > 0[INTEGER]]
     *     \_Eval[[$$emp_no$converted_to$long{r$}#46 AS emp_no_long#10]]
     *       \_UnionAll[[_meta_field{r}#33, emp_no{r}#34, $$emp_no$converted_to$long{r$}#46, first_name{r}#35, gender{r}#36,
     *                   hire_date{r}#37, job{r}#38, job.raw{r}#39, languages{r}#40, last_name{r}#41, long_noidx{r}#42, salary{r}#43]]
     *         |_Project[[_meta_field{f}#18, emp_no{f}#12, $$emp_no$converted_to$long{r$}#44, first_name{f}#13, gender{f}#14,
     *                    hire_date{f}#19, job{f}#20, job.raw{f}#21, languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17]]
     *         | \_Eval[[TOLONG(emp_no{f}#12) AS $$emp_no$converted_to$long#44]]
     *         |   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *         \_Project[[_meta_field{r}#23, emp_no{r}#7, $$emp_no$converted_to$long{r$}#45, first_name{r}#24, gender{r}#25,
     *                    hire_date{r}#26, job{r}#27, job.raw{r}#28, languages{r}#29, last_name{r}#30, long_noidx{r}#31, salary{r}#32]]
     *           \_Eval[[TOLONG(emp_no{r}#7) AS $$emp_no$converted_to$long#45]]
     *             \_Eval[[null[KEYWORD] AS _meta_field#23, null[KEYWORD] AS first_name#24, null[TEXT] AS gender#25,
     *                     null[DATETIME] AS hire_date#26, null[TEXT] AS job#27, null[KEYWORD] AS job.raw#28,
     *                     null[INTEGER] AS languages#29, null[KEYWORD] AS last_name#30, null[LONG] AS long_noidx#31,
     *                     null[INTEGER] AS salary#32]]
     *               \_Subquery[]
     *                 \_Eval[[TOINTEGER(emp_no{r}#4) AS emp_no#7]]
     *                   \_Row[[1[KEYWORD] AS emp_no#4]]
     */
    public void testMixedDataTypesInRowSubqueryWithExplicitCastingInsideAndOutside() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW emp_no = "1" | EVAL emp_no = emp_no::integer)
            | EVAL emp_no_long = emp_no::long
            | WHERE emp_no_long > 0
            """);

        Project project = as(plan, Project.class);
        Limit limit = as(project.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        ReferenceAttribute empNoLong = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("emp_no_long", empNoLong.name());
        assertEquals(LONG, empNoLong.dataType());

        // Outer Eval: emp_no_long = $$emp_no$converted_to$long (the pushed-down ref)
        Eval outerEval = as(filter.child(), Eval.class);
        assertEquals(1, outerEval.fields().size());
        Alias outerAlias = outerEval.fields().get(0);
        assertEquals("emp_no_long", outerAlias.name());
        ReferenceAttribute outerRef = as(outerAlias.child(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", outerRef.name());
        assertEquals(LONG, outerRef.dataType());

        UnionAll unionAll = as(outerEval.child(), UnionAll.class);
        Attribute empNoOut = unionAll.output().stream().filter(a -> "emp_no".equals(a.name())).findFirst().orElseThrow();
        assertEquals(INTEGER, empNoOut.dataType());
        assertEquals(2, unionAll.children().size());

        // index leg: Project → Eval[TOLONG(emp_no) AS $$converted] → EsRelation[test]
        Project indexProject = as(unionAll.children().get(0), Project.class);
        Eval indexCastEval = as(indexProject.child(), Eval.class);
        assertEquals("$$emp_no$converted_to$long", indexCastEval.fields().get(0).name());
        ToLong indexToLong = as(indexCastEval.fields().get(0).child(), ToLong.class);
        FieldAttribute indexEmpNo = as(indexToLong.field(), FieldAttribute.class);
        assertEquals("emp_no", indexEmpNo.name());
        assertEquals(INTEGER, indexEmpNo.dataType());
        EsRelation indexRelation = as(indexCastEval.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg: Project → Eval[TOLONG(emp_no) AS $$converted]
        // → Eval[10 nullEvals]
        // → Subquery → Eval[emp_no = emp_no::integer] → Row[emp_no = "1"]
        Project rowProject = as(unionAll.children().get(1), Project.class);
        Eval rowOuterCastEval = as(rowProject.child(), Eval.class);
        assertEquals("$$emp_no$converted_to$long", rowOuterCastEval.fields().get(0).name());
        as(rowOuterCastEval.fields().get(0).child(), ToLong.class);
        Eval rowMissingEval = as(rowOuterCastEval.child(), Eval.class);
        assertEquals(10, rowMissingEval.fields().size());
        Subquery rowSubquery = as(rowMissingEval.child(), Subquery.class);
        Eval rowInsideCastEval = as(rowSubquery.child(), Eval.class);
        assertEquals(1, rowInsideCastEval.fields().size());
        ToInteger toInteger = as(rowInsideCastEval.fields().get(0).child(), ToInteger.class);
        ReferenceAttribute toIntegerArg = as(toInteger.field(), ReferenceAttribute.class);
        assertEquals("emp_no", toIntegerArg.name());
        assertEquals(KEYWORD, toIntegerArg.dataType());
        Row row = as(rowInsideCastEval.child(), Row.class);
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(BytesRefs.toBytesRef("1"), rowLiteral.value());
        assertEquals(KEYWORD, rowLiteral.dataType());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[x{r}#15]]
     *   |_Project[[x{r}#7]]
     *   | \_Subquery[]
     *   |   \_Eval[[TOINTEGER(x{r}#4) AS x#7]]
     *   |     \_Row[[1[KEYWORD] AS x#4]]
     *   |_Project[[x{r}#12]]
     *   | \_Subquery[]
     *   |   \_Eval[[TOINTEGER(x{r}#9) AS x#12]]
     *   |     \_Row[[1.5[DOUBLE] AS x#9]]
     *   \_Project[[x{r}#14]]
     *     \_Subquery[]
     *       \_Row[[3[INTEGER] AS x#14]]
     */
    public void testMultipleRowSubqueriesWithExplicitCastingInside() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW x = "1" | EVAL x = x::integer)
               , (ROW x = 1.5 | EVAL x = x::integer)
               , (ROW x = 3)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // The three legs all expose `x` as INTEGER — no UnsupportedAttribute in the union output.
        Attribute xAttr = unionAll.output().stream().filter(a -> "x".equals(a.name())).findFirst().orElseThrow();
        ReferenceAttribute xRef = as(xAttr, ReferenceAttribute.class);
        assertEquals(INTEGER, xRef.dataType());

        // Leg 0: KEYWORD literal cast to INTEGER inside.
        Project leg0Project = as(unionAll.children().get(0), Project.class);
        Subquery leg0Subquery = as(leg0Project.child(), Subquery.class);
        Eval leg0Cast = as(leg0Subquery.child(), Eval.class);
        as(leg0Cast.fields().get(0).child(), ToInteger.class);
        Row leg0Row = as(leg0Cast.child(), Row.class);
        Literal leg0Literal = as(leg0Row.fields().get(0).child(), Literal.class);
        assertEquals(KEYWORD, leg0Literal.dataType());

        // Leg 1: DOUBLE literal cast to INTEGER inside.
        Project leg1Project = as(unionAll.children().get(1), Project.class);
        Subquery leg1Subquery = as(leg1Project.child(), Subquery.class);
        Eval leg1Cast = as(leg1Subquery.child(), Eval.class);
        as(leg1Cast.fields().get(0).child(), ToInteger.class);
        Row leg1Row = as(leg1Cast.child(), Row.class);
        Literal leg1Literal = as(leg1Row.fields().get(0).child(), Literal.class);
        assertEquals(DOUBLE, leg1Literal.dataType());

        // Leg 2: INTEGER literal, no cast required.
        Project leg2Project = as(unionAll.children().get(2), Project.class);
        Subquery leg2Subquery = as(leg2Project.child(), Subquery.class);
        Row leg2Row = as(leg2Subquery.child(), Row.class);
        Literal leg2Literal = as(leg2Row.fields().get(0).child(), Literal.class);
        assertEquals(INTEGER, leg2Literal.dataType());
        assertEquals(3, leg2Literal.value());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[a{r}#15, b{r}#16]]
     *   |_Project[[a{r}#4, b{r}#6]]
     *   | \_Subquery[]
     *   |   \_Row[[1[INTEGER] AS a#4, x[KEYWORD] AS b#6]]
     *   |_Project[[a{r}#8, b{r}#10]]
     *   | \_Subquery[]
     *   |   \_Row[[2[INTEGER] AS a#8, y[KEYWORD] AS b#10]]
     *   \_Project[[a{r}#12, b{r}#14]]
     *     \_Subquery[]
     *       \_Row[[3[INTEGER] AS a#12, z[KEYWORD] AS b#14]]
     */
    public void testUnionAllWithMatchingTypesFromMultipleRowSubqueries() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW a = 1, b = "x"), (ROW a = 2, b = "y"), (ROW a = 3, b = "z")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // The union output exposes both columns with their (compatible) types and no UnsupportedAttribute
        List<Attribute> output = unionAll.output();
        Attribute aAttr = output.stream().filter(attr -> "a".equals(attr.name())).findFirst().orElseThrow();
        ReferenceAttribute aRef = as(aAttr, ReferenceAttribute.class);
        assertEquals(INTEGER, aRef.dataType());
        Attribute bAttr = output.stream().filter(attr -> "b".equals(attr.name())).findFirst().orElseThrow();
        ReferenceAttribute bRef = as(bAttr, ReferenceAttribute.class);
        assertEquals(KEYWORD, bRef.dataType());

        for (int i = 0; i < 3; i++) {
            Project legProject = as(unionAll.children().get(i), Project.class);
            Subquery legSubquery = as(legProject.child(), Subquery.class);
            Row row = as(legSubquery.child(), Row.class);
            assertEquals(2, row.fields().size());
            assertEquals("a", row.fields().get(0).name());
            assertEquals("b", row.fields().get(1).name());
            Literal aLiteral = as(row.fields().get(0).child(), Literal.class);
            assertEquals(INTEGER, aLiteral.dataType());
            assertEquals(i + 1, aLiteral.value());
            Literal bLiteral = as(row.fields().get(1).child(), Literal.class);
            assertEquals(KEYWORD, bLiteral.dataType());
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[a{r}#11, b{r}#12]]
     *   |_Project[[a{r}#4, b{r}#6]]
     *   | \_Subquery[]
     *   |   \_Row[[1[INTEGER] AS a#4, [10, 20][INTEGER] AS b#6]]
     *   \_Project[[a{r}#8, b{r}#10]]
     *     \_Subquery[]
     *       \_Row[[[100, 200][INTEGER] AS a#8, 1[INTEGER] AS b#10]]
     */
    public void testTwoRowSubqueriesEachWithMixedScalarAndMultivalueFieldsMatchingTypes() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW a = 1, b = [10, 20]), (ROW a = [100, 200], b = 1)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Both fields are INTEGER on both legs — the union output carries no UnsupportedAttribute.
        Attribute aAttr = unionAll.output().stream().filter(attr -> "a".equals(attr.name())).findFirst().orElseThrow();
        assertEquals(INTEGER, as(aAttr, ReferenceAttribute.class).dataType());
        Attribute bAttr = unionAll.output().stream().filter(attr -> "b".equals(attr.name())).findFirst().orElseThrow();
        assertEquals(INTEGER, as(bAttr, ReferenceAttribute.class).dataType());

        // Leg 1: scalar a, multivalue b.
        Project leg0Project = as(unionAll.children().get(0), Project.class);
        Subquery leg0Subquery = as(leg0Project.child(), Subquery.class);
        Row leg0Row = as(leg0Subquery.child(), Row.class);
        assertEquals(2, leg0Row.fields().size());
        assertEquals("a", leg0Row.fields().get(0).name());
        Literal leg0A = as(leg0Row.fields().get(0).child(), Literal.class);
        assertEquals(INTEGER, leg0A.dataType());
        assertEquals(1, leg0A.value());
        assertEquals("b", leg0Row.fields().get(1).name());
        Literal leg0B = as(leg0Row.fields().get(1).child(), Literal.class);
        assertEquals(INTEGER, leg0B.dataType());
        assertEquals(List.of(10, 20), leg0B.value());

        // Leg 2: multivalue a, scalar b.
        Project leg1Project = as(unionAll.children().get(1), Project.class);
        Subquery leg1Subquery = as(leg1Project.child(), Subquery.class);
        Row leg1Row = as(leg1Subquery.child(), Row.class);
        assertEquals(2, leg1Row.fields().size());
        Literal leg1A = as(leg1Row.fields().get(0).child(), Literal.class);
        assertEquals(INTEGER, leg1A.dataType());
        assertEquals(List.of(100, 200), leg1A.value());
        Literal leg1B = as(leg1Row.fields().get(1).child(), Literal.class);
        assertEquals(INTEGER, leg1B.dataType());
        assertEquals(1, leg1B.value());
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
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW a = 1, b = ["cat", "dog"]), (ROW a = [1.5, 2.5], b = true)
            | KEEP a, b
            """);

        Limit limit = as(plan, Limit.class);
        Project project = as(limit.child(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(2));
        UnsupportedAttribute aUa = as(projections.get(0), UnsupportedAttribute.class);
        assertEquals("a", aUa.name());
        assertEquals(UNSUPPORTED, aUa.dataType());
        // Original types are reported in leg order: leg 0 declares INTEGER, leg 1 declares DOUBLE.
        assertThat(aUa.originalTypes(), is(List.of(INTEGER.esType(), DOUBLE.esType())));
        UnsupportedAttribute bUa = as(projections.get(1), UnsupportedAttribute.class);
        assertEquals("b", bUa.name());
        assertEquals(UNSUPPORTED, bUa.dataType());
        assertThat(bUa.originalTypes(), is(List.of(KEYWORD.esType(), BOOLEAN.esType())));

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
     * \_UnionAll[[a{r}#27, b{r}#28, c{r}#29, d{r}#30, e{r}#31, f{r}#32]]
     *   |_Project[[a{r}#4, b{r}#6, c{r}#15, d{r}#16, e{r}#17, f{r}#18]]
     *   | \_Eval[[null[KEYWORD] AS c#15, null[BOOLEAN] AS d#16, null[DOUBLE] AS e#17, null[INTEGER] AS f#18]]
     *   |   \_Subquery[]
     *   |     \_Row[[1[INTEGER] AS a#4, [10, 20, 30][INTEGER] AS b#6]]
     *   |_Project[[a{r}#19, b{r}#20, c{r}#8, d{r}#10, e{r}#21, f{r}#22]]
     *   | \_Eval[[null[INTEGER] AS a#19, null[INTEGER] AS b#20, null[DOUBLE] AS e#21, null[INTEGER] AS f#22]]
     *   |   \_Subquery[]
     *   |     \_Row[[hello[KEYWORD] AS c#8, [true, false][BOOLEAN] AS d#10]]
     *   \_Project[[a{r}#23, b{r}#24, c{r}#25, d{r}#26, e{r}#12, f{r}#14]]
     *     \_Eval[[null[INTEGER] AS a#23, null[INTEGER] AS b#24, null[KEYWORD] AS c#25, null[BOOLEAN] AS d#26]]
     *       \_Subquery[]
     *         \_Row[[[1.5, -2.5][DOUBLE] AS e#12, 100[INTEGER] AS f#14]]
     */
    public void testThreeRowSubqueriesWithDisjointFieldNamesMixedScalarAndMultivalue() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = analyzer().query("""
            FROM (ROW a = 1, b = [10, 20, 30])
               , (ROW c = "hello", d = [true, false])
               , (ROW e = [1.5, -2.5], f = 100)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // Six distinct fields, no conflicts.
        List<Attribute> output = unionAll.output();
        assertEquals(6, output.size());
        assertAttributeType(output, "a", INTEGER);
        assertAttributeType(output, "b", INTEGER);
        assertAttributeType(output, "c", KEYWORD);
        assertAttributeType(output, "d", BOOLEAN);
        assertAttributeType(output, "e", DOUBLE);
        assertAttributeType(output, "f", INTEGER);
        for (Attribute attr : output) {
            assertFalse("Unexpected UnsupportedAttribute for [" + attr.name() + "]", attr instanceof UnsupportedAttribute);
        }

        // Leg 0 (a, b) — nullEvals for c, d, e, f.
        assertRowLegWithNullEvals(unionAll.children().get(0), List.of("a", "b"), List.of(1, List.of(10, 20, 30)));
        // Leg 1 (c, d) — nullEvals for a, b, e, f.
        assertRowLegWithNullEvals(unionAll.children().get(1), List.of("c", "d"), List.of(new BytesRef("hello"), List.of(true, false)));
        // Leg 2 (e, f) — nullEvals for a, b, c, d.
        assertRowLegWithNullEvals(unionAll.children().get(2), List.of("e", "f"), List.of(List.of(1.5, -2.5), 100));
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
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().query("""
            FROM test, (ROW x = 1, y = ["cat", "dog"]), (ROW x = [1.5, -2.5], y = true)
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
        UnsupportedAttribute xUa = as(xAttr, UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, xUa.dataType());
        assertThat(xUa.originalTypes(), is(List.of(INTEGER.esType(), INTEGER.esType(), DOUBLE.esType())));
        // Same shape for y: KEYWORD null on the index leg, KEYWORD from ROW 1, BOOLEAN from ROW 2.
        Attribute yAttr = output.stream().filter(a -> "y".equals(a.name())).findFirst().orElseThrow();
        UnsupportedAttribute yUa = as(yAttr, UnsupportedAttribute.class);
        assertEquals(UNSUPPORTED, yUa.dataType());
        assertThat(yUa.originalTypes(), is(List.of(KEYWORD.esType(), KEYWORD.esType(), BOOLEAN.esType())));

        // Index leg: Project → Eval[null[KEYWORD] AS x, null[KEYWORD] AS y] ← conflict-resolution
        // → Eval[null[INTEGER] AS x, null[KEYWORD] AS y] ← missing-field fill
        // → EsRelation[test].
        Project indexProject = as(unionAll.children().get(0), Project.class);
        Eval indexConflictEval = as(indexProject.child(), Eval.class);
        assertEquals(2, indexConflictEval.fields().size());
        Eval indexMissingEval = as(indexConflictEval.child(), Eval.class);
        assertEquals(2, indexMissingEval.fields().size());
        EsRelation indexRelation = as(indexMissingEval.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

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

    // Subqueries with TS source command

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#1090, emp_no{r}#1091, first_name{r}#1092, gender{r}#1093, hire_date{r}#1094, job{r}#1095,
     *             job.raw{r}#1096, languages{r}#1097, last_name{r}#1098, long_noidx{r}#1099, salary{r}#1100, @timestamp{r}#1101,
     *             client.ip{r}#1102, cluster{r}#1103, event{r}#1104, event_city{r}#1105, event_city_boundary{r}#1106,
     *             event_location{r}#1107, event_log{r}#1108, event_shape{r}#1109, events_received{r}#1110, network.bytes_in{r}#1111,
     *             network.cost{r}#1112, network.eth0.currently_connected_clients{r}#1113, network.eth0.firmware_version{r}#1114,
     *             network.eth0.last_up{r}#1115, network.eth0.rx{r}#1116, network.eth0.tx{r}#1117, network.eth0.up{r}#1118,
     *             network.total_bytes_in{r}#1119, network.total_cost{r}#1120, pod{r}#1121]]
     *   |_Project[[_meta_field{f}#1029, emp_no{f}#1023, first_name{f}#1024, gender{f}#1025, hire_date{f}#1030, job{f}#1031,
     *              job.raw{f}#1032, languages{f}#1026, last_name{f}#1027, long_noidx{f}#1033, salary{f}#1028, @timestamp{r}#1058,
     *              client.ip{r}#1059, cluster{r}#1060, event{r}#1061, event_city{r}#1062, event_city_boundary{r}#1063,
     *              event_location{r}#1064, event_log{r}#1065, event_shape{r}#1066, events_received{r}#1067, network.bytes_in{r}#1068,
     *              network.cost{r}#1069, network.eth0.currently_connected_clients{r}#1070, network.eth0.firmware_version{r}#1071,
     *              network.eth0.last_up{r}#1072, network.eth0.rx{r}#1073, network.eth0.tx{r}#1074, network.eth0.up{r}#1075,
     *              network.total_bytes_in{r}#1076, network.total_cost{r}#1077, pod{r}#1078]]
     *   | \_Eval[[null[DATETIME] AS @timestamp#1058, null[IP] AS client.ip#1059, null[KEYWORD] AS cluster#1060,
     *             null[KEYWORD] AS event#1061, null[GEO_POINT] AS event_city#1062, null[GEO_SHAPE] AS event_city_boundary#1063,
     *             null[CARTESIAN_POINT] AS event_location#1064, null[TEXT] AS event_log#1065, null[CARTESIAN_SHAPE] AS event_shape#1066,
     *             null[LONG] AS events_received#1067, null[LONG] AS network.bytes_in#1068, null[DOUBLE] AS network.cost#1069,
     *             null[INTEGER] AS network.eth0.currently_connected_clients#1070, null[VERSION] AS network.eth0.firmware_version#1071,
     *             null[DATE_NANOS] AS network.eth0.last_up#1072, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#1073,
     *             null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#1074, null[BOOLEAN] AS network.eth0.up#1075,
     *             null[LONG] AS network.total_bytes_in#1076, null[DOUBLE] AS network.total_cost#1077, null[KEYWORD] AS pod#1078]]
     *   |   \_EsRelation[test][_meta_field{f}#1029, emp_no{f}#1023, first_name{f}#..]
     *   \_Project[[_meta_field{r}#1079, emp_no{r}#1080, first_name{r}#1081, gender{r}#1082, hire_date{r}#1083, job{r}#1084,
     *              job.raw{r}#1085, languages{r}#1086, last_name{r}#1087, long_noidx{r}#1088, salary{r}#1089, @timestamp{f}#1034,
     *              client.ip{f}#1038, cluster{f}#1035, event{f}#1039, event_city{f}#1042, event_city_boundary{f}#1043,
     *              event_location{f}#1045, event_log{f}#1040, event_shape{f}#1044, events_received{f}#1041, network.bytes_in{f}#1047,
     *              network.cost{f}#1049, network.eth0.currently_connected_clients{f}#1057, network.eth0.firmware_version{f}#1056,
     *              network.eth0.last_up{f}#1055, network.eth0.rx{f}#1054, network.eth0.tx{f}#1053, network.eth0.up{f}#1052,
     *              network.total_bytes_in{r}#1122, network.total_cost{r}#1123, pod{f}#1036]]
     *     \_Eval[[TOLONG(network.total_bytes_in{f}#1048) AS network.total_bytes_in#1122,
     *             TODOUBLE(network.total_cost{f}#1050) AS network.total_cost#1123]]
     *       \_Eval[[null[KEYWORD] AS _meta_field#1079, null[INTEGER] AS emp_no#1080, null[KEYWORD] AS first_name#1081,
     *               null[TEXT] AS gender#1082, null[DATETIME] AS hire_date#1083, null[TEXT] AS job#1084, null[KEYWORD] AS job.raw#1085,
     *               null [INTEGER] AS languages#1086, null[KEYWORD] AS last_name#1087, null[LONG] AS long_noidx#1088,
     *               null[INTEGER] AS salary#1089]]
     *         \_Subquery[]
     *           \_Filter[@timestamp{f}#1034 > 1759795200000[DATETIME]]
     *             \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#1034, client.ip{f}#1038, cluster{f}#1..]
     */
    public void testTSSubqueryInFrom() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().query("""
            FROM test, (TS k8s | WHERE @timestamp > "2025-10-07")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // 11 test fields + 21 k8s fields
        assertEquals(32, unionAll.output().size());
        assertEquals(2, unionAll.children().size());

        // Left leg (test): Project -> Eval[null evals for k8s fields] -> EsRelation[test]
        Project testProject = as(unionAll.children().get(0), Project.class);
        assertEquals(32, testProject.projections().size());
        Eval testNullEval = as(testProject.child(), Eval.class);
        // null evals for the 21 k8s fields missing in test
        assertEquals(21, testNullEval.fields().size());
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Right leg (TS k8s): Project -> Eval[counter demotions] -> Eval[null evals] -> Subquery -> Filter -> EsRelation[TIME_SERIES]
        Project tsProject = as(unionAll.children().get(1), Project.class);
        assertEquals(32, tsProject.projections().size());
        Eval counterDemotionEval = as(tsProject.child(), Eval.class);
        // network.total_bytes_in (counter_long -> long) and network.total_cost (counter_double -> double)
        assertEquals(2, counterDemotionEval.fields().size());
        Eval tsNullEval = as(counterDemotionEval.child(), Eval.class);
        // null evals for the 11 test fields missing in k8s
        assertEquals(11, tsNullEval.fields().size());
        Subquery subquery = as(tsNullEval.child(), Subquery.class);
        Filter subqueryFilter = as(subquery.child(), Filter.class);
        GreaterThan gt = as(subqueryFilter.condition(), GreaterThan.class);
        FieldAttribute ts = as(gt.left(), FieldAttribute.class);
        assertEquals("@timestamp", ts.name());
        EsRelation tsRelation = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[@timestamp{f}#2430,DESC,LAST]]]
     *   \_Filter[@timestamp{f}#2430 > 1759795200000[DATETIME]]
     *     \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#2430, client.ip{f}#2434, cluster{f}#2..]
     */
    public void testTSSubqueryInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().query("""
            FROM (TS k8s | WHERE @timestamp > "2025-10-07")
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        FieldAttribute orderField = as(order.get(0).child(), FieldAttribute.class);
        assertEquals("@timestamp", orderField.name());
        assertEquals(Order.OrderDirection.DESC, order.get(0).direction());

        Filter filter = as(orderBy.child(), Filter.class);
        GreaterThan gt = as(filter.condition(), GreaterThan.class);
        FieldAttribute ts = as(gt.left(), FieldAttribute.class);
        assertEquals("@timestamp", ts.name());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, relation.indexMode());
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#1281, emp_no{r}#1282, first_name{r}#1283, gender{r}#1284, hire_date{r}#1285, job{r}#1286,
     *             job.raw{r}#1287, languages{r}#1288, last_name{r}#1289, long_noidx{r}#1290, salary{r}#1291, m{r}#1292,cluster{r}#1293,
     *             pod{r}#1294]]
     *   |_Project[[_meta_field{f}#1237, emp_no{f}#1231, first_name{f}#1232, gender{f}#1233, hire_date{f}#1238, job{f}#1239,
     *              job.raw{f}#1240, languages{f}#1234, last_name{f}#1235, long_noidx{f}#1241, salary{f}#1236, m{r}#1267, cluster{r}#1268,
     *              pod{r}#1269]]
     *   | \_Eval[[null[DOUBLE] AS m#1267, null[KEYWORD] AS cluster#1268, null[KEYWORD] AS pod#1269]]
     *   |   \_EsRelation[test][_meta_field{f}#1237, emp_no{f}#1231, first_name{f}#..]
     *   \_Project[[_meta_field{r}#1270, emp_no{r}#1271, first_name{r}#1272, gender{r}#1273, hire_date{r}#1274, job{r}#1275,
     *              job.raw{r}#1276, languages{r}#1277, last_name{r}#1278, long_noidx{r}#1279, salary{r}#1280, m{r}#1229, cluster{f}#1243,
     *              pod{f}#1244]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#1270, null[INTEGER] AS emp_no#1271, null[KEYWORD] AS first_name#1272,
     *             null[TEXT] AS gender#1273, null[DATETIME] AS hire_date#1274, null[TEXT] AS job#1275, null[KEYWORD] AS job.raw#1276,
     *             null[INTEGER] AS languages#1277, null[KEYWORD] AS last_name#1278, null[LONG] AS long_noidx#1279,
     *             null[INTEGER] AS salary#1280]]
     *       \_Subquery[]
     *         \_Project[[m{r}#7, cluster{r}#21, pod{r}#22]]
     *          \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#77) AS cluster#21, UNPACKDIMENSION(group_pod_$1{r}#80) AS pod#22]]
     *            \_Aggregate[[pack_cluster_$1{r}#76 AS group_cluster_$1#77, pack_pod_$1{r}#79 AS group_pod_$1#80],
     *                        [MAX(RATE_$1{r}#74,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group_cluster_$1{r}#77, group_pod_$1{r}#80]]
     *              \_Eval[[PACKDIMENSION(cluster{r}#75) AS pack_cluster_$1#76, PACKDIMENSION(pod{r}#78) AS pack_pod_$1#79]]
     *                \_TimeSeriesAggregate[[_tsid{m}#73],[RATE(network.total_bytes_in{f}#34,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                       @timestamp{f}#20) AS RATE_$1#74, VALUES(cluster{f}#21,true[BOOLEAN],PT0S[TIME_DURATION])
     *                                       AS cluster#75, VALUES(pod{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS pod#78, _tsid{m}#73],
     *                                       null,null,@timestamp{f}#20,TS_COMMAND]
     *                  \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#20, client.ip{f}#24, cluster{f}#21, e..]
     */
    public void testTSSubqueryWithTimeSeriesAggregate() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().query("""
            FROM test, (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster, pod)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // 11 test fields + 3 subquery fields (cluster, pod, m)
        assertEquals(14, unionAll.output().size());
        assertEquals(2, unionAll.children().size());

        // Left leg (test): Project -> Eval[3 null evals: cluster, pod, m] -> EsRelation[test]
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        assertEquals(3, testNullEval.fields().size());
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Right leg (TS k8s | STATS): Project -> Eval[11 null evals for test fields] -> Subquery ->
        // Project[m, cluster, pod] -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TimeSeriesAggregate -> EsRelation[TIME_SERIES]
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        assertEquals(11, tsNullEval.fields().size());
        Subquery subquery = as(tsNullEval.child(), Subquery.class);
        Project tsInnerProject = as(subquery.child(), Project.class);
        Eval unpackEval = as(tsInnerProject.child(), Eval.class);
        // STATS BY cluster, pod -> 2 UNPACK evals
        assertEquals(2, unpackEval.fields().size());
        Aggregate outerAggregate = as(unpackEval.child(), Aggregate.class);
        assertFalse(outerAggregate instanceof TimeSeriesAggregate);
        assertEquals(2, outerAggregate.groupings().size());
        Eval packEval = as(outerAggregate.child(), Eval.class);
        assertEquals(2, packEval.fields().size());
        TimeSeriesAggregate tsAggregate = as(packEval.child(), TimeSeriesAggregate.class);
        // Inner TimeSeriesAggregate groups only by _tsid
        assertEquals(1, tsAggregate.groupings().size());
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#2034, emp_no{r}#2035, first_name{r}#2036, gender{r}#2037, hire_date{r}#2038, job{r}#2039,
     *             job.raw{r}#2040, languages{r}#2041, last_name{r}#2042, long_noidx{r}#2043, salary{r}#2044, rate{r}#2045,
     *             cluster{r}#2046, cnt{r}#2047]]
     *   |_Project[[_meta_field{f}#1972, emp_no{f}#1966, first_name{f}#1967, gender{f}#1968, hire_date{f}#1973, job{f}#1974,
     *              job.raw{f}#1975, languages{f}#1969, last_name{f}#1970, long_noidx{f}#1976, salary{f}#1971, rate{r}#2006,
     *              cluster{r}#2007, cnt{r}#2008]]
     *   | \_Eval[[null[DOUBLE] AS rate#2006, null[KEYWORD] AS cluster#2007, null[LONG] AS cnt#2008]]
     *   |   \_EsRelation[test][_meta_field{f}#1972, emp_no{f}#1966, first_name{f}#..]
     *   |_Project[[_meta_field{r}#2009, emp_no{r}#2010, first_name{r}#2011, gender{r}#2012, hire_date{r}#2013, job{r}#2014,
     *              job.raw{r}#2015, languages{r}#2016, last_name{r}#2017, long_noidx{r}#2018, salary{r}#2019, rate{r}#1962,
     *              cluster{f}#1978, cnt{r}#2020]]
     *   | \_Eval[[null[KEYWORD] AS _meta_field#2009, null[INTEGER] AS emp_no#2010, null[KEYWORD] AS first_name#2011,
     *             null[TEXT] AS gender#2012, null[DATETIME] AS hire_date#2013, null[TEXT] AS job#2014, null[KEYWORD] AS job.raw#2015,
     *             null [INTEGER] AS languages#2016, null[KEYWORD] AS last_name#2017, null[LONG] AS long_noidx#2018,
     *             null[INTEGER] AS salary#2019, null[LONG] AS cnt#2020]]
     *   |   \_Subquery[]
     *   |     \_Project[[rate{r}#6, cluster{r}#22]]
     *   |       \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#96) AS cluster#22]]
     *   |         \_Aggregate[[pack_cluster_$1{r}#95 AS group_cluster_$1#96],[MAX(RATE_$1{r}#93,true[BOOLEAN],PT0S[TIME_DURATION])
     *                          AS rate#6, group_cluster_$1{r}#96]]
     *   |           \_Eval[[PACKDIMENSION(cluster{r}#94) AS pack_cluster_$1#95]]
     *   |             \_TimeSeriesAggregate[[_tsid{m}#92],[RATE(network.total_bytes_in{f}#35,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                        @timestamp{f}#21) AS RATE_$1#93, VALUES(cluster{f}#22,true[BOOLEAN],PT0S[TIME_DURATION])
     *                                        AS cluster#94, _tsid{m}#92],null,null,@timestamp{f}#21,TS_COMMAND]
     *   |               \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#21, client.ip{f}#25, cluster{f}#22, e..]
     *   \_Project[[_meta_field{r}#2021, emp_no{r}#2022, first_name{r}#2023, gender{r}#2024, hire_date{r}#2025, job{r}#2026,
     *              job.raw{r}#2027, languages{r}#2028, last_name{r}#2029, long_noidx{r}#2030, salary{r}#2031, rate{r}#2032,
     *              cluster{r}#2033, cnt{r}#1965]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#2021, null[INTEGER] AS emp_no#2022, null[KEYWORD] AS first_name#2023,
     *             null[TEXT] AS gender#2024, null[DATETIME] AS hire_date#2025, null[TEXT] AS job#2026, null[KEYWORD] AS job.raw#2027,
     *             null[INTEGER] AS languages#2028, null[KEYWORD] AS last_name#2029, null[LONG] AS long_noidx#2030,
     *             null[INTEGER] AS salary#2031, null[DOUBLE] AS rate#2032, null[KEYWORD] AS cluster#2033]]
     *       \_Subquery[]
     *         \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS cnt#1965]]
     *           \_EsRelation[sample_data][@timestamp{f}#2001, client_ip{f}#2002, event_durati..]
     */
    public void testMultipleSubqueriesInFromWithTS() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test,
              (TS k8s | STATS rate = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | STATS cnt = count(*))
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // Branch 0: main FROM test
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Branch 1: TS k8s subquery with TimeSeriesAggregate (Project -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TsAggregate)
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        Project tsInnerProject = as(tsSubquery.child(), Project.class);
        Eval tsUnpackEval = as(tsInnerProject.child(), Eval.class);
        Aggregate tsOuterAggregate = as(tsUnpackEval.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        Eval tsPackEval = as(tsOuterAggregate.child(), Eval.class);
        TimeSeriesAggregate tsAggregate = as(tsPackEval.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 2: FROM sample_data subquery with regular Aggregate
        Project sampleProject = as(unionAll.children().get(2), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        Aggregate sampleAggregate = as(sampleSubquery.child(), Aggregate.class);
        assertFalse(sampleAggregate instanceof TimeSeriesAggregate);
        EsRelation sampleRelation = as(sampleAggregate.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#..., emp_no{r}#..., ..., rate{r}#..., cluster{r}#..., cnt{r}#..., synthetic{r}#...]]
     *   |_Project[[..., rate{r}#..., cluster{r}#..., cnt{r}#..., synthetic{r}#...]]
     *   | \_Eval[[null[DOUBLE] AS rate#..., null[KEYWORD] AS cluster#..., null[LONG] AS cnt#..., null[INTEGER] AS synthetic#...]]
     *   |   \_EsRelation[test][_meta_field{f}#..., emp_no{f}#..., first_name{f}#..]
     *   |_Project[[..., rate{r}#..., cluster{f}#..., cnt{r}#..., synthetic{r}#...]]
     *   | \_Eval[[..., null[LONG] AS cnt#..., null[INTEGER] AS synthetic#...]]
     *   |   \_Subquery[]
     *   |     \_Project[[rate{r}#, cluster{r}#]]
     *   |       \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#) AS cluster#]]
     *   |         \_Aggregate[[...,[MAX(RATE_$1{r}#,true[BOOLEAN],PT0S[TIME_DURATION]) AS rate#,...]]
     *   |           \_Eval[[PACKDIMENSION(cluster{r}#114) AS pack_cluster_$1#]]
     *   |             \_TimeSeriesAggregate[[...]]
     *   |               \_EsRelation[k8s][TIME_SERIES][...]
     *   |_Project[[..., rate{r}#..., cluster{r}#..., cnt{r}#..., synthetic{r}#...]]
     *   | \_Eval[[..., null[DOUBLE] AS rate#..., null[KEYWORD] AS cluster#..., null[INTEGER] AS synthetic#...]]
     *   |   \_Subquery[]
     *   |     \_Aggregate[[],[COUNT(*) AS cnt]]
     *   |       \_EsRelation[sample_data][...]
     *   \_Project[[..., rate{r}#..., cluster{r}#..., cnt{r}#..., synthetic{r}#...]]
     *     \_Eval[[..., null[DOUBLE] AS rate#..., null[KEYWORD] AS cluster#..., null[LONG] AS cnt#...]]
     *       \_Subquery[]
     *         \_Row[[1[INTEGER] AS synthetic]]
     */
    public void testMultipleSubqueriesInFromWithMixedTsRowAndFromSubqueries() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with ROW source support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test,
              (TS k8s | STATS rate = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | STATS cnt = count(*)),
              (ROW synthetic = 1)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        // Branch 0: main FROM test
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, testRelation.indexMode());

        // Branch 1: TS k8s subquery with TimeSeriesAggregate (Project -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TsAggregate)
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        Project tsInnerProject = as(tsSubquery.child(), Project.class);
        Eval tsUnpackEval = as(tsInnerProject.child(), Eval.class);
        Aggregate tsOuterAggregate = as(tsUnpackEval.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        Eval tsPackEval = as(tsOuterAggregate.child(), Eval.class);
        TimeSeriesAggregate tsAggregate = as(tsPackEval.child(), TimeSeriesAggregate.class);
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 2: FROM sample_data subquery with regular Aggregate
        Project sampleProject = as(unionAll.children().get(2), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        Aggregate sampleAggregate = as(sampleSubquery.child(), Aggregate.class);
        assertFalse(sampleAggregate instanceof TimeSeriesAggregate);
        EsRelation sampleRelation = as(sampleAggregate.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());

        // Branch 3: ROW subquery wrapping a Row leaf
        Project rowProject = as(unionAll.children().get(3), Project.class);
        Eval rowNullEval = as(rowProject.child(), Eval.class);
        Subquery rowSubquery = as(rowNullEval.child(), Subquery.class);
        Row row = as(rowSubquery.child(), Row.class);
        assertEquals(1, row.fields().size());
        assertEquals("synthetic", row.fields().get(0).name());
        Literal rowLiteral = as(row.fields().get(0).child(), Literal.class);
        assertEquals(1, rowLiteral.value());
        assertEquals(INTEGER, rowLiteral.dataType());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#573, emp_no{r}#574, first_name{r}#575, gender{r}#576, hire_date{r}#577, job{r}#578, job.raw{r}#579,
     *             languages{r}#580, last_name{r}#581, long_noidx{r}#582, salary{r}#583, cluster{r}#584, pod{r}#585, doubled{r}#586]]
     *   |_Project[[_meta_field{f}#530, emp_no{f}#524, first_name{f}#525, gender{f}#526, hire_date{f}#531, job{f}#532, job.raw{f}#533,
     *              languages{f}#527, last_name{f}#528, long_noidx{f}#534, salary{f}#529, cluster{r}#559, pod{r}#560, doubled{r}#561]]
     *   | \_Eval[[null[KEYWORD] AS cluster#559, null[KEYWORD] AS pod#560, null[DOUBLE] AS doubled#561]]
     *   |   \_EsRelation[test][_meta_field{f}#530, emp_no{f}#524, first_name{f}#52..]
     *   \_Project[[_meta_field{r}#562, emp_no{r}#563, first_name{r}#564, gender{r}#565, hire_date{r}#566, job{r}#567, job.raw{r}#568,
     *              languages{r}#569, last_name{r}#570, long_noidx{r}#571, salary{r}#572, cluster{f}#536, pod{f}#537, doubled{r}#523]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#562, null[INTEGER] AS emp_no#563, null[KEYWORD] AS first_name#564,
     *             null[TEXT] AS gender#565, null[DATETIME] AS hire_date#566, null[TEXT] AS job#567, null[KEYWORD] AS job.raw#568,
     *             null[INTEGER] AS languages#569, null[KEYWORD] AS last_name#570, null[LONG] AS long_noidx#571,
     *             null[INTEGER] AS salary#572]]
     *       \_Subquery[]
     *         \_Project[[cluster{f}#536, pod{f}#537, doubled{r}#523]]
     *           \_Eval[[network.cost{f}#550 * 2[INTEGER] AS doubled#523]]
     *             \_Filter[@timestamp{f}#535 > 1759795200000[DATETIME]]
     *               \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#535, client.ip{f}#539, cluster{f}#536, ..]
     */
    public void testTSSubqueryWithProcessingCommands() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().query("""
            FROM test, (TS k8s
                        | WHERE @timestamp > "2025-10-07"
                        | EVAL doubled = network.cost * 2
                        | KEEP cluster, pod, doubled)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // 11 test fields + 3 subquery fields (cluster, pod, doubled)
        assertEquals(14, unionAll.output().size());
        assertEquals(2, unionAll.children().size());

        // Left leg (test)
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        assertEquals(3, testNullEval.fields().size());
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());

        // Right leg (TS k8s | WHERE | EVAL | KEEP)
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        assertEquals(11, tsNullEval.fields().size());
        Subquery subquery = as(tsNullEval.child(), Subquery.class);
        Project keepProject = as(subquery.child(), Project.class);
        // KEEP projects 3 fields
        assertEquals(3, keepProject.projections().size());
        Eval evalNode = as(keepProject.child(), Eval.class);
        assertEquals(1, evalNode.fields().size());
        Alias doubledAlias = evalNode.fields().get(0);
        assertEquals("doubled", doubledAlias.name());
        Filter subqueryFilter = as(evalNode.child(), Filter.class);
        EsRelation tsRelation = as(subqueryFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /*
     * Project[[_meta_field{r}#125, emp_no{r}#126, first_name{r}#127, gender{r}#128, hire_date{r}#129, job{r}#130, job.raw{r}#131,
     *          languages{r}#132, last_name{r}#133, long_noidx{r}#134, salary{r}#135, @timestamp{r}#136, client_ip{r}#137,
     *          event_duration{r}#138, message{r}#139, client.ip{r}#140, cluster{r}#141, event{r}#142, event_city{r}#143,
     *          event_city_boundary{r}#144, event_location{r}#145, event_log{r}#146, event_shape{r}#147, events_received{r}#148,
     *          network.bytes_in{r}#149, network.cost{r}#150, network.eth0.currently_connected_clients{r}#151,
     *          network.eth0.firmware_version{r}#152, network.eth0.last_up{r}#153, network.eth0.rx{r}#154, network.eth0.tx{r}#155,
     *          network.eth0.up{r}#156, network.total_bytes_in{r}#157, network.total_cost{r}#158, pod{r}#159]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_UnionAll[[_meta_field{r}#125, emp_no{r}#126, first_name{r}#127, gender{r}#128, hire_date{r}#129, job{r}#130, job.raw{r}#131,
     *               languages{r}#132, last_name{r}#133, long_noidx{r}#134, salary{r}#135, @timestamp{r}#136, client_ip{r}#137,
     *               event_duration{r}#138, message{r}#139, client.ip{r}#140, cluster{r}#141, event{r}#142, event_city{r}#143,
     *               event_city_boundary{r}#144, event_location{r}#145, event_log{r}#146, event_shape{r}#147, events_received{r}#148,
     *               network.bytes_in{r}#149, network.cost{r}#150, network.eth0.currently_connected_clients{r}#151,
     *               network.eth0.firmware_version{r}#152, network.eth0.last_up{r}#153, network.eth0.rx{r}#154, network.eth0.tx{r}#155,
     *               network.eth0.up{r}#156, network.total_bytes_in{r}#157, network.total_cost{r}#158, pod{r}#159,
     *               $$network.total_bytes_in$converted_to$long{r$}#172, $$network.total_cost$converted_to$double{r$}#173]]
     *     |_Project[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13,
     *                languages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9, @timestamp{r}#90, client_ip{r}#91,
     *                event_duration{r}#92, message{r}#93, client.ip{r}#94, cluster{r}#95, event{r}#96, event_city{r}#97,
     *                event_city_boundary{r}#98, event_location{r}#99, event_log{r}#100, event_shape{r}#101, events_received{r}#102,
     *                network.bytes_in{r}#103, network.cost{r}#104, network.eth0.currently_connected_clients{r}#105,
     *                network.eth0.firmware_version{r}#106, network.eth0.last_up{r}#107, network.eth0.rx{r}#108, network.eth0.tx{r}#109,
     *                network.eth0.up{r}#110, network.total_bytes_in{r}#111, network.total_cost{r}#112, pod{r}#113,
     *                $$network.total_bytes_in$converted_to$long{r}#170,$$network.total_cost$converted_to$double{r}#171]]
     *     | \_Eval[[null[LONG] AS $$network.total_bytes_in$converted_to$long#170,
     *               null[DOUBLE] AS $$network.total_cost$converted_to$double#171]]
     *     |   \_Project[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13,
     *                    languages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9, @timestamp{r}#90, client_ip{r}#91,
     *                    event_duration{r}#92, message{r}#93, client.ip{r}#94, cluster{r}#95, event{r}#96, event_city{r}#97,
     *                    event_city_boundary{r}#98, event_location{r}#99, event_log{r}#100, event_shape{r}#101, events_received{r}#102,
     *                    network.bytes_in{r}#103, network.cost{r}#104, network.eth0.currently_connected_clients{r}#105,
     *                    network.eth0.firmware_version{r}#106, network.eth0.last_up{r}#107, network.eth0.rx{r}#108,
     *                    network.eth0.tx{r}#109, network.eth0.up{r}#110, network.total_bytes_in{r}#111, network.total_cost{r}#112,
     *                    pod{r}#113]]
     *     |     \_Eval[[null[DATETIME] AS @timestamp#90, null[IP] AS client_ip#91, null[LONG] AS event_duration#92,
     *                   null[KEYWORD] AS message#93, null[IP] AS client.ip#94, null[KEYWORD] AS cluster#95, null[KEYWORD] AS event#96,
     *                   null[GEO_POINT] AS event_city#97, null[GEO_SHAPE] AS event_city_boundary#98,
     *                   null[CARTESIAN_POINT] AS event_location#99, null[TEXT] AS event_log#100, null[CARTESIAN_SHAPE] AS event_shape#101,
     *                   null[LONG] AS events_received#102, null[LONG] AS network.bytes_in#103, null[DOUBLE] AS network.cost#104,
     *                   null[INTEGER] AS network.eth0.currently_connected_clients#105, null[VERSION] AS network.eth0.firmware_version#106,
     *                   null[DATE_NANOS] AS network.eth0.last_up#107, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#108,
     *                   null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#109, null[BOOLEAN] AS network.eth0.up#110,
     *                   null[LONG] AS network.total_bytes_in#111, null[DOUBLE] AS network.total_cost#112, null[KEYWORD] AS pod#113]]
     *     |       \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     *     \_Project[[_meta_field{r}#114, emp_no{r}#115, first_name{r}#116, gender{r}#117, hire_date{r}#118, job{r}#119, job.raw{r}#120,
     *                languages{r}#121, last_name{r}#122, long_noidx{r}#123, salary{r}#124, @timestamp{r}#66, client_ip{r}#67,
     *                event_duration{r}#68, message{r}#69, client.ip{r}#70, cluster{r}#71, event{r}#72, event_city{r}#73,
     *                event_city_boundary{r}#74, event_location{r}#75, event_log{r}#76, event_shape{r}#77, events_received{r}#78,
     *                network.bytes_in{r}#79, network.cost{r}#80, network.eth0.currently_connected_clients{r}#81,
     *                network.eth0.firmware_version{r}#82, network.eth0.last_up{r}#83, network.eth0.rx{r}#84, network.eth0.tx{r}#85,
     *                network.eth0.up{r}#86, network.total_bytes_in{r}#162, network.total_cost{r}#163, pod{r}#89,
     *                $$network.total_bytes_in$converted_to$long{r$}#168, $$network.total_cost$converted_to$double{r$}#169]]
     *       \_Eval[[$$network.total_bytes_in$converted_to$long{r$}#168 AS network.total_bytes_in#162,
     *               $$network.total_cost$converted_to$double{r$}#169 AS network.total_cost#163]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#114, null[INTEGER] AS emp_no#115, null[KEYWORD] AS first_name#116,
     *                 null[TEXT] AS gender#117, null[DATETIME] AS hire_date#118, null[TEXT] AS job#119, null[KEYWORD] AS job.raw#120,
     *                 null[INTEGER] AS languages#121, null[KEYWORD] AS last_name#122, null[LONG] AS long_noidx#123,
     *                 null[INTEGER] AS salary#124]]
     *           \_Subquery[]
     *             \_UnionAll[[@timestamp{r}#66, client_ip{r}#67, event_duration{r}#68, message{r}#69, client.ip{r}#70, cluster{r}#71,
     *                         event{r}#72, event_city{r}#73, event_city_boundary{r}#74, event_location{r}#75, event_log{r}#76,
     *                         event_shape{r}#77, events_received{r}#78, network.bytes_in{r}#79, network.cost{r}#80,
     *                         network.eth0.currently_connected_clients{r}#81, network.eth0.firmware_version{r}#82,
     *                         network.eth0.last_up{r}#83, network.eth0.rx{r}#84, network.eth0.tx{r}#85, network.eth0.up{r}#86,
     *                         network.total_bytes_in{r}#87, $$network.total_bytes_in$converted_to$long{r$}#168, network.total_cost{r}#88,
     *                         $$network.total_cost$converted_to$double{r$}#169, pod{r}#89]]
     *               |_Project[[@timestamp{f}#15, client_ip{f}#16, event_duration{f}#17, message{f}#18, client.ip{r}#43, cluster{r}#44,
     *                          event{r}#45, event_city{r}#46, event_city_boundary{r}#47, event_location{r}#48, event_log{r}#49,
     *                          event_shape{r}#50, events_received{r}#51, network.bytes_in{r}#52, network.cost{r}#53,
     *                          network.eth0.currently_connected_clients{r}#54, network.eth0.firmware_version{r}#55,
     *                          network.eth0.last_up{r}#56, network.eth0.rx{r}#57, network.eth0.tx{r}#58, network.eth0.up{r}#59,
     *                          network.total_bytes_in{r}#60, $$network.total_bytes_in$converted_to$long{r$}#164, network.total_cost{r}#61,
     *                          $$network.total_cost$converted_to$double{r$}#165, pod{r}#62]]
     *               | \_Eval[[TOLONG(network.total_bytes_in{r}#60) AS $$network.total_bytes_in$converted_to$long#164,
     *                         TODOUBLE(network.total_cost{r}#61) AS $$network.total_cost$converted_to$double#165]]
     *               |   \_Eval[[null[IP] AS client.ip#43, null[KEYWORD] AS cluster#44, null[KEYWORD] AS event#45,
     *                           null[GEO_POINT] AS event_city#46, null[GEO_SHAPE] AS event_city_boundary#47,
     *                           null[CARTESIAN_POINT] AS event_location#48, null[TEXT] AS event_log#49,
     *                           null[CARTESIAN_SHAPE] AS event_shape#50, null[LONG] AS events_received#51,
     *                           null[LONG] AS network.bytes_in#52, null[DOUBLE] AS network.cost#53,
     *                           null[INTEGER] AS network.eth0.currently_connected_clients#54,
     *                           null[VERSION] AS network.eth0.firmware_version#55, null[DATE_NANOS] AS network.eth0.last_up#56,
     *                           null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#57, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#58,
     *                           null[BOOLEAN] AS network.eth0.up#59, null[LONG] AS network.total_bytes_in#60,
     *                           null[DOUBLE] AS network.total_cost#61, null[KEYWORD] AS pod#62]]
     *               |     \_EsRelation[sample_data][@timestamp{f}#15, client_ip{f}#16, event_duration{f..]
     *               \_Project[[@timestamp{f}#19, client_ip{r}#63, event_duration{r}#64, message{r}#65, client.ip{f}#23, cluster{f}#20,
     *                          event{f}#24, event_city{f}#27, event_city_boundary{f}#28, event_location{f}#30, event_log{f}#25,
     *                          event_shape{f}#29, events_received{f}#26, network.bytes_in{f}#32, network.cost{f}#34,
     *                          network.eth0.currently_connected_clients{f}#42, network.eth0.firmware_version{f}#41,
     *                          network.eth0.last_up{f}#40, network.eth0.rx{f}#39, network.eth0.tx{f}#38, network.eth0.up{f}#37,
     *                          network.total_bytes_in{r}#160, $$network.total_bytes_in$converted_to$long{r$}#166,
     *                          network.total_cost{r}#161, $$network.total_cost$converted_to$double{r$}#167, pod{f}#21]]
     *                 \_Eval[[TOLONG(network.total_bytes_in{r}#160) AS $$network.total_bytes_in$converted_to$long#166,
     *                         TODOUBLE(network.total_cost{r}#161) AS $$network.total_cost$converted_to$double#167]]
     *                   \_Eval[[TOLONG(network.total_bytes_in{f}#33) AS network.total_bytes_in#160,
     *                           TODOUBLE(network.total_cost{f}#35) AS network.total_cost#161]]
     *                     \_Eval[[null[IP] AS client_ip#63, null[LONG] AS event_duration#64, null[KEYWORD] AS message#65]]
     *                       \_Subquery[]
     *                         \_Filter[@timestamp{f}#19 > 1759795200000[DATETIME]]
     *                           \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#19, client.ip{f}#23, cluster{f}#20, e..]
     */
    public void testNestedTSSubqueryInFrom() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test, (FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07"))
            """);

        Project project = as(plan, Project.class);
        Limit limit = as(project.child(), Limit.class);
        UnionAll outerUnion = as(limit.child(), UnionAll.class);
        assertEquals(2, outerUnion.children().size());

        // Left leg: test
        Project testProject = as(outerUnion.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        testProject = as(testNullEval.child(), Project.class);
        testNullEval = as(testProject.child(), Eval.class);
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());

        // Right leg: outer FROM subquery wrapping (sample_data union TS k8s)
        // Because the inner UnionAll surfaces counter-typed fields, the outer right leg adds a
        // counter-rename Eval on top of the null-eval before the Subquery wrapper.
        Project rightProject = as(outerUnion.children().get(1), Project.class);
        Eval rightCounterRename = as(rightProject.child(), Eval.class);
        assertEquals(2, rightCounterRename.fields().size());
        Eval rightNullEval = as(rightCounterRename.child(), Eval.class);
        // 11 null evals for the missing test fields
        assertEquals(11, rightNullEval.fields().size());
        Subquery outerSubquery = as(rightNullEval.child(), Subquery.class);
        UnionAll innerUnion = as(outerSubquery.child(), UnionAll.class);
        assertEquals(2, innerUnion.children().size());

        // Inner branch 0: sample_data — needs counter demotions for fields surfaced by the TS branch
        Project sampleProject = as(innerUnion.children().get(0), Project.class);
        Eval sampleCounterEval = as(sampleProject.child(), Eval.class);
        // TOLONG/TODOUBLE for network.total_bytes_in, network.total_cost
        assertEquals(2, sampleCounterEval.fields().size());
        Eval sampleNullEval = as(sampleCounterEval.child(), Eval.class);
        EsRelation sampleRelation = as(sampleNullEval.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());

        // Inner branch 1: TS k8s subquery — two stacked counter demotions (outer rename + inner cast),
        // followed by null evals for sample_data fields, then the Subquery wrapping the TS relation.
        Project tsProject = as(innerUnion.children().get(1), Project.class);
        Eval tsOuterRename = as(tsProject.child(), Eval.class);
        assertEquals(2, tsOuterRename.fields().size());
        Eval tsInnerCast = as(tsOuterRename.child(), Eval.class);
        assertEquals(2, tsInnerCast.fields().size());
        Eval tsNullEval = as(tsInnerCast.child(), Eval.class);
        Subquery innerTSSubquery = as(tsNullEval.child(), Subquery.class);
        Filter tsFilter = as(innerTSSubquery.child(), Filter.class);
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#55, client.ip{r}#56, cluster{r}#57, event{r}#58, event_city{r}#59, event_city_boundary{r}#60,
     *             event_location{r}#61, event_log{r}#62, event_shape{r}#63, events_received{r}#64, network.bytes_in{r}#65,
     *             network.cost{r}#66, network.eth0.currently_connected_clients{r}#67, network.eth0.firmware_version{r}#68,
     *             network.eth0.last_up{r}#69, network.eth0.rx{r}#70, network.eth0.tx{r}#71, network.eth0.up{r}#72,
     *             network.total_bytes_in{r}#73, network.total_cost{r}#74, pod{r}#75, client_ip{r}#76, event_duration{r}#77,
     *             message{r}#78]]
     *   |_Project[[@timestamp{f}#4, client.ip{f}#8, cluster{f}#5, event{f}#9, event_city{f}#12, event_city_boundary{f}#13,
     *              event_location{f}#15, event_log{f}#10, event_shape{f}#14, events_received{f}#11, network.bytes_in{f}#17,
     *              network.cost{f}#19, network.eth0.currently_connected_clients{f}#27, network.eth0.firmware_version{f}#26,
     *              network.eth0.last_up{f}#25, network.eth0.rx{f}#24, network.eth0.tx{f}#23, network.eth0.up{f}#22,
     *              network.total_bytes_in{r}#79, network.total_cost{r}#80, pod{f}#6, client_ip{r}#32, event_duration{r}#33,
     *              message{r}#34]]
     *   | \_Eval[[TOLONG(network.total_bytes_in{f}#18) AS network.total_bytes_in#79,
     *             TODOUBLE(network.total_cost{f}#20) AS network.total_cost#80]]
     *   |   \_Eval[[null[IP] AS client_ip#32, null[LONG] AS event_duration#33, null[KEYWORD] AS message#34]]
     *   |     \_Subquery[]
     *   |       \_Filter[@timestamp{f}#4 > 1759795200000[DATETIME]]
     *   |         \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#4, client.ip{f}#8, cluster{f}#5, even..]
     *   \_Project[[@timestamp{f}#28, client.ip{r}#35, cluster{r}#36, event{r}#37, event_city{r}#38, event_city_boundary{r}#39,
     *              event_location{r}#40, event_log{r}#41, event_shape{r}#42, events_received{r}#43, network.bytes_in{r}#44,
     *              network.cost{r}#45, network.eth0.currently_connected_clients{r}#46, network.eth0.firmware_version{r}#47,
     *              network.eth0.last_up{r}#48, network.eth0.rx{r}#49, network.eth0.tx{r}#50, network.eth0.up{r}#51,
     *              network.total_bytes_in{r}#52, network.total_cost{r}#53, pod{r}#54, client_ip{f}#29, event_duration{f}#30,
     *              message{f}#31]]
     *     \_Eval[[null[IP] AS client.ip#35, null[KEYWORD] AS cluster#36, null[KEYWORD] AS event#37, null[GEO_POINT] AS event_city#38,
     *             null[GEO_SHAPE] AS event_city_boundary#39, null[CARTESIAN_POINT] AS event_location#40, null[TEXT] AS event_log#41,
     *             null[CARTESIAN_SHAPE] AS event_shape#42, null[LONG] AS events_received#43, null[LONG] AS network.bytes_in#44,
     *             null[DOUBLE] AS network.cost#45, null[INTEGER] AS network.eth0.currently_connected_clients#46,
     *             null[VERSION] AS network.eth0.firmware_version#47, null[DATE_NANOS] AS network.eth0.last_up#48,
     *             null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#49, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#50,
     *             null[BOOLEAN] AS network.eth0.up#51, null[LONG] AS network.total_bytes_in#52, null[DOUBLE] AS network.total_cost#53,
     *             null[KEYWORD] AS pod#54]]
     *       \_Subquery[]
     *         \_EsRelation[sample_data][@timestamp{f}#28, client_ip{f}#29, event_duration{f..]
     */
    public void testTSAndFromSubqueriesInFromWithoutMainIndexPattern() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | WHERE @timestamp > "2025-10-07"), (FROM sample_data)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 0: TS k8s subquery
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsCounterDemotionEval = as(tsProject.child(), Eval.class);
        // counter_long and counter_double fields demoted to long/double
        assertEquals(2, tsCounterDemotionEval.fields().size());
        Eval tsNullEval = as(tsCounterDemotionEval.child(), Eval.class);
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data subquery
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        EsRelation sampleRelation = as(sampleSubquery.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_UnionAll[[m{r}#45, WITHOUT(pod){r}#46, @timestamp{r}#47, client_ip{r}#48, event_duration{r}#49, message{r}#50]]
     *   |_Project[[m{r}#7, WITHOUT(pod){r}#4, @timestamp{r}#39, client_ip{r}#40, event_duration{r}#41, message{r}#42]]
     *   | \_Eval[[null[DATETIME] AS @timestamp#39, null[IP] AS client_ip#40,
     *             null[LONG] AS event_duration#41, null[KEYWORD] AS message#42]]
     *   |   \_Subquery[]
     *   |     \_Project[[m{r}#7, _timeseries{r}#4]]
     *   |       \_Eval[[UNPACKDIMENSION(group__timeseries_$1{r}#55) AS _timeseries#4]]
     *   |         \_Aggregate[[pack__timeseries_$1{r}#54 AS group__timeseries_$1#55],
     *                         [MAX(RATE_$1{r}#52,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group__timeseries_$1{r}#55]]
     *   |           \_Eval[[PACKDIMENSION(_timeseries{r}#53) AS pack__timeseries_$1#54]]
     *   |             \_TimeSeriesAggregate[[_tsid{m}#51],[RATE(network.total_bytes_in{f}#24,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                       @timestamp{f}#10) AS RATE_$1#52,VALUES(_timeseries{f}#4,true[BOOLEAN],PT0S[TIME_DURATION])
     *                                       AS _timeseries#53,_tsid{m}#51],null,null,@timestamp{f}#10,TS_COMMAND]
     *   |               \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#10, client.ip{f}#14, cluster{f}#11, e..]
     *   \_Project[[m{r}#43, WITHOUT(pod){r}#44, @timestamp{f}#34, client_ip{f}#35, event_duration{f}#36, message{f}#37]]
     *     \_Eval[[null[DOUBLE] AS m#43, null[KEYWORD] AS WITHOUT(pod)#44]]
     *       \_Subquery[]
     *         \_EsRelation[sample_data][@timestamp{f}#34, client_ip{f}#35, event_duration{f..]
     */
    public void testTSSubqueryWithByWithoutAndFromSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires ESQL_WITHOUT_GROUPING", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());

        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY WITHOUT(pod)),
              (FROM sample_data)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // The UnionAll output combines the TS aggregate columns (m, WITHOUT(pod)) with the
        // sample_data fields (@timestamp, client_ip, event_duration, message).
        List<? extends NamedExpression> unionOutput = unionAll.output();
        assertEquals(6, unionOutput.size());
        Attribute mAttr = (Attribute) unionOutput.get(0);
        assertEquals("m", mAttr.name());
        as(mAttr, ReferenceAttribute.class);
        Attribute withoutAttr = (Attribute) unionOutput.get(1);
        assertEquals("WITHOUT(pod)", withoutAttr.name());
        as(withoutAttr, ReferenceAttribute.class);

        // Branch 0: TS subquery with BY WITHOUT(pod)
        Project tsProject = as(unionAll.children().get(0), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        assertEquals(4, tsNullEval.fields().size());
        // Each null eval corresponds to a missing sample_data field.
        for (Alias nullAlias : tsNullEval.fields()) {
            as(nullAlias.child(), Literal.class);
        }
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        // BY WITHOUT(pod) is now expanded: Project -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TimeSeriesAggregate
        Project tsInnerProject = as(tsSubquery.child(), Project.class);
        Eval tsUnpackEval = as(tsInnerProject.child(), Eval.class);
        assertEquals(1, tsUnpackEval.fields().size());
        Aggregate tsOuterAggregate = as(tsUnpackEval.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        assertEquals(1, tsOuterAggregate.groupings().size());
        Eval tsPackEval = as(tsOuterAggregate.child(), Eval.class);
        assertEquals(1, tsPackEval.fields().size());
        TimeSeriesAggregate tsAggregate = as(tsPackEval.child(), TimeSeriesAggregate.class);
        // Inner TimeSeriesAggregate groups only by _tsid
        assertEquals(1, tsAggregate.groupings().size());

        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 1: FROM sample_data — produces null Evals for m (DOUBLE) and WITHOUT(pod) (KEYWORD).
        Project sampleProject = as(unionAll.children().get(1), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        assertEquals(2, sampleNullEval.fields().size());
        Alias sampleMNull = sampleNullEval.fields().get(0);
        assertEquals("m", sampleMNull.name());
        Alias sampleWithoutNull = sampleNullEval.fields().get(1);
        assertEquals("WITHOUT(pod)", sampleWithoutNull.name());
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        EsRelation sampleRelation = as(sampleSubquery.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#1376, emp_no{r}#1377, first_name{r}#1378, gender{r}#1379, hire_date{r}#1380, job{r}#1381,
     *             job.raw{r}#1382, languages{r}#1383, last_name{r}#1384, long_noidx{r}#1385, salary{r}#1386, m{r}#1387,
     *             WITHOUT(pod){r}#1388, @timestamp{r}#1389, client_ip{r}#1390, event_duration{r}#1391, message{r}#1392]]
     *   |_Project[[_meta_field{f}#1308, emp_no{f}#1302, first_name{f}#1303, gender{f}#1304, hire_date{f}#1309, job{f}#1310,
     *              job.raw{f}#1311, languages{f}#1305, last_name{f}#1306, long_noidx{f}#1312, salary{f}#1307, m{r}#1342,
     *              WITHOUT(pod){r}#1343, @timestamp{r}#1344, client_ip{r}#1345, event_duration{r}#1346, message{r}#1347]]
     *   | \_Eval[[null[DOUBLE] AS m#1342, null[KEYWORD] AS WITHOUT(pod)#1343, null[DATETIME] AS @timestamp#1344,
     *             null[IP] AS client_ip#1345, null[LONG] AS event_duration#1346, null[KEYWORD] AS message#1347]]
     *   |   \_EsRelation[test][_meta_field{f}#1308, emp_no{f}#1302, first_name{f}#..]
     *   |_Project[[_meta_field{r}#1348, emp_no{r}#1349, first_name{r}#1350, gender{r}#1351, hire_date{r}#1352, job{r}#1353,
     *              job.raw{r}#1354, languages{r}#1355, last_name{r}#1356, long_noidx{r}#1357, salary{r}#1358, m{r}#1299,
     *              WITHOUT(pod){r}#1296, @timestamp{r}#1359, client_ip{r}#1360, event_duration{r}#1361, message{r}#1362]]
     *   | \_Eval[[null[KEYWORD] AS _meta_field#1348, null[INTEGER] AS emp_no#1349, null[KEYWORD] AS first_name#1350,
     *             null[TEXT] AS gender#1351, null[DATETIME] AS hire_date#1352, null[TEXT] AS job#1353, null[KEYWORD] AS job.raw#1354,
     *             null [INTEGER] AS languages#1355, null[KEYWORD] AS last_name#1356, null[LONG] AS long_noidx#1357,
     *             null[INTEGER] AS salary#1358, null[DATETIME] AS @timestamp#1359, null[IP] AS client_ip#1360,
     *             null[LONG] AS event_duration#1361,null[KEYWORD] AS message#1362]]
     *   |   \_Subquery[]
     *   |     \_Project[[m{r}#7, _timeseries{r}#4]]
     *   |       \_Eval[[UNPACKDIMENSION(group__timeseries_$1{r}#105) AS _timeseries#4]]
     *   |         \_Aggregate[[pack__timeseries_$1{r}#104 AS group__timeseries_$1#105],
     *                         [MAX(RATE_$1{r}#102,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group__timeseries_$1{r}#105]]
     *   |           \_Eval[[PACKDIMENSION(_timeseries{r}#103) AS pack__timeseries_$1#104]]
     *   |             \_TimeSeriesAggregate[[_tsid{m}#101],[RATE(network.total_bytes_in{f}#35,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                       @timestamp{f}#21) AS RATE_$1#102, VALUES(_timeseries{f}#4,true[BOOLEAN],PT0S[TIME_DURATION])
     *                                       AS _timeseries#103, _tsid{m}#101],null,null,@timestamp{f}#21,TS_COMMAND]
     *   |               \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#21, client.ip{f}#25, cluster{f}#22, e..]
     *   \_Project[[_meta_field{r}#1363, emp_no{r}#1364, first_name{r}#1365, gender{r}#1366, hire_date{r}#1367, job{r}#1368,
     *              job.raw{r}#1369, languages{r}#1370, last_name{r}#1371, long_noidx{r}#1372, salary{r}#1373, m{r}#1374,
     *              WITHOUT(pod){r}#1375, @timestamp{f}#1337, client_ip{f}#1338, event_duration{f}#1339, message{f}#1340]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#1363, null[INTEGER] AS emp_no#1364, null[KEYWORD] AS first_name#1365,
     *             null[TEXT] AS gender#1366, null[DATETIME] AS hire_date#1367, null[TEXT] AS job#1368, null[KEYWORD] AS job.raw#1369,
     *             null[INTEGER] AS languages#1370, null[KEYWORD] AS last_name#1371, null[LONG] AS long_noidx#1372,
     *             null[INTEGER] AS salary#1373, null[DOUBLE] AS m#1374, null[KEYWORD] AS WITHOUT(pod)#1375]]
     *       \_Subquery[]
     *         \_EsRelation[sample_data][@timestamp{f}#1337, client_ip{f}#1338, event_durati..]
     */
    public void testTSSubqueryWithByWithoutInFromCommand() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires ESQL_WITHOUT_GROUPING", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());

        LogicalPlan plan = basic().addK8sDownsampled().addSampleData().query("""
            FROM test,
              (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY WITHOUT(pod)),
              (FROM sample_data)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // The UnionAll output spans all branches: 11 test fields + m + WITHOUT(pod) + 4 sample_data fields.
        List<? extends NamedExpression> unionOutput = unionAll.output();
        assertEquals(17, unionOutput.size());
        // Locate the WITHOUT(pod) attribute in the union output.
        Attribute unionWithout = (Attribute) unionOutput.stream()
            .filter(a -> a.name().equals("WITHOUT(pod)"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("UnionAll output must contain WITHOUT(pod) attribute"));
        as(unionWithout, ReferenceAttribute.class);
        Attribute unionM = (Attribute) unionOutput.stream()
            .filter(a -> a.name().equals("m"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("UnionAll output must contain m attribute"));
        as(unionM, ReferenceAttribute.class);

        // Branch 0: main FROM test — null Evals for the missing aggregate + sample_data columns.
        Project testProject = as(unionAll.children().get(0), Project.class);
        Eval testNullEval = as(testProject.child(), Eval.class);
        // 6 nulls: m, WITHOUT(pod), @timestamp, client_ip, event_duration, message
        assertEquals(6, testNullEval.fields().size());
        assertTrue(
            "test branch must materialize WITHOUT(pod) as a null reference",
            testNullEval.fields().stream().anyMatch(a -> a.name().equals("WITHOUT(pod)"))
        );
        EsRelation testRelation = as(testNullEval.child(), EsRelation.class);
        assertEquals("test", testRelation.indexPattern());

        // Branch 1: TS subquery with BY WITHOUT(pod).
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsNullEval = as(tsProject.child(), Eval.class);
        // null evals for the 11 test fields + 4 sample_data fields = 15.
        assertEquals(15, tsNullEval.fields().size());
        // Verify the TS branch itself does not emit a null for WITHOUT(pod) — it owns that column.
        assertTrue(
            "TS branch must not null-eval WITHOUT(pod)",
            tsNullEval.fields().stream().noneMatch(a -> a.name().equals("WITHOUT(pod)"))
        );
        Subquery tsSubquery = as(tsNullEval.child(), Subquery.class);
        // BY WITHOUT(pod) is now expanded: Project -> Eval[UNPACK] -> Aggregate -> Eval[PACK] -> TimeSeriesAggregate
        Project tsInnerProject = as(tsSubquery.child(), Project.class);
        Eval tsUnpackEval = as(tsInnerProject.child(), Eval.class);
        assertEquals(1, tsUnpackEval.fields().size());
        Aggregate tsOuterAggregate = as(tsUnpackEval.child(), Aggregate.class);
        assertFalse(tsOuterAggregate instanceof TimeSeriesAggregate);
        assertEquals(1, tsOuterAggregate.groupings().size());
        Eval tsPackEval = as(tsOuterAggregate.child(), Eval.class);
        TimeSeriesAggregate tsAggregate = as(tsPackEval.child(), TimeSeriesAggregate.class);
        assertEquals(1, tsAggregate.groupings().size());
        EsRelation tsRelation = as(tsAggregate.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelation.indexMode());

        // Branch 2: FROM sample_data — null Evals for the 11 test fields + m + WITHOUT(pod) = 13.
        Project sampleProject = as(unionAll.children().get(2), Project.class);
        Eval sampleNullEval = as(sampleProject.child(), Eval.class);
        assertEquals(13, sampleNullEval.fields().size());
        assertTrue(
            "sample_data branch must materialize WITHOUT(pod) as a null reference",
            sampleNullEval.fields().stream().anyMatch(a -> a.name().equals("WITHOUT(pod)"))
        );
        Subquery sampleSubquery = as(sampleNullEval.child(), Subquery.class);
        EsRelation sampleRelation = as(sampleSubquery.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        assertEquals(IndexMode.STANDARD, sampleRelation.indexMode());
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
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

        LogicalPlan plan = analyzer().addK8sDownsampled().addSampleData().query("""
            FROM (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster),
              (FROM sample_data | EVAL m = "abc")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        List<Attribute> output = unionAll.output();
        assertEquals(6, output.size());
        UnsupportedAttribute mUnsupported = as(output.get(0), UnsupportedAttribute.class);
        assertEquals("m", mUnsupported.name());
        assertEquals(UNSUPPORTED, mUnsupported.dataType());
        assertThat(mUnsupported.originalTypes(), hasSize(2));
        assertThat(mUnsupported.originalTypes(), is(List.of(DOUBLE.esType(), KEYWORD.esType())));

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
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

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
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());

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
        UnsupportedAttribute mNoCast = as(noCastOutput.get(0), UnsupportedAttribute.class);
        assertEquals("m", mNoCast.name());
        assertEquals(UNSUPPORTED, mNoCast.dataType());
        assertThat(mNoCast.originalTypes(), is(List.of(LONG.esType(), DOUBLE.esType())));

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
     * Limit[..]
     * \_UnionAll[[name{r}#..]]
     *   |_Project[..]                                                         (regular index branch)
     *   | \_..
     *   |   \_EsRelation[sample_data][..]                                      IndexMode.STANDARD
     *   |_Project[..]                                                         (TS k8s rate branch)
     *   | \_..(lowered time-series aggregation: PACKDIMENSION/UNPACKDIMENSION around a TimeSeriesAggregate)..
     *   |   \_EsRelation[k8s][TIME_SERIES][..]                                 rate keeps it TIME_SERIES
     *   \_Project[..]                                                         (external dataset branch)
     *     \_Subquery[]
     *       \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][..]
     */
    public void testSubqueryUnionOfIndexTimeSeriesRateAndExternalDataset() {
        assumeTrue("Requires TS source inside a FROM subquery", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeExternalDatasetSubquery("""
            FROM (FROM sample_data | EVAL name = message | KEEP name),
                 (TS k8s | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster | EVAL name = cluster | KEEP name),
                 (FROM salaries_int | KEEP name)
            """);

        as(plan, Limit.class);

        List<UnionAll> unionAlls = new ArrayList<>();
        plan.forEachDown(UnionAll.class, unionAlls::add);
        assertThat(unionAlls, hasSize(1));
        UnionAll unionAll = unionAlls.getFirst();
        assertEquals(3, unionAll.children().size());
        assertThat(unionAll.output(), hasSize(1));
        assertEquals("name", unionAll.output().getFirst().name());
        assertEquals(KEYWORD, unionAll.output().getFirst().dataType());

        // Branch 0: regular index, read as a standard EsRelation.
        List<EsRelation> indexRelations = new ArrayList<>();
        unionAll.children().get(0).forEachDown(EsRelation.class, indexRelations::add);
        assertThat(indexRelations, hasSize(1));
        assertEquals("sample_data", indexRelations.getFirst().indexPattern());
        assertEquals(IndexMode.STANDARD, indexRelations.getFirst().indexMode());

        // Branch 1: TS k8s with a rate aggregation; the rate keeps the k8s source relation in IndexMode.TIME_SERIES.
        List<TimeSeriesAggregate> tsAggregates = new ArrayList<>();
        unionAll.children().get(1).forEachDown(TimeSeriesAggregate.class, tsAggregates::add);
        assertThat(tsAggregates, hasSize(1));
        List<EsRelation> tsRelations = new ArrayList<>();
        unionAll.children().get(1).forEachDown(EsRelation.class, tsRelations::add);
        assertThat(tsRelations, hasSize(1));
        assertEquals("k8s", tsRelations.getFirst().indexPattern());
        assertEquals(IndexMode.TIME_SERIES, tsRelations.getFirst().indexMode());

        // Branch 2: external (blob-backed) dataset, read as an ExternalRelation.
        List<ExternalRelation> externalRelations = new ArrayList<>();
        unionAll.children().get(2).forEachDown(ExternalRelation.class, externalRelations::add);
        assertThat(externalRelations, hasSize(1));
        assertEquals(SALARIES_INT_RESOURCE, externalRelations.getFirst().sourcePath());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[x{r}#6]]
     *   \_Project[[emp_no{r}#35 AS x#6]]
     *     \_Project[[emp_no{r}#35]]
     *       \_UnionAll[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40,
     *                   languages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{r}#45, language_name{r}#46]]
     *         |_Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *                    languages{f}#11, last_name{f}#12, long_noidx{f}#18, salary{f}#13, language_code{r}#21, language_name{r}#22]]
     *         | \_Eval[[null[INTEGER] AS language_code#21, null[KEYWORD] AS language_name#22]]
     *         |   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *         \_Project[[_meta_field{r}#23, emp_no{r}#24, first_name{r}#25, gender{r}#26, hire_date{r}#27, job{r}#28, job.raw{r}#29,
     *                    languages{r}#30, last_name{r}#31, long_noidx{r}#32, salary{r}#33, language_code{f}#19, language_name{f}#20]]
     *           \_Eval[[null[KEYWORD] AS _meta_field#23, null[INTEGER] AS emp_no#24, null[KEYWORD] AS first_name#25,
     *                   null[TEXT] AS gender#26, null[DATETIME] AS hire_date#27, null[TEXT] AS job#28, null[KEYWORD] AS job.raw#29,
     *                   null[INTEGER] AS languages#30, null[KEYWORD] AS last_name#31, null[LONG] AS long_noidx#32,
     *                   null[INTEGER] AS salary#33]]
     *             \_Subquery[]
     *               \_EsRelation[languages][language_code{f}#19, language_name{f}#20]
     */
    public void testSubqueryRenameKeepStarOnMissingColumnPreservesType() {
        LogicalPlan plan = basic().addLanguages().query("""
            FROM test, (FROM languages)
            | KEEP emp_no
            | RENAME emp_no AS x
            | KEEP *
            """);

        Limit limit = as(plan, Limit.class);
        Project project = as(limit.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute x = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("x", x.name());
        assertEquals(INTEGER, x.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(INTEGER, xAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute emp_no = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("emp_no", emp_no.name());
        assertEquals(INTEGER, emp_no.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "emp_no".equals(a.name()) && INTEGER.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[x{r}#7,ASC,LAST]]]
     *   \_Project[[x{r}#7, y{r}#10]]
     *     \_Project[[network.total_bytes_in{r}#79 AS x#7, network.eth0.tx{r}#77 AS y#10]]
     *       \_Project[[network.total_bytes_in{r}#79, network.eth0.tx{r}#77]]
     *         \_UnionAll[[@timestamp{r}#61, client.ip{r}#62, cluster{r}#63, event{r}#64, event_city{r}#65, event_city_boundary{r}#66,
     *                   event_location{r}#67, event_log{r}#68, event_shape{r}#69, events_received{r}#70, network.bytes_in{r}#71,
     *                   network.cost{r}#72, network.eth0.currently_connected_clients{r}#73, network.eth0.firmware_version{r}#74,
     *                   network.eth0.last_up{r}#75, network.eth0.rx{r}#76, network.eth0.tx{r}#77, network.eth0.up{r}#78,
     *                   network.total_bytes_in{r}#79, network.total_cost{r}#80, pod{r}#81, language_code{r}#82, language_name{r}#83]]
     *         |_Project[[@timestamp{f}#12, client.ip{f}#16, cluster{f}#13, event{f}#17, event_city{f}#20, event_city_boundary{f}#21,
     *                    event_location{f}#23, event_log{f}#18, event_shape{f}#22, events_received{f}#19, network.bytes_in{f}#25,
     *                    network.cost{f}#27, network.eth0.currently_connected_clients{f}#35, network.eth0.firmware_version{f}#34,
     *                    network.eth0.last_up{f}#33, network.eth0.rx{f}#32, network.eth0.tx{f}#31, network.eth0.up{f}#30,
     *                    network.total_bytes_in{r}#84, network.total_cost{r}#85, pod{f}#14, language_code{r}#38, language_name{r}#39]]
     *         | \_Eval[[TOLONG(network.total_bytes_in{f}#26) AS network.total_bytes_in#84,
     *                   TODOUBLE(network.total_cost{f}#28) AS network.total_cost#85]]
     *         |   \_Eval[[null[INTEGER] AS language_code#38, null[KEYWORD] AS language_name#39]]
     *         |     \_EsRelation[k8s][@timestamp{f}#12, client.ip{f}#16, cluster{f}#13, e..]
     *         \_Project[[@timestamp{r}#40, client.ip{r}#41, cluster{r}#42, event{r}#43, event_city{r}#44, event_city_boundary{r}#45,
     *                    event_location{r}#46, event_log{r}#47, event_shape{r}#48, events_received{r}#49, network.bytes_in{r}#50,
     *                    network.cost{r}#51, network.eth0.currently_connected_clients{r}#52, network.eth0.firmware_version{r}#53,
     *                    network.eth0.last_up{r}#54, network.eth0.rx{r}#55, network.eth0.tx{r}#56, network.eth0.up{r}#57,
     *                    network.total_bytes_in{r}#58, network.total_cost{r}#59, pod{r}#60, language_code{f}#36, language_name{f}#37]]
     *           \_Eval[[null[DATETIME] AS @timestamp#40, null[IP] AS client.ip#41, null[KEYWORD] AS cluster#42,
     *                   null[KEYWORD] AS event#43, null[GEO_POINT] AS event_city#44, null[GEO_SHAPE] AS event_city_boundary#45,
     *                   null[CARTESIAN_POINT] AS event_location#46, null[TEXT] AS event_log#47, null[CARTESIAN_SHAPE] AS event_shape#48,
     *                   null[LONG] AS events_received#49, null[LONG] AS network.bytes_in#50, null[DOUBLE] AS network.cost#51,
     *                   null[INTEGER] AS network.eth0.currently_connected_clients#52, null[VERSION] AS network.eth0.firmware_version#53,
     *                   null[DATE_NANOS] AS network.eth0.last_up#54, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#55,
     *                   null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#56, null[BOOLEAN] AS network.eth0.up#57,
     *                   null[LONG] AS network.total_bytes_in#58, null[DOUBLE] AS network.total_cost#59, null[KEYWORD] AS pod#60]]
     *             \_Subquery[]
     *               \_EsRelation[languages][language_code{f}#36, language_name{f}#37]
     */
    public void testSubqueryRenameKeepOnMissingCounterFields() {
        assumeTrue(
            "Require the fix to inconsistent counter type",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME.isEnabled()
        );
        LogicalPlan plan = analyzer().addK8sDownsampled().addLanguages().query("""
            FROM k8s, (FROM languages)
            | KEEP network.total_bytes_in, network.eth0.tx
            | RENAME network.total_bytes_in AS x, network.eth0.tx AS y
            | KEEP *
            | SORT x
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute xOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("x", xOrder.name());
        assertEquals(LONG, xOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(2, projections.size());
        ReferenceAttribute x = as(projections.get(0), ReferenceAttribute.class);
        ReferenceAttribute y = as(projections.get(1), ReferenceAttribute.class);
        assertEquals("x", x.name());
        assertEquals(LONG, x.dataType());
        assertEquals("y", y.name());
        assertEquals(AGGREGATE_METRIC_DOUBLE, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(2, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(LONG, xAlias.dataType());
        Alias yAlias = as(projections.get(1), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(AGGREGATE_METRIC_DOUBLE, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(2, projections.size());
        ReferenceAttribute total_bytes_in = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("network.total_bytes_in", total_bytes_in.name());
        assertEquals(LONG, total_bytes_in.dataType());
        ReferenceAttribute network_eth0_tx = as(projections.get(1), ReferenceAttribute.class);
        assertEquals("network.eth0.tx", network_eth0_tx.name());
        assertEquals(AGGREGATE_METRIC_DOUBLE, network_eth0_tx.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "network.total_bytes_in".equals(a.name()) && LONG.equals(a.dataType())));
        assertTrue(
            unionAll.output().stream().anyMatch(a -> "network.eth0.tx".equals(a.name()) && AGGREGATE_METRIC_DOUBLE.equals(a.dataType()))
        );
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[y{r}#9,ASC,LAST]]]
     *  \_Project[[y{r}#9]]
     *   \_Project[[network.total_bytes_in{r}#78 AS y#9]]
     *     \_Project[[network.total_bytes_in{r}#78]]
     *       \_UnionAll[[@timestamp{r}#60, client.ip{r}#61, cluster{r}#62, event{r}#63, event_city{r}#64, event_city_boundary{r}#65,
     *                   event_location{r}#66, event_log{r}#67, event_shape{r}#68, events_received{r}#69, network.bytes_in{r}#70,
     *                   network.cost{r}#71, network.eth0.currently_connected_clients{r}#72, network.eth0.firmware_version{r}#73,
     *                   network.eth0.last_up{r}#74, network.eth0.rx{r}#75, network.eth0.tx{r}#76, network.eth0.up{r}#77,
     *                   network.total_bytes_in{r}#78, network.total_cost{r}#79, pod{r}#80, language_code{r}#81, language_name{r}#82]]
     *         |_Project[[@timestamp{f}#11, client.ip{f}#15, cluster{f}#12, event{f}#16, event_city{f}#19, event_city_boundary{f}#20,
     *                    event_location{f}#22, event_log{f}#17, event_shape{f}#21, events_received{f}#18, network.bytes_in{f}#24,
     *                    network.cost{f}#26, network.eth0.currently_connected_clients{f}#34, network.eth0.firmware_version{f}#33,
     *                    network.eth0.last_up{f}#32, network.eth0.rx{f}#31, network.eth0.tx{f}#30, network.eth0.up{f}#29,
     *                    network.total_bytes_in{r}#83, network.total_cost{r}#84, pod{f}#13, language_code{r}#37, language_name{r}#38]]
     *         | \_Eval[[TOLONG(network.total_bytes_in{f}#25) AS network.total_bytes_in#83,
     *                   TODOUBLE(network.total_cost{f}#27) AS network.total_cost#84]]
     *         |   \_Eval[[null[INTEGER] AS language_code#37, null[KEYWORD] AS language_name#38]]
     *         |     \_EsRelation[k8s][@timestamp{f}#11, client.ip{f}#15, cluster{f}#12, e..]
     *         \_Project[[@timestamp{r}#39, client.ip{r}#40, cluster{r}#41, event{r}#42, event_city{r}#43, event_city_boundary{r}#44,
     *         event_location{r}#45, event_log{r}#46, event_shape{r}#47, events_received{r}#48, network.bytes_in{r}#49,
     *         network.cost{r}#50, network.eth0.currently_connected_clients{r}#51, network.eth0.firmware_version{r}#52,
     *         network.eth0.last_up{r}#53, network.eth0.rx{r}#54, network.eth0.tx{r}#55, network.eth0.up{r}#56,
     *         network.total_bytes_in{r}#57, network.total_cost{r}#58, pod{r}#59, language_code{f}#35, language_name{f}#36]]
     *           \_Eval[[null[DATETIME] AS @timestamp#39, null[IP] AS client.ip#40, null[KEYWORD] AS cluster#41, null[KEYWORD] AS event#42,
     *                   null[GEO_POINT] AS event_city#43, null[GEO_SHAPE] AS event_city_boundary#44,
     *                   null[CARTESIAN_POINT] AS event_location#45, null[TEXT] AS event_log#46, null[CARTESIAN_SHAPE] AS event_shape#47,
     *                   null[LONG] AS events_received#48, null[LONG] AS network.bytes_in#49, null[DOUBLE] AS network.cost#50,
     *                   null[INTEGER] AS network.eth0.currently_connected_clients#51, null[VERSION] AS network.eth0.firmware_version#52,
     *                   null[DATE_NANOS] AS network.eth0.last_up#53, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#54,
     *                   null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#55, null[BOOLEAN] AS network.eth0.up#56,
     *                   null[LONG] AS network.total_bytes_in#57, null[DOUBLE] AS network.total_cost#58, null[KEYWORD] AS pod#59]]
     *             \_Subquery[]
     *               \_EsRelation[languages][language_code{f}#35, language_name{f}#36]
     */
    public void testSubqueryRenameChainKeepStarOnMissingCounterField() {
        assumeTrue(
            "Require the fix to inconsistent counter type",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME.isEnabled()
        );
        LogicalPlan plan = analyzer().addK8sDownsampled().addLanguages().query("""
            FROM k8s, (FROM languages)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x, x as y
            | KEEP y
            | SORT y
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute yOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(LONG, yOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute y = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("y", y.name());
        assertEquals(LONG, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias yAlias = as(projections.get(0), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(LONG, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute total_bytes_in = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("network.total_bytes_in", total_bytes_in.name());
        assertEquals(LONG, total_bytes_in.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "network.total_bytes_in".equals(a.name()) && LONG.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[y{r}#9,ASC,LAST]]]
     *  \_Project[[y{r}#9]]
     *   \_Project[[x{r}#6 AS y#9]]
     *     \_Project[[network.total_bytes_in{r}#78 AS x#6]]
     *       \_Project[[network.total_bytes_in{r}#78]]
     *         \_UnionAll[[@timestamp{r}#60, client.ip{r}#61, cluster{r}#62, event{r}#63, event_city{r}#64, event_city_boundary{r}#65,
     *                     event_location{r}#66, event_log{r}#67, event_shape{r}#68, events_received{r}#69, network.bytes_in{r}#70,
     *                     network.cost{r}#71, network.eth0.currently_connected_clients{r}#72, network.eth0.firmware_version{r}#73,
     *                     network.eth0.last_up{r}#74, network.eth0.rx{r}#75, network.eth0.tx{r}#76, network.eth0.up{r}#77,
     *                     network.total_bytes_in{r}#78, network.total_cost{r}#79, pod{r}#80, language_code{r}#81, language_name{r}#82]]
     *           |_Project[[@timestamp{f}#11, client.ip{f}#15, cluster{f}#12, event{f}#16, event_city{f}#19, event_city_boundary{f}#20,
     *                      event_location{f}#22, event_log{f}#17, event_shape{f}#21, events_received{f}#18, network.bytes_in{f}#24,
     *                      network.cost{f}#26, network.eth0.currently_connected_clients{f}#34, network.eth0.firmware_version{f}#33,
     *                      network.eth0.last_up{f}#32, network.eth0.rx{f}#31, network.eth0.tx{f}#30, network.eth0.up{f}#29,
     *                      network.total_bytes_in{r}#83, network.total_cost{r}#84, pod{f}#13, language_code{r}#37, language_name{r}#38]]
     *           | \_Eval[[TOLONG(network.total_bytes_in{f}#25) AS network.total_bytes_in#83,
     *                     TODOUBLE(network.total_cost{f}#27) AS network.total_cost#84]]
     *           |   \_Eval[[null[INTEGER] AS language_code#37, null[KEYWORD] AS language_name#38]]
     *           |     \_EsRelation[k8s][@timestamp{f}#11, client.ip{f}#15, cluster{f}#12, e..]
     *           \_Project[[@timestamp{r}#39, client.ip{r}#40, cluster{r}#41, event{r}#42, event_city{r}#43, event_city_boundary{r}#44,
     *                      event_location{r}#45, event_log{r}#46, event_shape{r}#47, events_received{r}#48, network.bytes_in{r}#49,
     *                      network.cost{r}#50, network.eth0.currently_connected_clients{r}#51, network.eth0.firmware_version{r}#52,
     *                      network.eth0.last_up{r}#53, network.eth0.rx{r}#54, network.eth0.tx{r}#55, network.eth0.up{r}#56,
     *                      network.total_bytes_in{r}#57, network.total_cost{r}#58, pod{r}#59, language_code{f}#35, language_name{f}#36]]
     *             \_Eval[[null[DATETIME] AS @timestamp#39, null[IP] AS client.ip#40, null[KEYWORD] AS cluster#41,
     *                     null[KEYWORD] AS event#42, null[GEO_POINT] AS event_city#43, null[GEO_SHAPE] AS event_city_boundary#44,
     *                     null[CARTESIAN_POINT] AS event_location#45, null[TEXT] AS event_log#46, null[CARTESIAN_SHAPE] AS event_shape#47,
     *                     null[LONG] AS events_received#48, null[LONG] AS network.bytes_in#49, null[DOUBLE] AS network.cost#50,
     *                     null[INTEGER] AS network.eth0.currently_connected_clients#51, null[VERSION] AS network.eth0.firmware_version#52,
     *                     null[DATE_NANOS] AS network.eth0.last_up#53, null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.rx#54,
     *                     null[AGGREGATE_METRIC_DOUBLE] AS network.eth0.tx#55, null[BOOLEAN] AS network.eth0.up#56,
     *                     null[LONG] AS network.total_bytes_in#57, null[DOUBLE] AS network.total_cost#58, null[KEYWORD] AS pod#59]]
     *               \_Subquery[]
     *                 \_EsRelation[languages][language_code{f}#35, language_name{f}#36]
     */
    public void testSubqueryDoubleRenameKeepStarOnMissingCounterField() {
        assumeTrue(
            "Require the fix to inconsistent counter type",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME.isEnabled()
        );
        LogicalPlan plan = analyzer().addK8sDownsampled().addLanguages().query("""
            FROM k8s, (FROM languages)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x
            | RENAME x as y
            | KEEP *
            | SORT y
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute yOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(LONG, yOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute y = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("y", y.name());
        assertEquals(LONG, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias yAlias = as(projections.get(0), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(LONG, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(LONG, xAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute total_bytes_in = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("network.total_bytes_in", total_bytes_in.name());
        assertEquals(LONG, total_bytes_in.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "network.total_bytes_in".equals(a.name()) && LONG.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[x{r}#7,ASC,LAST]]]
     *   \_Project[[x{r}#7, y{r}#10]]
     *     \_Project[[network.total_bytes_in{r}#79 AS x#7, network.eth0.tx{r}#77 AS y#10]]
     *       \_Project[[network.total_bytes_in{r}#79, network.eth0.tx{r}#77]]
     *         \_UnionAll[...]
     *           |_...
     *           \_...
     *
     * Same as {@link #testSubqueryRenameKeepOnMissingCounterFields()} but with {@code SET unmapped_fields="nullify"}
     */
    public void testSubqueryRenameKeepOnMissingCounterFieldsWithNullifyAndSort() {
        assumeTrue(
            "Require the fix to inconsistent counter type",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME.isEnabled()
        );
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().addLanguages().statement("""
            SET unmapped_fields="nullify";
            FROM k8s, (FROM languages)
            | KEEP network.total_bytes_in, network.eth0.tx
            | RENAME network.total_bytes_in AS x, network.eth0.tx AS y
            | KEEP *
            | SORT x
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute xOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("x", xOrder.name());
        assertEquals(LONG, xOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(2, projections.size());
        ReferenceAttribute x = as(projections.get(0), ReferenceAttribute.class);
        ReferenceAttribute y = as(projections.get(1), ReferenceAttribute.class);
        assertEquals("x", x.name());
        assertEquals(LONG, x.dataType());
        assertEquals("y", y.name());
        assertEquals(AGGREGATE_METRIC_DOUBLE, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(2, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(LONG, xAlias.dataType());
        Alias yAlias = as(projections.get(1), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(AGGREGATE_METRIC_DOUBLE, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(2, projections.size());
        ReferenceAttribute total_bytes_in = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("network.total_bytes_in", total_bytes_in.name());
        assertEquals(LONG, total_bytes_in.dataType());
        ReferenceAttribute network_eth0_tx = as(projections.get(1), ReferenceAttribute.class);
        assertEquals("network.eth0.tx", network_eth0_tx.name());
        assertEquals(AGGREGATE_METRIC_DOUBLE, network_eth0_tx.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "network.total_bytes_in".equals(a.name()) && LONG.equals(a.dataType())));
        assertTrue(
            unionAll.output().stream().anyMatch(a -> "network.eth0.tx".equals(a.name()) && AGGREGATE_METRIC_DOUBLE.equals(a.dataType()))
        );
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[y{r}#9,ASC,LAST]]]
     *   \_Project[[y{r}#9]]
     *     \_Project[[network.total_bytes_in{r}#78 AS y#9]]
     *       \_Project[[network.total_bytes_in{r}#78]]
     *         \_UnionAll[...]
     *
     * Same as {@link #testSubqueryRenameChainKeepStarOnMissingCounterField()} but with {@code SET unmapped_fields="nullify"}
     */
    public void testSubqueryRenameChainKeepStarOnMissingCounterFieldWithNullifyAndSort() {
        assumeTrue(
            "Require the fix to inconsistent counter type",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME.isEnabled()
        );
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().addLanguages().statement("""
            SET unmapped_fields="nullify";
            FROM k8s, (FROM languages)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x, x as y
            | KEEP y
            | SORT y
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute yOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(LONG, yOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute y = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("y", y.name());
        assertEquals(LONG, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias yAlias = as(projections.get(0), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(LONG, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute total_bytes_in = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("network.total_bytes_in", total_bytes_in.name());
        assertEquals(LONG, total_bytes_in.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "network.total_bytes_in".equals(a.name()) && LONG.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[y{r}#?,ASC,LAST]]]
     *   \_Project[[<remaining fields>, x{r}#? AS y#?]]
     *     \_Project[[<remaining fields>, network.total_bytes_in{r}#? AS x#?]]
     *       \_Project[[<remaining fields after DROP>]]
     *         \_UnionAll[...]
     *
     * DROP variant of {@link #testSubqueryDoubleRenameKeepStarOnMissingCounterFieldWithNullifyAndSort()}.
     */
    public void testSubqueryDoubleRenameDropStarOnMissingCounterFieldWithNullifyAndSort() {
        assumeTrue(
            "Require the fix to inconsistent counter type",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_UNION_TYPES_IMPLICIT_CASTING_INCONSISTENT_AFTER_RENAME.isEnabled()
        );
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        LogicalPlan plan = analyzer().addK8sDownsampled().addLanguages().statement("""
            SET unmapped_fields="nullify";
            FROM k8s, (FROM languages)
            | DROP @timestamp, language_*
            | RENAME network.total_bytes_in AS x
            | RENAME x as y
            | KEEP *
            | SORT y
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute yOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(LONG, yOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        ReferenceAttribute y = as(
            projections.stream().filter(p -> "y".equals(p.name())).findFirst().orElseThrow(),
            ReferenceAttribute.class
        );
        assertEquals(LONG, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        Alias yAlias = as(projections.stream().filter(p -> "y".equals(p.name())).findFirst().orElseThrow(), Alias.class);
        assertEquals(LONG, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        Alias xAlias = as(projections.stream().filter(p -> "x".equals(p.name())).findFirst().orElseThrow(), Alias.class);
        assertEquals(LONG, xAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        ReferenceAttribute totalBytesIn = as(
            projections.stream().filter(p -> "network.total_bytes_in".equals(p.name())).findFirst().orElseThrow(),
            ReferenceAttribute.class
        );
        assertEquals(LONG, totalBytesIn.dataType());
        assertTrue("@timestamp should have been dropped", projections.stream().noneMatch(p -> "@timestamp".equals(p.name())));
        assertTrue("language_code should have been dropped", projections.stream().noneMatch(p -> "language_code".equals(p.name())));
        assertTrue("language_name should have been dropped", projections.stream().noneMatch(p -> "language_name".equals(p.name())));
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "network.total_bytes_in".equals(a.name()) && LONG.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[x{r}#?,ASC,LAST]]]
     *   \_Project[[x{r}#?]]
     *     \_Project[[@timestamp{r}#? AS x#?]]
     *       \_Project[[@timestamp{r}#?]]
     *         \_UnionAll[[@timestamp{r}#?, client_ip{r}#?, event_duration{r}#?, message{r}#?]]
     *           |_Project[[@timestamp{r}#?, client_ip{f}#?, event_duration{f}#?, message{f}#?]]
     *           | \_Eval[[TODATENANOS(@timestamp{f}#?) AS @timestamp#?]]
     *           |   \_EsRelation[sample_data][@timestamp{f}#?(date), client_ip{f}#?, event_duration..]
     *           \_Project[[@timestamp{f}#?, client_ip{f}#?, event_duration{f}#?, message{f}#?]]
     *             \_Subquery[]
     *               \_EsRelation[sample_data_ts_nanos][@timestamp{f}#?(date_nanos), client_ip{f}#?, event_duration..]
     *
     * Same pattern as {@link #testSubqueryRenameKeepOnMissingCounterFields()} but the field whose type is updated by the {@code UnionAll}
     * resolution is {@code @timestamp}, which is mapped as {@code date} in {@code sample_data} and as {@code date_nanos} in
     * {@code sample_data_ts_nanos}. {@code ResolveUnionTypesInUnionAll} casts the two to {@code DATE_NANOS} implicitly. The alias-cascade
     * in {@code updateAttributesReferencingUpdatedUnionAllOutput} also transform that reference to {@code DATE_NANOS} for the plan to stay
     * consistent.
     */
    public void testSubqueryRenameKeepOnDateAndDateNanosTimestamp() {
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsNanosIndex()).query("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute xOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("x", xOrder.name());
        assertEquals(DATE_NANOS, xOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute x = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("x", x.name());
        assertEquals(DATE_NANOS, x.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(DATE_NANOS, xAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute timestamp = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("@timestamp", timestamp.name());
        assertEquals(DATE_NANOS, timestamp.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "@timestamp".equals(a.name()) && DATE_NANOS.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[y{r}#?,ASC,LAST]]]
     *   \_Project[[y{r}#?]]
     *     \_Project[[@timestamp{r}#? AS y#?]]
     *       \_Project[[@timestamp{r}#?]]
     *         \_UnionAll[...]
     */
    public void testSubqueryRenameChainKeepOnDateAndDateNanosTimestamp() {
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsNanosIndex()).query("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x, x AS y
            | KEEP y
            | SORT y
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute yOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(DATE_NANOS, yOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute y = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("y", y.name());
        assertEquals(DATE_NANOS, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias yAlias = as(projections.get(0), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(DATE_NANOS, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute timestamp = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("@timestamp", timestamp.name());
        assertEquals(DATE_NANOS, timestamp.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "@timestamp".equals(a.name()) && DATE_NANOS.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[y{r}#?,ASC,LAST]]]
     *   \_Project[[y{r}#?]]
     *     \_Project[[x{r}#? AS y#?]]
     *       \_Project[[@timestamp{r}#? AS x#?]]
     *         \_Project[[@timestamp{r}#?]]
     *           \_UnionAll[...]
     */
    public void testSubqueryDoubleRenameKeepStarOnDateAndDateNanosTimestamp() {
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsNanosIndex()).query("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | RENAME x AS y
            | KEEP *
            | SORT y
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute yOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("y", yOrder.name());
        assertEquals(DATE_NANOS, yOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute y = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("y", y.name());
        assertEquals(DATE_NANOS, y.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias yAlias = as(projections.get(0), Alias.class);
        assertEquals("y", yAlias.name());
        assertEquals(DATE_NANOS, yAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(DATE_NANOS, xAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute timestamp = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("@timestamp", timestamp.name());
        assertEquals(DATE_NANOS, timestamp.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "@timestamp".equals(a.name()) && DATE_NANOS.equals(a.dataType())));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_OrderBy[[Order[x{r}#6,ASC,LAST]]]
     *   \_Project[[x{r}#6]]
     *     \_Project[[@timestamp{r}#18 AS x#6]]
     *       \_Project[[@timestamp{r}#18]]
     *         \_UnionAll[[@timestamp{r}#18, client_ip{r}#19, event_duration{r}#20, message{r}#21]]
     *           |_Project[[@timestamp{r}#22, client_ip{f}#11, event_duration{f}#12, message{f}#13]]
     *           | \_Eval[[TODATENANOS(@timestamp{f}#10) AS @timestamp#22]]
     *           |   \_EsRelation[sample_data][@timestamp{f}#10, client_ip{f}#11, event_duration{f..]
     *           \_Project[[@timestamp{f}#14, client_ip{f}#15, event_duration{f}#16, message{f}#17]]
     *             \_Subquery[]
     *               \_EsRelation[sample_data_ts_nanos][@timestamp{f}#14, client_ip{f}#15, event_duration{f..]
     *
     * Same as {@link #testSubqueryRenameKeepOnDateAndDateNanosTimestamp()} but with {@code SET unmapped_fields="nullify"}.
     */
    public void testSubqueryRenameKeepOnDateAndDateNanosTimestampWithNullify() {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        LogicalPlan plan = analyzer().addSampleData().addIndex(sampleDataTsNanosIndex()).statement("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        List<Order> order = orderBy.order();
        assertEquals(1, order.size());
        ReferenceAttribute xOrder = as(order.get(0).child(), ReferenceAttribute.class);
        assertEquals("x", xOrder.name());
        assertEquals(DATE_NANOS, xOrder.dataType());
        Project project = as(orderBy.child(), Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute x = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("x", x.name());
        assertEquals(DATE_NANOS, x.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        Alias xAlias = as(projections.get(0), Alias.class);
        assertEquals("x", xAlias.name());
        assertEquals(DATE_NANOS, xAlias.dataType());
        project = as(project.child(), Project.class);
        projections = project.projections();
        assertEquals(1, projections.size());
        ReferenceAttribute timestamp = as(projections.get(0), ReferenceAttribute.class);
        assertEquals("@timestamp", timestamp.name());
        assertEquals(DATE_NANOS, timestamp.dataType());
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertTrue(unionAll.output().stream().anyMatch(a -> "@timestamp".equals(a.name()) && DATE_NANOS.equals(a.dataType())));
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
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
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

    private static EsIndex sampleDataTsLongIndex() {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            new EsField("@timestamp", LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "client_ip",
            new EsField("client_ip", IP, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "event_duration",
            new EsField("event_duration", LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "message",
            new EsField("message", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        return new EsIndex("sample_data_ts_long", mapping, Map.of("sample_data_ts_long", IndexMode.STANDARD), Map.of(), Map.of());
    }

    private static EsIndex sampleDataTsNanosIndex() {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            new EsField("@timestamp", DATE_NANOS, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "client_ip",
            new EsField("client_ip", IP, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "event_duration",
            new EsField("event_duration", LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "message",
            new EsField("message", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        return new EsIndex("sample_data_ts_nanos", mapping, Map.of("sample_data_ts_nanos", IndexMode.STANDARD), Map.of(), Map.of());
    }

    /**
     * Helper: asserts the {@link UnionAll} output contains an attribute with the given name and
     * data type; the attribute is expected to be a {@link ReferenceAttribute} (not an
     * {@link UnsupportedAttribute}).
     */
    private static void assertAttributeType(List<Attribute> output, String name, DataType expected) {
        Attribute attr = output.stream().filter(a -> name.equals(a.name())).findFirst().orElseThrow();
        ReferenceAttribute ref = as(attr, ReferenceAttribute.class);
        assertEquals("Wrong type for [" + name + "]", expected, ref.dataType());
    }

    /**
     * Helper: asserts a {@link UnionAll} leg has the shape
     * {@code Project → Eval[<nullEvals>] → Subquery → Row[<expectedFieldNames>]} with the given
     * literal values for each ROW field. Used by
     * {@link #testThreeRowSubqueriesWithDisjointFieldNamesMixedScalarAndMultivalue()}.
     */
    private static void assertRowLegWithNullEvals(LogicalPlan legPlan, List<String> rowFieldNames, List<Object> rowFieldValues) {
        Project legProject = as(legPlan, Project.class);
        Eval legEval = as(legProject.child(), Eval.class);
        // The other 4 fields not in this leg are null-filled.
        assertEquals(4, legEval.fields().size());
        Subquery legSubquery = as(legEval.child(), Subquery.class);
        Row row = as(legSubquery.child(), Row.class);
        assertEquals(rowFieldNames.size(), row.fields().size());
        for (int i = 0; i < rowFieldNames.size(); i++) {
            assertEquals(rowFieldNames.get(i), row.fields().get(i).name());
            Literal literal = as(row.fields().get(i).child(), Literal.class);
            assertEquals(rowFieldValues.get(i), literal.value());
        }
    }

    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    /**
     * Analyzes a subquery query that may mix source kinds, registering everything the subquery analyzer tests need:
     * the {@code sample_data} regular index, the {@code k8s} time-series index, and two external datasets
     * ({@code salaries_int}/{@code salaries_long}) that share {@code emp_no}/{@code name} but type {@code salary}
     * differently ({@code integer} vs {@code long}). Indices and datasets that a given query does not reference are
     * simply left unused. Mirrors the production pipeline: {@link DatasetRewriter} turns each {@code FROM <dataset>}
     * into the {@code UnresolvedExternalRelation} the {@code EXTERNAL} command produces, which the analyzer resolves
     * against the configured external source schemas — so a dataset branch is backed by an {@link ExternalRelation},
     * exactly like a real dataset subquery. The plan is analyzed (not optimized) to match the neighbouring tests.
     *
     * <p>Callers that exercise a {@code TS} source inside a subquery must additionally guard on
     * {@link EsqlCapabilities.Cap#SUBQUERY_WITH_TS}.
     */
    private static LogicalPlan analyzeExternalDatasetSubquery(String query) {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset intDataset = new Dataset("salaries_int", new DataSourceReference("external_ds"), SALARIES_INT_RESOURCE, null, Map.of());
        Dataset longDataset = new Dataset("salaries_long", new DataSourceReference("external_ds"), SALARIES_LONG_RESOURCE, null, Map.of());
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of("salaries_int", intDataset, "salaries_long", longDataset))
            .build();
        LogicalPlan rewritten = DatasetRewriter.rewrite(
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
        return analyzer().addSampleData().addK8s().externalSourceResolution(resolution).buildAnalyzer().analyze(rewritten);
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

    private static TestAnalyzer basic() {
        return analyzer().addEmployees("test").stripErrorPrefix(true);
    }

    private static TestAnalyzer defaultMapping() {
        return analyzer().addDefaultIndex();
    }

    private static TestAnalyzer sampleData() {
        return analyzer().addSampleData();
    }

    private static TestAnalyzer k8s() {
        return analyzer().addK8sDownsampled();
    }
}
