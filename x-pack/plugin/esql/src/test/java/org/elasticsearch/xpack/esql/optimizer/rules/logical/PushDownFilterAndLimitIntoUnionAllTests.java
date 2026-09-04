/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.junit.Before;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfigurationBuilder;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.instanceOf;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PushDownFilterAndLimitIntoUnionAllTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkSubqueryInFromCommandSupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue(
            "Requires subquery in FROM command support",
            EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled()
        );
    }

    private static void checkSubqueryWithTSCommand() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
    }

    private static void checkExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51,
     *                    languages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30,
     *                           language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Filter[emp_no{f}#6 > 10000[INTEGER]]
     *   |     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32,
     *                           language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPushDownSimpleFilterPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
            | WHERE emp_no > 10000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);

        // the last child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Eval eval = as(child1.child(), Eval.class);
        Filter childFilter = as(eval.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        greaterThan = as(and.left(), GreaterThan.class);
        empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", empNo.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        greaterThan = as(and.right(), GreaterThan.class);
        empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#27, emp_no{r}#28, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *                    languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *   |_Project[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *                           languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10]]
     *   | \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *                            languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21]]
     *     \_Subquery[]
     *       \_TopN[[Order[emp_no{f}#16,ASC,LAST]],1000[INTEGER],false]
     *         \_Filter[languages{f}#19 > 0[INTEGER]]
     *           \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testSubqueryWithSort() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 | SORT emp_no | LIMIT 1000)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        EsRelation relation = as(child1.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        TopN topN = as(subquery.child(), TopN.class);
        Filter childFilter = as(topN.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", empNo.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *                    languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   |_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   | \_Filter[emp_no{f}#6 > 10000[INTEGER]]
     *   |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   \_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *     \_Subquery[]
     *       \_TopN[[Order[emp_no{f}#17,ASC,LAST]],1000[INTEGER],false]
     *         \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER]]
     *           \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPushDownFilterAndLimitPastSubqueryWithSort() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 | SORT emp_no | LIMIT 1000)
            | WHERE emp_no > 10000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter childFilter = as(child1.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        greaterThan = as(childFilter.condition(), GreaterThan.class);
        empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        TopN topN = as(childFilter.child(), TopN.class);
        childFilter = as(topN.child(), Filter.class);
        greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *                    languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, language_code{r}#57, language_name{r}#58]]
     *   |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Filter[emp_no{f}#7 > 10000[INTEGER] AND salary{f}#12 > 50000[INTEGER]]
     *   |     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, language_code{r}#33,
     *                           language_name{r}#34]]
     *   | \_Eval[[null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Filter[languages{f}#21 > 0[INTEGER] AND emp_no{f}#18 > 10000[INTEGER] AND salary{f}#23 > 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testPushDownConjunctiveFilterPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
            | WHERE emp_no > 10000 and salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);

        // the last child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Eval eval = as(child1.child(), Eval.class);
        Filter childFilter = as(eval.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        GreaterThan emp_no = as(and.left(), GreaterThan.class);
        FieldAttribute empNo = as(emp_no.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(emp_no.right(), Literal.class);
        assertEquals(10000, right.value());
        GreaterThan salary = as(and.right(), GreaterThan.class);
        FieldAttribute salaryField = as(salary.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        right = as(salary.right(), Literal.class);
        assertEquals(50000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        and = as(childFilter.condition(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        and = as(and.right(), And.class);
        emp_no = as(and.left(), GreaterThan.class);
        empNo = as(emp_no.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        right = as(emp_no.right(), Literal.class);
        assertEquals(10000, right.value());
        salary = as(and.right(), GreaterThan.class);
        salaryField = as(salary.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        right = as(salary.right(), Literal.class);
        assertEquals(50000, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *                    languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, language_code{r}#57, language_name{r}#58]]
     *   |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Filter[emp_no{f}#7 > 10000[INTEGER] OR salary{f}#12 > 50000[INTEGER]]
     *   |     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, language_code{r}#33,
     *                           language_name{r}#34]]
     *   | \_Eval[[null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Filter[languages{f}#21 > 0[INTEGER] AND emp_no{f}#18 > 10000[INTEGER] OR salary{f}#23 > 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testPushDownDisjunctiveFilterPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
            | WHERE emp_no > 10000 or salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);

        // the last child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Eval eval = as(child1.child(), Eval.class);
        Filter childFilter = as(eval.child(), Filter.class);
        Or or = as(childFilter.condition(), Or.class);
        GreaterThan emp_no = as(or.left(), GreaterThan.class);
        FieldAttribute empNo = as(emp_no.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(emp_no.right(), Literal.class);
        assertEquals(10000, right.value());
        GreaterThan salary = as(or.right(), GreaterThan.class);
        FieldAttribute salaryField = as(salary.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        right = as(salary.right(), Literal.class);
        assertEquals(50000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        or = as(and.right(), Or.class);
        emp_no = as(or.left(), GreaterThan.class);
        empNo = as(emp_no.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        right = as(emp_no.right(), Literal.class);
        assertEquals(10000, right.value());
        salary = as(or.right(), GreaterThan.class);
        salaryField = as(salary.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        right = as(salary.right(), Literal.class);
        assertEquals(50000, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *                    languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, language_code{r}#57, language_name{r}#58]]
     *   |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Filter[emp_no{f}#7 > 10000[INTEGER] AND salary{f}#12 < 50000[INTEGER]]
     *   |     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, language_code{r}#33,
     *                           language_name{r}#34]]
     *   | \_Eval[[null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Filter[salary{f}#23 < 50000[INTEGER] AND emp_no{f}#18 > 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testPushDownFilterPastUnionAllAndCombineWithFilterInSubquery() {
        var plan = planSubquery("""
            FROM test, (FROM test | where salary < 100000), (FROM languages  | WHERE language_code > 0)
            | WHERE emp_no > 10000 and salary < 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // the last child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Eval eval = as(child1.child(), Eval.class);
        Filter childFilter = as(eval.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        GreaterThan emp_no = as(and.left(), GreaterThan.class);
        FieldAttribute empNo = as(emp_no.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(emp_no.right(), Literal.class);
        assertEquals(10000, right.value());
        LessThan salary = as(and.right(), LessThan.class);
        FieldAttribute salaryField = as(salary.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        right = as(salary.right(), Literal.class);
        assertEquals(50000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        and = as(childFilter.condition(), And.class);
        emp_no = as(and.right(), GreaterThan.class);
        empNo = as(emp_no.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        right = as(emp_no.right(), Literal.class);
        assertEquals(10000, right.value());
        salary = as(and.left(), LessThan.class);
        salaryField = as(salary.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        right = as(salary.right(), Literal.class);
        assertEquals(50000, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     *Project[[_meta_field{r}#102, emp_no{r}#103, first_name{r}#104, gender{r}#105, hire_date{r}#106, job{r}#107, job.raw{r}#108,
     *               languages{r}#109, last_name{r}#110, long_noidx{r}#111, salary{r}#112, z{r}#115, language_name{r}#116,
     *               $$x$converted_to$long{r$}#125 AS x#38, $$y$converted_to$long{r$}#126 AS y#41]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_UnionAll[[_meta_field{r}#102, emp_no{r}#103, first_name{r}#104, gender{r}#105, hire_date{r}#106, job{r}#107, job.raw{r}#108,
     *                      languages{r}#109, last_name{r}#110, long_noidx{r}#111, salary{r}#112, x{r}#113, $$x$converted_to$long{r$}#125,
     *                      y{r}#114, $$y$converted_to$long{r$}#126, z{r}#115, language_name{r}#116]]
     *     |_Project[[_meta_field{f}#62, emp_no{f}#56, first_name{f}#57, gender{f}#58, hire_date{f}#63, job{f}#64, job.raw{f}#65,
     *                             languages{f}#59, last_name{f}#60, long_noidx{f}#66, salary{f}#61, x{r}#5, $$x$converted_to$long{r}#119,
     *                             y{r}#128, $$y$converted_to$long{r}#120, z{r}#11, language_name{r}#86]]
     *     | \_Filter[ISNOTNULL($$y$converted_to$long{r}#120)]
     *     |   \_Eval[[null[KEYWORD] AS language_name#86, 1[LONG] AS $$x$converted_to$long#119,
     *                     TOLONG(y{r}#8) AS $$y$converted_to$long#120, null[KEYWORD] AS y#128]]
     *     |     \_Subquery[]
     *     |       \_Project[[_meta_field{f}#62, emp_no{f}#56, first_name{f}#57, gender{f}#58, hire_date{f}#63, job{f}#64, job.raw{f}#65,
     *                              languages{f}#59, last_name{f}#60, long_noidx{f}#66, salary{f}#61, x{r}#5, emp_no{f}#56 AS y#8, z{r}#11]]
     *     |         \_Filter[z{r}#11 > 0[INTEGER]]
     *     |           \_Eval[[1[INTEGER] AS x#5, emp_no{f}#56 + 1[INTEGER] AS z#11]]
     *     |             \_Filter[salary{f}#61 < 100000[INTEGER]]
     *     |               \_EsRelation[test][_meta_field{f}#62, emp_no{f}#56, first_name{f}#57, ..]
     *     |_EsqlProject[[_meta_field{r}#87, emp_no{r}#88, first_name{r}#89, gender{r}#90, hire_date{r}#91, job{r}#92, job.raw{r}#93,
     *                             languages{r}#94, last_name{r}#95, long_noidx{r}#96, salary{r}#97, x{r}#22, $$x$converted_to$long{r}#121,
     *                             y{r}#129, $$y$converted_to$long{r}#122, z{r}#17, language_name{r}#98]]
     *     | \_Filter[ISNOTNULL($$y$converted_to$long{r}#122)]
     *     |   \_Eval[[null[KEYWORD] AS _meta_field#87, null[INTEGER] AS emp_no#88, null[KEYWORD] AS first_name#89,
     *                     null[TEXT] AS gender#90, null[DATETIME] AS hire_date#91, null[TEXT] AS job#92, null[KEYWORD] AS job.raw#93,
     *                     null[INTEGER] AS languages#94, null[KEYWORD] AS last_name#95, null[LONG] AS long_noidx#96,
     *                     null[INTEGER] AS salary#97, null[KEYWORD] AS language_name#98, 1[LONG] AS $$x$converted_to$long#121,
     *                     TOLONG(y{r}#20) AS $$y$converted_to$long#122, null[KEYWORD] AS y#129]]
     *     |     \_Subquery[]
     *     |       \_Eval[[1[INTEGER] AS x#22]]
     *     |         \_Filter[z{r}#17 > 0[INTEGER]]
     *     |           \_Aggregate[[language_code{f}#67],[COUNT(*[KEYWORD],true[BOOLEAN]) AS y#20, language_code{f}#67 AS z#17]]
     *     |             \_EsRelation[languages][language_code{f}#67, language_name{f}#68]
     *     \_EsqlProject[[_meta_field{f}#75, emp_no{r}#99, first_name{f}#70, gender{f}#71, hire_date{f}#76, job{f}#77, job.raw{f}#78,
     *                             languages{r}#100, last_name{f}#73, long_noidx{f}#79, salary{r}#101, x{r}#29,
     *                             $$x$converted_to$long{r}#123, y{r}#130, $$y$converted_to$long{r}#124, z{r}#35, language_name{f}#81]]
     *       \_Filter[ISNOTNULL($$x$converted_to$long{r}#123) AND ISNOTNULL($$y$converted_to$long{r}#124)]
     *         \_Eval[[null[INTEGER] AS emp_no#99, null[INTEGER] AS languages#100, null[INTEGER] AS salary#101,
     *                     TOLONG(x{r}#29) AS $$x$converted_to$long#123, TOLONG(y{r}#32) AS $$y$converted_to$long#124,
     *                     null[KEYWORD] AS y#130]]
     *           \_Subquery[]
     *             \_Project[[_meta_field{f}#75, emp_no{f}#69 AS x#29, first_name{f}#70, gender{f}#71, hire_date{f}#76, job{f}#77,
     *                              job.raw{f}#78, languages{f}#72 AS z#35, last_name{f}#73, long_noidx{f}#79, salary{f}#74 AS y#32,
     *                              language_name{f}#81]]
     *               \_Join[LEFT,[languages{f}#72],[language_code{f}#80],null]
     *                 | \_Filter[languages{f}#72 > 0[INTEGER]]
     *                 |   \_EsRelation[test][_meta_field{f}#75, emp_no{f}#69, first_name{f}#70, ..]
     *                 \_EsRelation[languages_lookup][LOOKUP][language_code{f}#80, language_name{f}#81]
     */
    public void testPushDownFilterOnReferenceAttributesPastUnionAll() {
        var plan = planSubquery("""
            FROM test
                       , (FROM test
                          | where salary < 100000
                          | EVAL x = 1, y = emp_no, z = emp_no + 1)
                       , (FROM languages
                          | STATS cnt = COUNT(*) by language_code
                          | RENAME language_code AS z, cnt AS y
                          | EVAL x = 1)
                       , (FROM test
                          | RENAME languages AS language_code
                          | LOOKUP JOIN languages_lookup ON language_code
                          | RENAME emp_no AS x, salary AS y, language_code AS z)
            | EVAL x = x::long, y = y::long
            | WHERE x is not null and y is not null and z > 0
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(15, projections.size());
        Limit limit = as(project.child(), Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // the last child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(3, unionAll.children().size());

        Project child2 = as(unionAll.children().get(0), Project.class);
        Filter filter = as(child2.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        ReferenceAttribute y = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$y$converted_to$long", y.name());
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(4, aliases.size());
        assertEquals("language_name", aliases.get(0).name());
        assertEquals("$$x$converted_to$long", aliases.get(1).name());
        assertEquals("$$y$converted_to$long", aliases.get(2).name());
        assertEquals("y", aliases.get(3).name());
        Subquery subquery = as(eval.child(), Subquery.class);
        project = as(subquery.child(), Project.class);
        Filter childFilter = as(project.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        ReferenceAttribute z = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("z", z.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        eval = as(childFilter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias aliasX = aliases.get(0);
        assertEquals("x", aliasX.name());
        Literal xLiteral = as(aliasX.child(), Literal.class);
        assertEquals(1, xLiteral.value());
        Alias aliasZ = aliases.get(1);
        assertEquals("z", aliasZ.name());
        childFilter = as(eval.child(), Filter.class);
        LessThan lessThan = as(childFilter.condition(), LessThan.class);
        FieldAttribute salaryField = as(lessThan.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        Literal literal = as(lessThan.right(), Literal.class);
        assertEquals(100000, literal.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child3 = as(unionAll.children().get(1), Project.class);
        filter = as(child3.child(), Filter.class);
        isNotNull = as(filter.condition(), IsNotNull.class);
        y = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$y$converted_to$long", y.name());
        eval = as(filter.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        eval = as(subquery.child(), Eval.class);
        filter = as(eval.child(), Filter.class);
        greaterThan = as(filter.condition(), GreaterThan.class);
        z = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("z", z.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        Aggregate aggregate = as(filter.child(), Aggregate.class);
        List<Expression> groupings = aggregate.groupings();
        assertEquals(1, groupings.size());
        FieldAttribute language_code = as(groupings.get(0), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        List<? extends NamedExpression> aggregates = aggregate.aggregates();
        assertEquals(2, aggregates.size());
        assertEquals("y", aggregates.get(0).name());
        assertEquals("z", aggregates.get(1).name());
        relation = as(aggregate.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());

        Project child4 = as(unionAll.children().get(2), Project.class);
        filter = as(child4.child(), Filter.class);
        And and = as(filter.condition(), And.class);
        isNotNull = as(and.left(), IsNotNull.class);
        ReferenceAttribute x = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$x$converted_to$long", x.name());
        isNotNull = as(and.right(), IsNotNull.class);
        ReferenceAttribute yAttr = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$y$converted_to$long", yAttr.name());
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(6, aliases.size());
        subquery = as(eval.child(), Subquery.class);
        project = as(subquery.child(), Project.class);
        Join lookupJoin = as(project.child(), Join.class);
        filter = as(lookupJoin.left(), Filter.class);
        greaterThan = as(filter.condition(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", language_code.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(filter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
        relation = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", relation.indexPattern());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#36, emp_no{r}#37, first_name{r}#38, gender{r}#39, hire_date{r}#40, job{r}#41, job.raw{r}#42,
     *                    languages{r}#43, last_name{r}#44, long_noidx{r}#45, salary{r}#46, x{r}#47, y{r}#48]]
     *   \_Project[[_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, gender{f}#25, hire_date{f}#30, job{f}#31, job.raw{f}#32,
     *                           languages{f}#26, last_name{f}#27, long_noidx{f}#33, salary{f}#28, x{r}#5, y{r}#8]]
     *     \_Subquery[]
     *       \_Filter[y{r}#8 > 0[INTEGER]]
     *         \_Eval[[1[INTEGER] AS x#5, emp_no{f}#23 + 1[INTEGER] AS y#8]]
     *           \_Filter[salary{f}#28 < 100000[INTEGER] AND emp_no{f}#23 > 0[INTEGER]]
     *             \_EsRelation[test][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     */
    public void testPushDownFilterOnReferenceAttributesAndFieldAttributesPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | where salary < 100000 | EVAL x = 1, y = emp_no + 1)
            | WHERE x is not null and y > 0 and emp_no > 0
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // the first child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(1, unionAll.children().size());

        Project child2 = as(unionAll.children().get(0), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        Filter childFilter = as(subquery.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        ReferenceAttribute y = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("y", y.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        Eval eval = as(childFilter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Alias aliasX = aliases.get(0);
        assertEquals("x", aliasX.name());
        Literal xLiteral = as(aliasX.child(), Literal.class);
        assertEquals(1, xLiteral.value());
        Alias aliasZ = aliases.get(1);
        assertEquals("y", aliasZ.name());
        childFilter = as(eval.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        greaterThan = as(and.right(), GreaterThan.class);
        FieldAttribute emp_no = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", emp_no.name());
        LessThan lessThan = as(and.left(), LessThan.class);
        FieldAttribute salaryField = as(lessThan.left(), FieldAttribute.class);
        assertEquals("salary", salaryField.name());
        Literal literal = as(lessThan.right(), Literal.class);
        assertEquals(100000, literal.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     * TODO push down filter on mixed typed attributes
     * Project[[_meta_field{r}#67, first_name{r}#69, hire_date{r}#71, job{r}#72, job.raw{r}#73, languages{r}#74, last_name{r}#75,
     *              long_noidx{r}#76, salary{r}#77, avg_worked_seconds{r}#78, birth_date{r}#79, height{r}#80, height.double{r}#81,
     *              height.half_float{r}#82, height.scaled_float{r}#83, is_rehired{r}#84, job_positions{r}#85, languages.int{r}#86,
     *              languages.long{r}#87, languages.short{r}#88, salary_change{r}#89, still_hired{r}#90,
     *              $$emp_no$converted_to$double{r$}#97 AS x#6, $$emp_no$converted_to$long{r$}#98 AS emp_no#9,
     *              $$gender$converted_to$keyword{r$}#99 AS gender#12, languages{r}#74 AS y#15]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[$$emp_no$converted_to$long{r$}#98 > 10000[INTEGER] AND
     *                ISNOTNULL($$gender$converted_to$keyword{r$}#99) AND
     *                languages{r}#74 < 5[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#67, emp_no{r}#68, $$emp_no$converted_to$double{r$}#97, $$emp_no$converted_to$long{r$}#98,
     *                        first_name{r}#69, gender{r}#70, $$gender$converted_to$keyword{r$}#99, hire_date{r}#71, job{r}#72,
     *                        job.raw{r}#73, languages{r}#74, last_name{r}#75, long_noidx{r}#76, salary{r}#77, avg_worked_seconds{r}#78,
     *                        birth_date{r}#79, height{r}#80, height.double{r}#81, height.half_float{r}#82, height.scaled_float{r}#83,
     *                        is_rehired{r}#84, job_positions{r}#85, languages.int{r}#86, languages.long{r}#87, languages.short{r}#88,
     *                        salary_change{r}#89, still_hired{r}#90]]
     *       |_Project[[_meta_field{f}#25, emp_no{r}#100, $$emp_no$converted_to$double{r}#91, $$emp_no$converted_to$long{r}#92,
     *                               first_name{r}#101, gender{f}#21, $$gender$converted_to$keyword{r}#93, hire_date{r}#102, job{f}#27,
     *                               job.raw{f}#28, languages{f}#22, last_name{r}#103, long_noidx{f}#29, salary{r}#104,
     *                               avg_worked_seconds{r}#50, birth_date{r}#51, height{r}#52, height.double{r}#53,
     *                               height.half_float{r}#54, height.scaled_float{r}#55, is_rehired{r}#56, job_positions{r}#57,
     *                               languages.int{r}#58, languages.long{r}#59, languages.short{r}#60, salary_change{r}#61,
     *                               still_hired{r}#62]]
     *       | \_Eval[[null[UNSIGNED_LONG] AS avg_worked_seconds#50, null[DATETIME] AS birth_date#51, null[DOUBLE] AS height#52,
     *                     null[DOUBLE] AS height.double#53, null[DOUBLE] AS height.half_float#54, null[DOUBLE] AS height.scaled_float#55,
     *                     null[KEYWORD] AS is_rehired#56, null[TEXT] AS job_positions#57, null[INTEGER] AS languages.int#58,
     *                     null[LONG] AS languages.long#59, null[INTEGER] AS languages.short#60, null[DOUBLE] AS salary_change#61,
     *                     null[KEYWORD] AS still_hired#62, TODOUBLE(emp_no{f}#19) AS $$emp_no$converted_to$double#91,
     *                     TOLONG(emp_no{f}#19) AS $$emp_no$converted_to$long#92,
     *                     TOSTRING(gender{f}#21) AS $$gender$converted_to$keyword#93, null[KEYWORD] AS emp_no#100,
     *                     null[KEYWORD] AS first_name#101, TODATENANOS(hire_date{f}#26) AS hire_date#102,
     *                     null[KEYWORD] AS last_name#103, null[KEYWORD] AS salary#104]]
     *       |   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *       \_Project[[_meta_field{r}#63, $$emp_no$temp_name$109{r}#110 AS emp_no#105, $$emp_no$converted_to$double{r}#94,
     *                        emp_no{f}#30 AS $$emp_no$converted_to$long#95, first_name{r}#106, gender{f}#33,
     *                        $$gender$converted_to$keyword{r}#96, hire_date{f}#35, job{r}#64, job.raw{r}#65, languages{f}#37,
     *                        last_name{r}#107, long_noidx{r}#66, salary{r}#108, avg_worked_seconds{f}#46, birth_date{f}#34, height{f}#41,
     *                        height.double{f}#42, height.half_float{f}#44, height.scaled_float{f}#43, is_rehired{f}#48,
     *                        job_positions{f}#47, languages.int{f}#40, languages.long{f}#38, languages.short{f}#39, salary_change{f}#49,
     *                        still_hired{f}#45]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#63, null[TEXT] AS job#64, null[KEYWORD] AS job.raw#65, null[LONG] AS long_noidx#66,
     *                     TODOUBLE(emp_no{f}#30) AS $$emp_no$converted_to$double#94,
     *                     TOSTRING(gender{f}#33) AS $$gender$converted_to$keyword#96, null[KEYWORD] AS $$emp_no$temp_name$109#110,
     *                     null[KEYWORD] AS first_name#106, null[KEYWORD] AS last_name#107, null[KEYWORD] AS salary#108]]
     *           \_Subquery[]
     *             \_Filter[languages{f}#37 > 1[INTEGER]]
     *               \_EsRelation[test_mixed_types][avg_worked_seconds{f}#46, birth_date{f}#34, emp_no{..]
     */
    public void testFilterOnMixedDataTypesFields() {
        var plan = planSubquery("""
            FROM test, (FROM test_mixed_types | WHERE languages > 1)
            | EVAL x = emp_no::double,  emp_no = emp_no::long, gender = gender::keyword, y = languages
            | WHERE emp_no > 10000 AND gender is not null AND y < 5
            """);

        Project project = as(plan, Project.class);
        List<? extends NamedExpression> projections = project.projections();
        assertEquals(26, projections.size());
        Limit limit = as(project.child(), Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter childFilter = as(child1.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        ReferenceAttribute emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        IsNotNull isNotNull = as(and.right(), IsNotNull.class);
        ReferenceAttribute gender = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$gender$converted_to$keyword", gender.name());
        Eval eval = as(childFilter.child(), Eval.class);
        childFilter = as(eval.child(), Filter.class);
        LessThan lessThan = as(childFilter.condition(), LessThan.class);
        FieldAttribute languages = as(lessThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(lessThan.right(), Literal.class);
        assertEquals(5, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        childFilter = as(child2.child(), Filter.class);
        isNotNull = as(childFilter.condition(), IsNotNull.class);
        gender = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$gender$converted_to$keyword", gender.name());
        eval = as(childFilter.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        and = as(childFilter.condition(), And.class);
        greaterThan = as(and.left(), GreaterThan.class);
        languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(1, right.value());
        and = as(and.right(), And.class);
        greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute emp_no_field = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", emp_no_field.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        lessThan = as(and.right(), LessThan.class);
        languages = as(lessThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(lessThan.right(), Literal.class);
        assertEquals(5, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#27, emp_no{r}#28, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *                    languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *   |_Project[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *                           languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10]]
     *   | \_Filter[:(first_name{f}#6,first[KEYWORD])]
     *   |   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *                           languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21]]
     *     \_Subquery[]
     *       \_Filter[languages{f}#19 > 0[INTEGER] AND :(first_name{f}#17,first[KEYWORD])]
     *         \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testPushDownSingleFullTextFunctionPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0)
            | WHERE first_name:"first"
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter childFilter = as(child1.child(), Filter.class);
        MatchOperator match = as(childFilter.condition(), MatchOperator.class);
        FieldAttribute first_name = as(match.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        Literal right = as(match.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        MatchOperator matchOperator = as(and.right(), MatchOperator.class);
        first_name = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        right = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#26, emp_no{r}#27, first_name{r}#28, gender{r}#29, hire_date{r}#30, job{r}#31, job.raw{r}#32,
     *                    languages{r}#33, last_name{r}#34, long_noidx{r}#35, salary{r}#36]]
     *   |_Project[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13,
     *                           languages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9]]
     *   | \_Filter[QSTR(first_name:first[KEYWORD]) AND KQL(last_name:last[KEYWORD])]
     *   |   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     *   \_EsqlProject[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24,
     *                           languages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *     \_Subquery[]
     *       \_Filter[languages{f}#18 > 0[INTEGER] AND QSTR(gender:female[KEYWORD]) AND
     *                    QSTR(first_name:first[KEYWORD]) AND KQL(last_name:last[KEYWORD])]
     *         \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDownFullTextFunctionNoFieldRequiredPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 AND qstr("gender:female"))
            | WHERE qstr("first_name:first") == true AND kql("last_name:last") == false
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter childFilter = as(child1.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        QueryString queryString = as(and.left(), QueryString.class);
        Literal queryStringLiteral = as(queryString.query(), Literal.class);
        assertEquals(new BytesRef("first_name:first"), queryStringLiteral.value());
        Not not = as(and.right(), Not.class);
        Kql kql = as(not.negate(), Kql.class);
        Literal kqlLiteral = as(kql.query(), Literal.class);
        assertEquals(new BytesRef("last_name:last"), kqlLiteral.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        and = as(childFilter.condition(), And.class);
        And subqueryAnd = as(and.left(), And.class);
        GreaterThan greaterThan = as(subqueryAnd.left(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        queryString = as(subqueryAnd.right(), QueryString.class);
        queryStringLiteral = as(queryString.query(), Literal.class);
        assertEquals(new BytesRef("gender:female"), queryStringLiteral.value());
        and = as(and.right(), And.class);
        queryString = as(and.left(), QueryString.class);
        queryStringLiteral = as(queryString.query(), Literal.class);
        assertEquals(new BytesRef("first_name:first"), queryStringLiteral.value());
        not = as(and.right(), Not.class);
        Kql kqlFunction = as(not.negate(), Kql.class);
        kqlLiteral = as(kqlFunction.query(), Literal.class);
        assertEquals(new BytesRef("last_name:last"), kqlLiteral.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *                    languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   |_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   | \_Filter[:(first_name{f}#7,first[KEYWORD]) AND MATCH(last_name{f}#10,last[KEYWORD]) AND QSTR(gender:female[KEYWORD])]
     *   |   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   \_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *     \_Subquery[]
     *       \_Filter[languages{f}#20 > 0[INTEGER] AND :(first_name{f}#18,first[KEYWORD]) AND
     *                      MATCH(last_name{f}#21,last[KEYWORD]) AND QSTR(gender:female[KEYWORD])]
     *         \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPushDownConjunctiveFullTextFunctionPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0)
            | WHERE first_name:"first" and match(last_name, "last") and qstr("gender:female")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter childFilter = as(child1.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        QueryString queryString = as(and.right(), QueryString.class);
        Literal queryStringLiteral = as(queryString.query(), Literal.class);
        assertEquals(new BytesRef("gender:female"), queryStringLiteral.value());
        and = as(and.left(), And.class);
        MatchOperator matchOperator = as(and.left(), MatchOperator.class);
        FieldAttribute first_name = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        Literal right = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        Match matchFunction = as(and.right(), Match.class);
        FieldAttribute last_name = as(matchFunction.field(), FieldAttribute.class);
        assertEquals("last_name", last_name.name());
        right = as(matchFunction.query(), Literal.class);
        assertEquals(new BytesRef("last"), right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        and = as(childFilter.condition(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        and = as(and.right(), And.class);
        queryString = as(and.right(), QueryString.class);
        queryStringLiteral = as(queryString.query(), Literal.class);
        assertEquals(new BytesRef("gender:female"), queryStringLiteral.value());
        and = as(and.left(), And.class);
        matchOperator = as(and.left(), MatchOperator.class);
        first_name = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        right = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        matchFunction = as(and.right(), Match.class);
        last_name = as(matchFunction.field(), FieldAttribute.class);
        assertEquals("last_name", last_name.name());
        right = as(matchFunction.query(), Literal.class);
        assertEquals(new BytesRef("last"), right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#29, emp_no{r}#30, first_name{r}#31, gender{r}#32, hire_date{r}#33, job{r}#34, job.raw{r}#35,
     *                    languages{r}#36, last_name{r}#37, long_noidx{r}#38, salary{r}#39]]
     *   |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12]]
     *   | \_Filter[:(first_name{f}#8,first[KEYWORD]) OR MatchPhrase(last_name{f}#11,last[KEYWORD]) OR KQL(gender:female[KEYWORD])]
     *   |   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   \_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23]]
     *     \_Subquery[]
     *       \_Filter[languages{f}#21 > 0[INTEGER] AND MATCH(gender{f}#20,F[KEYWORD]) AND :(first_name{f}#19,first[KEYWORD]) OR
     *                     MatchPhrase(last_name{f}#22,last[KEYWORD]) OR KQL(gender:female[KEYWORD])]
     *         \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testPushDownDisjunctiveFullTextFunctionPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 and match(gender , "F"))
            | WHERE first_name:"first" or match_phrase(last_name, "last") or kql("gender:female")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter childFilter = as(child1.child(), Filter.class);
        Or or = as(childFilter.condition(), Or.class);
        Kql kql = as(or.right(), Kql.class);
        Literal kqlLiteral = as(kql.query(), Literal.class);
        assertEquals(new BytesRef("gender:female"), kqlLiteral.value());
        or = as(or.left(), Or.class);
        MatchOperator matchOperator = as(or.left(), MatchOperator.class);
        FieldAttribute first_name = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        Literal right = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        MatchPhrase matchPhrase = as(or.right(), MatchPhrase.class);
        FieldAttribute last_name = as(matchPhrase.field(), FieldAttribute.class);
        assertEquals("last_name", last_name.name());
        right = as(matchPhrase.query(), Literal.class);
        assertEquals(new BytesRef("last"), right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childFilter = as(subquery.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
        or = as(and.right(), Or.class);
        kql = as(or.right(), Kql.class);
        kqlLiteral = as(kql.query(), Literal.class);
        assertEquals(new BytesRef("gender:female"), kqlLiteral.value());
        or = as(or.left(), Or.class);
        matchOperator = as(or.left(), MatchOperator.class);
        first_name = as(matchOperator.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        right = as(matchOperator.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        matchPhrase = as(or.right(), MatchPhrase.class);
        last_name = as(matchPhrase.field(), FieldAttribute.class);
        assertEquals("last_name", last_name.name());
        right = as(matchPhrase.query(), Literal.class);
        assertEquals(new BytesRef("last"), right.value());
        and = as(and.left(), And.class);
        Match matchFunction = as(and.right(), Match.class);
        FieldAttribute gender = as(matchFunction.field(), FieldAttribute.class);
        assertEquals("gender", gender.name());
        right = as(matchFunction.query(), Literal.class);
        assertEquals(new BytesRef("F"), right.value());
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute languages = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());
    }

    /*
     * If the field used in the full text function is not present in one of the indices in the UnionAll branches,
     * the full text function can be pushed down.
     */
    public void testFullTextFunctionCanBePushedDownPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM languages)
            | WHERE match(language_name, "text")
            """);

        // Limit[1000[INTEGER],false,false]
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // First child is pruned: LocalRelation with EMPTY data since filter on language_name can't be applied to test index
        assertEquals(1, unionAll.children().size());
        // Second child: languages subquery with MATCH filter pushed down
        Project child2 = as(unionAll.children().getFirst(), Project.class);
        Eval eval2 = as(child2.child(), Eval.class);
        List<Alias> aliases = eval2.fields();
        assertEquals(11, aliases.size());

        Subquery subquery = as(eval2.child(), Subquery.class);
        Filter filter = as(subquery.child(), Filter.class);
        Match match = as(filter.condition(), Match.class);
        FieldAttribute languageName = as(match.field(), FieldAttribute.class);
        assertEquals("language_name", languageName.name());
        Literal queryLiteral = as(match.query(), Literal.class);
        assertEquals(new BytesRef("text"), queryLiteral.value());

        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    /*
     * Knn's implicitK is not serialized when the plan is sent to remote nodes, if there is no implicit limit appended to the subquery,
     *  knn validation will fail, or it may return wrong results. If a knn function is found in a subquery,
     * a limit is appended to the subquery to make sure the implicitK is preserved.
     *
     * EsqlProject[[color{r}#27, rgb_vector{r}#32, language_name{r}#35]]
     * \_TopN[[Order[_score{r}#33,DESC,FIRST], Order[color{r}#27,ASC,LAST]],10[INTEGER],false]
     *   \_UnionAll[[color{r}#27, hex_code{r}#28, id{r}#29, primary{r}#30, rgb_byte_vector{r}#31, rgb_vector{r}#32, _score{r}#33,
     *                      language_code{r}#34, language_name{r}#35]]
     *     |_EsqlProject[[color{f}#11, hex_code{f}#12, id{f}#10, primary{f}#13, rgb_byte_vector{f}#15, rgb_vector{f}#14, _score{m}#3,
     *                             language_code{r}#18, language_name{r}#19]]
     *     | \_Eval[[null[INTEGER] AS language_code#18, null[KEYWORD] AS language_name#19]]
     *     |   \_Limit[10[INTEGER],false,false]
     *     |     \_Filter[KNN(rgb_vector{f}#14,[0.0, 120.0, 0.0][DENSE_VECTOR])]
     *     |       \_EsRelation[colors][color{f}#11, hex_code{f}#12, id{f}#10, primary{f}#1..]
     */
    public void testPushDownKnnPastUnionAll() {
        var plan = planSubquery("""
            from colors, (from languages) metadata _score
            | where knn(rgb_vector, "007800")
            | sort _score desc, color asc
            | keep color, rgb_vector, language_name
            | limit 10
            """);

        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        UnionAll unionAll = as(topN.child(), UnionAll.class);

        // the last child is pruned, since it becomes an empty LocalRelation since the filter cannot be applied
        assertEquals(1, unionAll.children().size());

        Project esqlProject = as(unionAll.children().get(0), Project.class);
        Eval eval = as(esqlProject.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        Limit limit = as(eval.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Knn knn = as(filter.condition(), Knn.class);
        FieldAttribute rgb_vector = as(knn.field(), FieldAttribute.class);
        assertEquals("rgb_vector", rgb_vector.name());
        // knn should have an implicitK set
        assertNotNull(knn.implicitK());
        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("colors", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#32, emp_no{r}#33, first_name{r}#34, gender{r}#35, hire_date{r}#36, job{r}#37, job.raw{r}#38,
     *                    languages{r}#39, last_name{r}#40, long_noidx{r}#41, salary{r}#42, language_code{r}#43, language_name{r}#44]]
     *   |_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                    languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#19, language_name{r}#20]]
     *   | \_Eval[[null[INTEGER] AS language_code#19, null[KEYWORD] AS language_name#20]]
     *   |   \_Subquery[]
     *   |     \_TopN[[Order[last_name{f}#10,ASC,LAST]],10000[INTEGER],false]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   \_Project[[_meta_field{r}#21, emp_no{r}#22, first_name{r}#23, gender{r}#24, hire_date{r}#25, job{r}#26, job.raw{r}#27,
     *                    languages{r}#28, last_name{r}#29, long_noidx{r}#30, salary{r}#31, language_code{f}#17, language_name{f}#18]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#21, null[INTEGER] AS emp_no#22, null[KEYWORD] AS first_name#23,
     *                 null[TEXT] AS gender#24, null[DATETIME] AS hire_date#25, null[TEXT] AS job#26, null[KEYWORD] AS job.raw#27,
     *                 null[INTEGER] AS languages#28, null[KEYWORD] AS last_name#29, null[LONG] AS long_noidx#30,
     *                 null[INTEGER] AS salary#31]]
     *       \_Subquery[]
     *         \_TopN[[Order[language_name{f}#18,ASC,LAST]],10000[INTEGER],false]
     *           \_Filter[language_code{f}#17 > 0[INTEGER]]
     *             \_EsRelation[languages][language_code{f}#17, language_name{f}#18]
     */
    public void testSortInSubquery() {
        var plan = planSubquery("""
            FROM
                (FROM test
                 | SORT last_name),
                (FROM languages
                 | WHERE language_code > 0
                 | SORT language_name
                )
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project project = as(unionAll.children().get(0), Project.class);
        Eval eval = as(project.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        TopN topN = as(subquery.child(), TopN.class);
        EsRelation relation = as(topN.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        project = as(unionAll.children().get(1), Project.class);
        eval = as(project.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        topN = as(subquery.child(), TopN.class);
        Filter filter = as(topN.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("language_code", language_code.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *             languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   |_Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *              languages{f}#11, last_name{f}#12, long_noidx{f}#18, salary{f}#13]]
     *   | \_Filter[emp_no{f}#8 > 10[INTEGER]]
     *   |   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *   \_Project[[_meta_field{r}#19, emp_no{r}#4, first_name{r}#20, gender{r}#21, hire_date{r}#22, job{r}#23, job.raw{r}#24,
     *              languages{r}#25, last_name{r}#26, long_noidx{r}#27, salary{r}#6]]
     *     \_Eval[[null[KEYWORD] AS _meta_field#19, null[KEYWORD] AS first_name#20, null[TEXT] AS gender#21,
     *             null[DATETIME] AS hire_date#22, null[TEXT] AS job#23, null[KEYWORD] AS job.raw#24, null[INTEGER] AS languages#25,
     *             null[KEYWORD] AS last_name#26, null[LONG] AS long_noidx#27]]
     *       \_Subquery[]
     *         \_LocalRelation[[emp_no{r}#4, salary{r}#6],Page{blocks=[IntVectorBlock[vector=ConstantIntVector[positions=1, value=100]],
     *                          IntVectorBlock[vector=ConstantIntVector[positions=1, value=50000]]]}]
     */
    public void testPushDownSimpleFilterPastUnionAllWithRowSubquery() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (ROW emp_no = 100, salary = 50000)
            | WHERE emp_no > 10
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // index leg: Project → Filter[emp_no > 10] → EsRelation[test]
        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter indexFilter = as(child1.child(), Filter.class);
        GreaterThan indexGt = as(indexFilter.condition(), GreaterThan.class);
        FieldAttribute indexEmpNo = as(indexGt.left(), FieldAttribute.class);
        assertEquals("emp_no", indexEmpNo.name());
        Literal indexThreshold = as(indexGt.right(), Literal.class);
        assertEquals(10, indexThreshold.value());
        EsRelation indexRelation = as(indexFilter.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());

        // ROW leg: Project → Eval[null evals for missing test fields] → Subquery → LocalRelation.
        // The pushed-down `emp_no > 10` filter is constant-folded against the ROW values
        // (`100 > 10 == true`) so the Filter node is removed and the LocalRelation is preserved.
        Project child2 = as(unionAll.children().get(1), Project.class);
        Eval rowMissingEval = as(child2.child(), Eval.class);
        assertEquals(9, rowMissingEval.fields().size()); // 11 test fields - emp_no - salary
        Subquery subquery = as(rowMissingEval.child(), Subquery.class);
        as(subquery.child(), LocalRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *             languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   \_Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *              languages{f}#11, last_name{f}#12, long_noidx{f}#18, salary{f}#13]]
     *     \_Filter[emp_no{f}#8 > 10[INTEGER]]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testPushDownSimpleFilterPrunesRowBranch() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (ROW emp_no = 1, salary = 100)
            | WHERE emp_no > 10
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(1, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter indexFilter = as(child1.child(), Filter.class);
        GreaterThan indexGt = as(indexFilter.condition(), GreaterThan.class);
        FieldAttribute indexEmpNo = as(indexGt.left(), FieldAttribute.class);
        assertEquals("emp_no", indexEmpNo.name());
        EsRelation indexRelation = as(indexFilter.child(), EsRelation.class);
        assertEquals("test", indexRelation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[emp_no{r}#16, salary{r}#17]]
     *   |_Project[[emp_no{r}#4, salary{r}#6]]
     *   | \_Subquery[]
     *   |   \_LocalRelation[[emp_no{r}#4, salary{r}#6],Page{blocks=[IntVectorBlock[vector=ConstantIntVector[positions=1, value=100]],
     *                        IntVectorBlock[vector=ConstantIntVector[positions=1, value=50000]]]}]
     *   \_Project[[emp_no{r}#8, salary{r}#10]]
     *     \_Subquery[]
     *       \_LocalRelation[[emp_no{r}#8, salary{r}#10],Page{blocks=[IntVectorBlock[vector=ConstantIntVector[positions=1, value=200]],
     *                        IntVectorBlock[vector=ConstantIntVector[positions=1, value=80000]]]}]
     */
    public void testPushDownFilterPastUnionAllWithRowOnlySubqueries() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM (ROW emp_no = 100, salary = 50000)
               , (ROW emp_no = 200, salary = 80000)
               , (ROW emp_no = 10,  salary = 5000)
            | WHERE emp_no > 50
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // First two legs (emp_no=100, emp_no=200) survive; the third (emp_no=10) is pruned.
        assertEquals(2, unionAll.children().size());

        for (int i = 0; i < 2; i++) {
            Project childProject = as(unionAll.children().get(i), Project.class);
            Subquery subquery = as(childProject.child(), Subquery.class);
            // The ROW values (100, 200) both satisfy emp_no > 50, so the pushed-down filter
            // constant-folds away and only the bare LocalRelation remains.
            as(subquery.child(), LocalRelation.class);
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#27, emp_no{r}#28, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *             languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *   \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *              languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *     \_Filter[first_name{f}#7 == Bob[KEYWORD]]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testPushDownFilterPrunesRowBranchWithoutTheField() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (ROW emp_no = 1)
            | WHERE first_name == "Bob"
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // The ROW leg is pruned because its first_name is null and `null == "Bob"` is false.
        assertEquals(1, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter filter = as(child1.child(), Filter.class);
        as(filter.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *             languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   \_Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *              languages{f}#11, last_name{f}#12, long_noidx{f}#18, salary{f}#13]]
     *     \_Filter[:(first_name{f}#9,Bob[KEYWORD])]
     *       \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     */
    public void testPushDownFullTextMatchOperatorPastUnionAllWithRowSubquery() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (ROW emp_no = 1, salary = 50000)
            | WHERE first_name:"Bob"
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // ROW leg is pruned — first_name is null in the ROW so the pushed-down match folds to false.
        assertEquals(1, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter indexFilter = as(child1.child(), Filter.class);
        MatchOperator indexMatch = as(indexFilter.condition(), MatchOperator.class);
        FieldAttribute indexFirstName = as(indexMatch.field(), FieldAttribute.class);
        assertEquals("first_name", indexFirstName.name());
        Literal indexQuery = as(indexMatch.query(), Literal.class);
        assertEquals(new BytesRef("Bob"), indexQuery.value());
        as(indexFilter.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#41, emp_no{r}#42, first_name{r}#43, gender{r}#44, hire_date{r}#45, job{r}#46, job.raw{r}#47,
     *             languages{r}#48, last_name{r}#49, long_noidx{r}#50, salary{r}#51]]
     *   |_Project[[_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18, job.raw{f}#19,
     *              languages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15]]
     *   | \_Filter[:(first_name{f}#11,first[KEYWORD]) AND MATCH(last_name{f}#14,last[KEYWORD]) AND
     *              QSTR(gender:female[KEYWORD]) AND KQL(first_name:bob[KEYWORD])]
     *   |   \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *   \_Project[[_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, gender{f}#23, hire_date{f}#28, job{f}#29, job.raw{f}#30,
     *              languages{f}#24, last_name{f}#25, long_noidx{f}#31, salary{f}#26]]
     *     \_Subquery[]
     *       \_Filter[languages{f}#24 > 0[INTEGER] AND :(first_name{f}#22,first[KEYWORD]) AND
     *                MATCH(last_name{f}#25,last[KEYWORD]) AND QSTR(gender:female[KEYWORD]) AND KQL(first_name:bob[KEYWORD])]
     *         \_EsRelation[test][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     */
    public void testPushDownConjunctiveFullTextFunctionsPastUnionAllWithRowSubquery() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (ROW emp_no = 1, salary = 50000)
            | WHERE first_name:"first" AND match(last_name, "last") AND qstr("gender:female") AND kql("first_name:bob")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // The ROW leg is pruned, only the two FROM legs remain.
        assertEquals(2, unionAll.children().size());

        // index leg 1: Project → Filter[<full text conjunction>] → EsRelation
        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter indexFilter1 = as(child1.child(), Filter.class);
        assertFullTextConjunction(indexFilter1.condition());
        as(indexFilter1.child(), EsRelation.class);

        // index leg 2: Project → Subquery → Filter[<conjunction> AND languages > 0] → EsRelation
        Project child2 = as(unionAll.children().get(1), Project.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        Filter indexFilter2 = as(subquery.child(), Filter.class);
        And outer = as(indexFilter2.condition(), And.class);
        // The pre-existing `languages > 0` from the subquery sits on the left, the pushed-down
        // full-text conjunction sits on the right.
        GreaterThan languagesGt = as(outer.left(), GreaterThan.class);
        assertEquals("languages", as(languagesGt.left(), FieldAttribute.class).name());
        assertEquals(0, as(languagesGt.right(), Literal.class).value());
        assertFullTextConjunction(outer.right());
        as(indexFilter2.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *             languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   \_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *              languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12]]
     *     \_Filter[:(first_name{f}#8,first[KEYWORD]) OR MatchPhrase(last_name{f}#11,last[KEYWORD])]
     *       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     */
    public void testPushDownDisjunctiveFullTextFunctionPastUnionAllWithRowSubquery() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (ROW emp_no = 1)
            | WHERE first_name:"first" OR match_phrase(last_name, "last")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // ROW leg is pruned — every disjunct matches against a null and folds to false.
        assertEquals(1, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter indexFilter = as(child1.child(), Filter.class);
        Or or = as(indexFilter.condition(), Or.class);
        MatchOperator matchOperator = as(or.left(), MatchOperator.class);
        assertEquals("first_name", as(matchOperator.field(), FieldAttribute.class).name());
        assertEquals(new BytesRef("first"), as(matchOperator.query(), Literal.class).value());
        MatchPhrase matchPhrase = as(or.right(), MatchPhrase.class);
        assertEquals("last_name", as(matchPhrase.field(), FieldAttribute.class).name());
        assertEquals(new BytesRef("last"), as(matchPhrase.query(), Literal.class).value());
        as(indexFilter.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#29, emp_no{r}#30, first_name{r}#31, gender{r}#32, hire_date{r}#33, job{r}#34, job.raw{r}#35,
     *             languages{r}#36, last_name{r}#37, long_noidx{r}#38, salary{r}#39]]
     *   \_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *              languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     *     \_Filter[:(first_name{f}#10,Bob[KEYWORD]) AND emp_no{f}#9 > 10[INTEGER]]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testPushDownMixedFullTextAndComparisonPastUnionAllWithRowSubquery() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM test, (ROW emp_no = 100, salary = 50000)
            | WHERE first_name:"Bob" AND emp_no > 10
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        // ROW leg pruned — `first_name:"Bob"` evaluates to false on the null-filled first_name.
        assertEquals(1, unionAll.children().size());

        Project child1 = as(unionAll.children().get(0), Project.class);
        Filter indexFilter = as(child1.child(), Filter.class);
        And and = as(indexFilter.condition(), And.class);
        MatchOperator matchOperator = as(and.left(), MatchOperator.class);
        assertEquals("first_name", as(matchOperator.field(), FieldAttribute.class).name());
        assertEquals(new BytesRef("Bob"), as(matchOperator.query(), Literal.class).value());
        GreaterThan gt = as(and.right(), GreaterThan.class);
        assertEquals("emp_no", as(gt.left(), FieldAttribute.class).name());
        assertEquals(10, as(gt.right(), Literal.class).value());
        as(indexFilter.child(), EsRelation.class);
    }

    private static void assertFullTextConjunction(Expression condition) {
        And outer = as(condition, And.class);
        And leftAnd = as(outer.left(), And.class);
        MatchOperator matchOperator = as(leftAnd.left(), MatchOperator.class);
        assertEquals("first_name", as(matchOperator.field(), FieldAttribute.class).name());
        assertEquals(new BytesRef("first"), as(matchOperator.query(), Literal.class).value());
        Match matchFunction = as(leftAnd.right(), Match.class);
        assertEquals("last_name", as(matchFunction.field(), FieldAttribute.class).name());
        assertEquals(new BytesRef("last"), as(matchFunction.query(), Literal.class).value());

        And rightAnd = as(outer.right(), And.class);
        QueryString queryString = as(rightAnd.left(), QueryString.class);
        assertEquals(new BytesRef("gender:female"), as(queryString.query(), Literal.class).value());
        Kql kql = as(rightAnd.right(), Kql.class);
        assertEquals(new BytesRef("first_name:bob"), as(kql.query(), Literal.class).value());
    }

    // ============================================================================================
    // The following tests mirror the FROM-subquery push-down tests above but exercise subqueries
    // sourced from the TS command. The TS branch is wrapped in a Subquery node and resolves over
    // an EsRelation in TIME_SERIES mode.
    // ============================================================================================

    /*
     * The outer "@timestamp > 2024-01-01" predicate is pushed past the UnionAll into both branches.
     * - sample_data branch: the predicate lands directly above the EsRelation (under the Eval that
     *   widens the schema to match the UnionAll's combined attributes).
     * - TS k8s branch: the outer predicate is combined with the subquery's existing
     *   "WHERE @timestamp > 2025-10-07"; constant folding collapses the AND into the stricter
     *   "@timestamp > 2025-10-07".
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#135, client_ip{r}#136, event_duration{r}#137, message{r}#138, client.ip{r}#139,
     *                    cluster{r}#140, event{r}#141, ..., network.total_bytes_in{r}#156, network.total_cost{r}#157, pod{r}#158]]
     *   |_Project[[@timestamp{f}#84, client_ip{f}#85, event_duration{f}#86, message{f}#87, client.ip{r}#112,
     *                            cluster{r}#113, event{r}#114, ..., network.total_bytes_in{r}#129, network.total_cost{r}#130, pod{r}#131]]
     *   | \_Eval[[null[IP] AS client.ip#112, null[KEYWORD] AS cluster#113, null[KEYWORD] AS event#114, ...,
     *                  null[LONG] AS network.total_bytes_in#129, null[DOUBLE] AS network.total_cost#130, null[KEYWORD] AS pod#131]]
     *   |   \_Filter[@timestamp{f}#84 > 1704067200000[DATETIME]]
     *   |     \_EsRelation[sample_data][@timestamp{f}#84, client_ip{f}#85, event_duration{f..]
     *   \_Project[[@timestamp{f}#88, client_ip{r}#132, event_duration{r}#133, message{r}#134, client.ip{f}#92, cluster{f}#89,
     *                           event{f}#93, ..., network.total_bytes_in{r}#159, network.total_cost{r}#160, pod{f}#90]]
     *     \_Eval[[null[IP] AS client_ip#132, null[LONG] AS event_duration#133, null[KEYWORD] AS message#134,
     *                  TOLONG(network.total_bytes_in{f}#102) AS network.total_bytes_in#159,
     *                  TODOUBLE(network.total_cost{f}#104) AS network.total_cost#160]]
     *       \_Subquery[]
     *         \_Filter[@timestamp{f}#88 > 1759795200000[DATETIME]]
     *           \_EsRelation[k8s][@timestamp{f}#88, client.ip{f}#92, cluster{f}#89, e..]
     */
    public void testPushDownSimpleFilterPastUnionAllWithTsSubquery() {
        checkSubqueryWithTSCommand();
        var plan = planSubquery("""
            FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07")
            | WHERE @timestamp > "2024-01-01"
            """);

        Configuration configuration = randomConfigurationBuilder().setting(QuerySettings.TIME_ZONE, ZoneOffset.UTC).build();

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // branch 1 — sample_data: outer @timestamp filter pushed past Eval directly above EsRelation.
        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        GreaterThan sampleGt = as(sampleFilter.condition(), GreaterThan.class);
        FieldAttribute sampleTimestamp = as(sampleGt.left(), FieldAttribute.class);
        assertEquals("@timestamp", sampleTimestamp.name());
        Literal sampleValue = as(sampleGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), sampleValue.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        // branch 2 — TS k8s: outer predicate is combined with the subquery's WHERE @timestamp.
        // After constant folding, "@timestamp > 2024-01-01 AND @timestamp > 2025-10-07" collapses
        // to a single predicate "@timestamp > 2025-10-07".
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        GreaterThan tsGt = as(tsFilter.condition(), GreaterThan.class);
        FieldAttribute tsTimestamp = as(tsGt.left(), FieldAttribute.class);
        assertEquals("@timestamp", tsTimestamp.name());
        Literal tsValue = as(tsGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-10-07", DataType.DATETIME, configuration), tsValue.value());
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
    }

    /*
     * Conjunctive outer predicate "@timestamp > X AND @timestamp < Y" is pushed past
     * the UnionAll into each branch; the TS branch combines it with its existing WHERE filter.
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#373, client_ip{r}#374, event_duration{r}#375, message{r}#376, client.ip{r}#377,
     *                    cluster{r}#378, event{r}#379, ..., network.total_bytes_in{r}#394, network.total_cost{r}#395, pod{r}#396]]
     *   |_Project[[@timestamp{f}#322, client_ip{f}#323, event_duration{f}#324, message{f}#325, client.ip{r}#350,
     *                            cluster{r}#351, event{r}#352, ..., network.total_bytes_in{r}#367, network.total_cost{r}#368, pod{r}#369]]
     *   | \_Eval[[null[IP] AS client.ip#350, null[KEYWORD] AS cluster#351, null[KEYWORD] AS event#352, ...,
     *                  null[LONG] AS network.total_bytes_in#367, null[DOUBLE] AS network.total_cost#368, null[KEYWORD] AS pod#369]]
     *   |   \_Filter[@timestamp{f}#322 > 1704067200000[DATETIME] AND @timestamp{f}#322 < 1767139200000[DATETIME]]
     *   |     \_EsRelation[sample_data][@timestamp{f}#322, client_ip{f}#323, event_duration..]
     *   \_Project[[@timestamp{f}#326, client_ip{r}#370, event_duration{r}#371, message{r}#372, client.ip{f}#330, cluster{f}#327,
     *                           event{f}#331, ..., network.total_bytes_in{r}#397, network.total_cost{r}#398, pod{f}#328]]
     *     \_Eval[[null[IP] AS client_ip#370, null[LONG] AS event_duration#371, null[KEYWORD] AS message#372,
     *                  TOLONG(network.total_bytes_in{f}#340) AS network.total_bytes_in#397,
     *                  TODOUBLE(network.total_cost{f}#342) AS network.total_cost#398]]
     *       \_Subquery[]
     *         \_Filter[@timestamp{f}#326 > 1759795200000[DATETIME] AND @timestamp{f}#326 < 1767139200000[DATETIME]]
     *           \_EsRelation[k8s][@timestamp{f}#326, client.ip{f}#330, cluster{f}#327, ..]
     */
    public void testPushDownConjunctiveFilterPastUnionAllWithTsSubquery() {
        checkSubqueryWithTSCommand();
        var plan = planSubquery("""
            FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07")
            | WHERE @timestamp > "2024-01-01" AND @timestamp < "2025-12-31"
            """);

        Configuration configuration = randomConfigurationBuilder().setting(QuerySettings.TIME_ZONE, ZoneOffset.UTC).build();

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        // branch 1 — sample_data: Filter[@timestamp > "2024-01-01" AND @timestamp < "2025-12-31"]
        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        And sampleAnd = as(sampleFilter.condition(), And.class);
        GreaterThan sampleGt = as(sampleAnd.left(), GreaterThan.class);
        assertEquals("@timestamp", as(sampleGt.left(), FieldAttribute.class).name());
        Literal sampleValue = as(sampleGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), sampleValue.value());
        LessThan sampleLt = as(sampleAnd.right(), LessThan.class);
        assertEquals("@timestamp", as(sampleLt.left(), FieldAttribute.class).name());
        sampleValue = as(sampleLt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-12-31", DataType.DATETIME, configuration), sampleValue.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        // branch 2 — TS k8s: original "@timestamp > 2025-10-07" combined with outer conjunction.
        // The "@timestamp > 2024-01-01" branch is absorbed because of the stricter inner predicate,
        // leaving Filter[@timestamp > 2025-10-07 AND @timestamp < 2025-12-31].
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        And tsAnd = as(tsFilter.condition(), And.class);
        GreaterThan tsGt = as(tsAnd.left(), GreaterThan.class);
        assertEquals("@timestamp", as(tsGt.left(), FieldAttribute.class).name());
        Literal tsValue = as(tsGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-10-07", DataType.DATETIME, configuration), tsValue.value());
        LessThan tsLt = as(tsAnd.right(), LessThan.class);
        assertEquals("@timestamp", as(tsLt.left(), FieldAttribute.class).name());
        tsValue = as(tsLt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-12-31", DataType.DATETIME, configuration), tsValue.value());
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
    }

    /*
     * Disjunctive outer predicate "@timestamp > X OR @timestamp < Y" is pushed past the UnionAll
     * into both branches.
     * - sample_data branch: the Or sits unchanged above the EsRelation.
     * - TS k8s branch: the inner WHERE "@timestamp > 2025-10-07" is combined with the outer Or as
     *   And(inner_gt, Or(outer_gt, outer_lt)).
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#453, client_ip{r}#454, event_duration{r}#455, message{r}#456, client.ip{r}#457,
     *                    cluster{r}#458, event{r}#459, ..., network.total_bytes_in{r}#474, network.total_cost{r}#475, pod{r}#476]]
     *   |_Project[[@timestamp{f}#402, client_ip{f}#403, event_duration{f}#404, message{f}#405, client.ip{r}#430,
     *                            cluster{r}#431, event{r}#432, ..., network.total_bytes_in{r}#447, network.total_cost{r}#448, pod{r}#449]]
     *   | \_Eval[[null[IP] AS client.ip#430, null[KEYWORD] AS cluster#431, null[KEYWORD] AS event#432, ...,
     *                  null[LONG] AS network.total_bytes_in#447, null[DOUBLE] AS network.total_cost#448, null[KEYWORD] AS pod#449]]
     *   |   \_Filter[@timestamp{f}#402 > 1704067200000[DATETIME] OR @timestamp{f}#402 < 1577836800000[DATETIME]]
     *   |     \_EsRelation[sample_data][@timestamp{f}#402, client_ip{f}#403, event_duration..]
     *   \_Project[[@timestamp{f}#406, client_ip{r}#450, event_duration{r}#451, message{r}#452, client.ip{f}#410, cluster{f}#407,
     *                           event{f}#411, ..., network.total_bytes_in{r}#477, network.total_cost{r}#478, pod{f}#408]]
     *     \_Eval[[null[IP] AS client_ip#450, null[LONG] AS event_duration#451, null[KEYWORD] AS message#452,
     *                  TOLONG(network.total_bytes_in{f}#420) AS network.total_bytes_in#477,
     *                  TODOUBLE(network.total_cost{f}#422) AS network.total_cost#478]]
     *       \_Subquery[]
     *         \_Filter[@timestamp{f}#406 > 1759795200000[DATETIME] AND
     *                       @timestamp{f}#406 > 1704067200000[DATETIME] OR @timestamp{f}#406 < 1577836800000[DATETIME]]
     *           \_EsRelation[k8s][@timestamp{f}#406, client.ip{f}#410, cluster{f}#407, ..]
     */
    public void testPushDownDisjunctiveFilterPastUnionAllWithTsSubquery() {
        checkSubqueryWithTSCommand();
        var plan = planSubquery("""
            FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07")
            | WHERE @timestamp > "2024-01-01" OR @timestamp < "2020-01-01"
            """);

        Configuration configuration = randomConfigurationBuilder().setting(QuerySettings.TIME_ZONE, ZoneOffset.UTC).build();

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // branch 1 — sample_data: a single Or sits above EsRelation.
        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        Or sampleOr = as(sampleFilter.condition(), Or.class);
        GreaterThan sampleGt = as(sampleOr.left(), GreaterThan.class);
        assertEquals("@timestamp", as(sampleGt.left(), FieldAttribute.class).name());
        Literal sampleValue = as(sampleGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), sampleValue.value());
        LessThan sampleLt = as(sampleOr.right(), LessThan.class);
        assertEquals("@timestamp", as(sampleLt.left(), FieldAttribute.class).name());
        sampleValue = as(sampleLt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2020-01-01", DataType.DATETIME, configuration), sampleValue.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        // branch 2 — TS k8s: the in-subquery WHERE is combined with the outer OR via
        // And(inner_gt, Or(outer_gt, outer_lt)). The optimizer keeps the inner @timestamp gt
        // on the left of the And because it is the tightest bound.
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        And tsAnd = as(tsFilter.condition(), And.class);
        GreaterThan innerGt = as(tsAnd.left(), GreaterThan.class);
        assertEquals("@timestamp", as(innerGt.left(), FieldAttribute.class).name());
        Literal tsValue = as(innerGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-10-07", DataType.DATETIME, configuration), tsValue.value());
        Or tsOr = as(tsAnd.right(), Or.class);
        GreaterThan outerGt = as(tsOr.left(), GreaterThan.class);
        assertEquals("@timestamp", as(outerGt.left(), FieldAttribute.class).name());
        tsValue = as(outerGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), tsValue.value());
        LessThan outerLt = as(tsOr.right(), LessThan.class);
        assertEquals("@timestamp", as(outerLt.left(), FieldAttribute.class).name());
        tsValue = as(outerLt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2020-01-01", DataType.DATETIME, configuration), tsValue.value());
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
    }

    /*
     * A full-text predicate outside the UnionAll on a field that exists only in one branch
     * (`message` is present in `sample_data` but not in the TS branch).
     * The TS branch is pruned (its `message` reference is null and cannot be matched),
     * leaving the single sample_data leg with the MatchOperator pushed onto the EsRelation.
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#56, client_ip{r}#57, event_duration{r}#58, message{r}#59, client.ip{r}#60,
     *                    cluster{r}#61, event{r}#62, ..., network.total_bytes_in{r}#77, network.total_cost{r}#78, pod{r}#79]]
     *   \_Project[[@timestamp{f}#5, client_ip{f}#6, event_duration{f}#7, message{f}#8, client.ip{r}#33, cluster{r}#34,
     *                            event{r}#35, ..., network.total_bytes_in{r}#50, network.total_cost{r}#51, pod{r}#52]]
     *     \_Eval[[null[IP] AS client.ip#33, null[KEYWORD] AS cluster#34, null[KEYWORD] AS event#35, ...,
     *                  null[LONG] AS network.total_bytes_in#50, null[DOUBLE] AS network.total_cost#51, null[KEYWORD] AS pod#52]]
     *       \_Filter[:(message{f}#8,connect[KEYWORD])]
     *         \_EsRelation[sample_data][@timestamp{f}#5, client_ip{f}#6, event_duration{f}#..]
     */
    public void testPushDownFullTextFunctionPastUnionAllWithTsSubqueryPrunesTsBranch() {
        checkSubqueryWithTSCommand();
        var plan = planSubquery("""
            FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07")
            | WHERE message:"connect"
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(1, unionAll.children().size());

        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        MatchOperator match = as(sampleFilter.condition(), MatchOperator.class);
        FieldAttribute messageField = as(match.field(), FieldAttribute.class);
        assertEquals("message", messageField.name());
        Literal queryLiteral = as(match.query(), Literal.class);
        assertEquals(new BytesRef("connect"), queryLiteral.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
        // ts branch is pruned as there is no message field
    }

    /*
     * Conjunctive full-text predicate "message:'connect' AND qstr('message:disconnect')" is pushed past the
     * UnionAll. The TS k8s branch has no `message` field, so it cannot satisfy either match function and is
     * pruned, leaving only the sample_data leg with the combined filter above the EsRelation.
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#214, client_ip{r}#215, event_duration{r}#216, message{r}#217, client.ip{r}#218,
     *                    cluster{r}#219, event{r}#220, ..., network.total_bytes_in{r}#235, network.total_cost{r}#236, pod{r}#237]]
     *   \_Project[[@timestamp{f}#163, client_ip{f}#164, event_duration{f}#165, message{f}#166, client.ip{r}#191,
     *                            cluster{r}#192, event{r}#193, ..., network.total_bytes_in{r}#208, network.total_cost{r}#209, pod{r}#210]]
     *     \_Eval[[null[IP] AS client.ip#191, null[KEYWORD] AS cluster#192, null[KEYWORD] AS event#193, ...,
     *                  null[LONG] AS network.total_bytes_in#208, null[DOUBLE] AS network.total_cost#209, null[KEYWORD] AS pod#210]]
     *       \_Filter[:(message{f}#166,connect[KEYWORD]) AND QSTR(message:disconnect[KEYWORD])]
     *         \_EsRelation[sample_data][@timestamp{f}#163, client_ip{f}#164, event_duration..]
     */
    public void testPushDownConjunctiveFullTextFunctionPastUnionAllWithTsSubquery() {
        checkSubqueryWithTSCommand();
        var plan = planSubquery("""
            FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07")
            | WHERE message:"connect" AND qstr("message:disconnect")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(1, unionAll.children().size());

        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        And and = as(sampleFilter.condition(), And.class);
        MatchOperator match = as(and.left(), MatchOperator.class);
        FieldAttribute messageField = as(match.field(), FieldAttribute.class);
        assertEquals("message", messageField.name());
        QueryString qstr = as(and.right(), QueryString.class);
        Literal qstrQuery = as(qstr.query(), Literal.class);
        assertEquals(new BytesRef("message:disconnect"), qstrQuery.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());
    }

    /*
     * Mixed conjunctive predicate combining a range filter on @timestamp with a full-text qstr
     * function. Both halves can be evaluated against the TS k8s branch (qstr does not require an
     * Elasticsearch field reference), so the predicate is pushed past the UnionAll into both legs.
     * - sample_data branch: Filter[@timestamp > 2024-01-01 AND QSTR(message:disconnect)].
     * - TS k8s branch: combined with the inner WHERE; constant folding collapses
     *   "@timestamp > 2024-01-01 AND @timestamp > 2025-10-07" into the stricter
     *   "@timestamp > 2025-10-07", leaving Filter[QSTR(...) AND @timestamp > 2025-10-07].
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#293, client_ip{r}#294, event_duration{r}#295, message{r}#296, client.ip{r}#297,
     *                    cluster{r}#298, event{r}#299, ..., network.total_bytes_in{r}#314, network.total_cost{r}#315, pod{r}#316]]
     *   |_Project[[@timestamp{f}#242, client_ip{f}#243, event_duration{f}#244, message{f}#245, client.ip{r}#270,
     *                            cluster{r}#271, event{r}#272, ..., network.total_bytes_in{r}#287, network.total_cost{r}#288, pod{r}#289]]
     *   | \_Eval[[null[IP] AS client.ip#270, null[KEYWORD] AS cluster#271, null[KEYWORD] AS event#272, ...,
     *                  null[LONG] AS network.total_bytes_in#287, null[DOUBLE] AS network.total_cost#288, null[KEYWORD] AS pod#289]]
     *   |   \_Filter[@timestamp{f}#242 > 1704067200000[DATETIME] AND QSTR(message:disconnect[KEYWORD])]
     *   |     \_EsRelation[sample_data][@timestamp{f}#242, client_ip{f}#243, event_duration..]
     *   \_Project[[@timestamp{f}#246, client_ip{r}#290, event_duration{r}#291, message{r}#292, client.ip{f}#250, cluster{f}#247,
     *                           event{f}#251, ..., network.total_bytes_in{r}#317, network.total_cost{r}#318, pod{f}#248]]
     *     \_Eval[[null[IP] AS client_ip#290, null[LONG] AS event_duration#291, null[KEYWORD] AS message#292,
     *                  TOLONG(network.total_bytes_in{f}#260) AS network.total_bytes_in#317,
     *                  TODOUBLE(network.total_cost{f}#262) AS network.total_cost#318]]
     *       \_Subquery[]
     *         \_Filter[QSTR(message:disconnect[KEYWORD]) AND @timestamp{f}#246 > 1759795200000[DATETIME]]
     *           \_EsRelation[k8s][@timestamp{f}#246, client.ip{f}#250, cluster{f}#247, ..]
     */
    public void testPushDownMixedFilterAndFullTextFunctionPastUnionAllWithTsSubquery() {
        checkSubqueryWithTSCommand();
        var plan = planSubquery("""
            FROM sample_data, (TS k8s | WHERE @timestamp > "2025-10-07")
            | WHERE @timestamp > "2024-01-01" AND qstr("message:disconnect")
            """);

        Configuration configuration = randomConfigurationBuilder().setting(QuerySettings.TIME_ZONE, ZoneOffset.UTC).build();

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        And and = as(sampleFilter.condition(), And.class);
        GreaterThan sampleGT = as(and.left(), GreaterThan.class);
        assertEquals("@timestamp", as(sampleGT.left(), FieldAttribute.class).name());
        Literal sampleValue = as(sampleGT.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), sampleValue.value());
        QueryString qstr = as(and.right(), QueryString.class);
        Literal qstrQuery = as(qstr.query(), Literal.class);
        assertEquals(new BytesRef("message:disconnect"), qstrQuery.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        and = as(tsFilter.condition(), And.class);
        GreaterThan gt = as(and.right(), GreaterThan.class);
        assertEquals("@timestamp", as(gt.left(), FieldAttribute.class).name());
        Literal tsValue = as(gt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-10-07", DataType.DATETIME, configuration), tsValue.value());
        qstr = as(and.left(), QueryString.class);
        qstrQuery = as(qstr.query(), Literal.class);
        assertEquals(new BytesRef("message:disconnect"), qstrQuery.value());
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());
    }

    /*
     * Push-down test mixing all three subquery flavours in a single UnionAll: a main FROM
     * index pattern (sample_data), a TS subquery, a FROM subquery and a ROW subquery. The outer
     * "@timestamp > 2024-01-01" predicate is pushed past the UnionAll into every branch:
     * - sample_data branch: the predicate lands directly above the EsRelation.
     * - TS k8s branch: combined with the inner WHERE @timestamp > 2025-10-07; constant folding
     *   collapses the AND to the stricter "@timestamp > 2025-10-07".
     * - FROM sample_data subquery: combined with the inner WHERE client_ip == "172.21.0.5".
     * - ROW branch: the pushed-down filter constant-folds against the literal
     *   (2026-01-01 > 2024-01-01 == true), so the Filter is removed and the LocalRelation is
     *   preserved under the Subquery wrapper.
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[@timestamp{r}#..., client_ip{r}#..., ..., cluster{r}#..., ..., pod{r}#...]]
     *   |_Project[[@timestamp{f}#..., client_ip{f}#..., event_duration{f}#..., message{f}#..., ...]]
     *   | \_Eval[[null fills for missing k8s fields]]
     *   |   \_Filter[@timestamp{f}#... > 1704067200000[DATETIME]]
     *   |     \_EsRelation[sample_data][@timestamp{f}#..., client_ip{f}#..., event_duration{f}#..]
     *   |_Project[[@timestamp{f}#..., client_ip{r}#..., ..., cluster{f}#..., ..., pod{f}#...]]
     *   | \_Eval[[null fills for sample_data fields, TOLONG/TODOUBLE counter demotions]]
     *   |   \_Subquery[]
     *   |     \_Filter[@timestamp{f}#... > 1759795200000[DATETIME]]
     *   |       \_EsRelation[k8s][@timestamp{f}#..., client.ip{f}#..., cluster{f}#..., ..]
     *   |_Project[[@timestamp{f}#..., client_ip{f}#..., event_duration{f}#..., message{f}#..., ...]]
     *   | \_Eval[[null fills for missing k8s fields]]
     *   |   \_Subquery[]
     *   |     \_Filter[client_ip{f}#... == 172.21.0.5[IP] AND @timestamp{f}#... > 1704067200000[DATETIME]]
     *   |       \_EsRelation[sample_data][@timestamp{f}#..., client_ip{f}#..., event_duration{f}#..]
     *   \_Project[[@timestamp{r}#..., client_ip{r}#..., ..., cluster{r}#..., ..., pod{r}#...]]
     *     \_Eval[[null fills for every column except @timestamp]]
     *       \_Subquery[]
     *         \_LocalRelation[[@timestamp{r}#...], Page[...]]
     */
    public void testPushDownSimpleFilterPastUnionAllWithMixedTsRowAndFromSubqueries() {
        checkSubqueryWithTSCommand();
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
        var plan = planSubquery("""
            FROM sample_data,
                 (TS k8s | WHERE @timestamp > "2025-10-07"),
                 (FROM sample_data | WHERE client_ip == "172.21.0.5"),
                 (ROW @timestamp = "2026-01-01T00:00:00.000Z"::datetime)
            | WHERE @timestamp > "2024-01-01"
            """);

        Configuration configuration = randomConfigurationBuilder().setting(QuerySettings.TIME_ZONE, ZoneOffset.UTC).build();

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        // branch 0 — sample_data main: outer @timestamp filter pushed past Eval directly above EsRelation.
        Project sampleProject = as(unionAll.children().get(0), Project.class);
        Eval sampleEval = as(sampleProject.child(), Eval.class);
        Filter sampleFilter = as(sampleEval.child(), Filter.class);
        GreaterThan sampleGt = as(sampleFilter.condition(), GreaterThan.class);
        assertEquals("@timestamp", as(sampleGt.left(), FieldAttribute.class).name());
        Literal sampleValue = as(sampleGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), sampleValue.value());
        EsRelation sampleRelation = as(sampleFilter.child(), EsRelation.class);
        assertEquals("sample_data", sampleRelation.indexPattern());

        // branch 1 — TS k8s: outer predicate combined with the inner WHERE; constant folding
        // collapses "@timestamp > 2024-01-01 AND @timestamp > 2025-10-07" to the stricter
        // "@timestamp > 2025-10-07".
        Project tsProject = as(unionAll.children().get(1), Project.class);
        Eval tsEval = as(tsProject.child(), Eval.class);
        Subquery tsSubquery = as(tsEval.child(), Subquery.class);
        Filter tsFilter = as(tsSubquery.child(), Filter.class);
        GreaterThan tsGt = as(tsFilter.condition(), GreaterThan.class);
        assertEquals("@timestamp", as(tsGt.left(), FieldAttribute.class).name());
        Literal tsValue = as(tsGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2025-10-07", DataType.DATETIME, configuration), tsValue.value());
        EsRelation tsRelation = as(tsFilter.child(), EsRelation.class);
        assertEquals("k8s", tsRelation.indexPattern());

        // branch 2 — FROM sample_data subquery: outer @timestamp filter combined with the inner
        // WHERE client_ip == "172.21.0.5". The And keeps the subquery's original filter on the
        // left and the pushed-down predicate on the right.
        Project fromProject = as(unionAll.children().get(2), Project.class);
        Eval fromEval = as(fromProject.child(), Eval.class);
        Subquery fromSubquery = as(fromEval.child(), Subquery.class);
        Filter fromFilter = as(fromSubquery.child(), Filter.class);
        And fromAnd = as(fromFilter.condition(), And.class);
        // right side: pushed-down @timestamp predicate
        GreaterThan fromOuterGt = as(fromAnd.right(), GreaterThan.class);
        assertEquals("@timestamp", as(fromOuterGt.left(), FieldAttribute.class).name());
        Literal fromOuterValue = as(fromOuterGt.right(), Literal.class);
        assertEquals(EsqlDataTypeConverter.convert("2024-01-01", DataType.DATETIME, configuration), fromOuterValue.value());
        EsRelation fromRelation = as(fromFilter.child(), EsRelation.class);
        assertEquals("sample_data", fromRelation.indexPattern());

        // branch 3 — ROW @timestamp = 2026-01-01: pushed-down "@timestamp > 2024-01-01" folds to
        // true against the literal, so the Filter node is removed and only the LocalRelation
        // remains under the Subquery wrapper.
        Project rowProject = as(unionAll.children().get(3), Project.class);
        Eval rowEval = as(rowProject.child(), Eval.class);
        Subquery rowSubquery = as(rowEval.child(), Subquery.class);
        as(rowSubquery.child(), LocalRelation.class);
    }

    /*
     * The external dataset exposes the _file.* metadata columns, so the UnionAll carries them and the
     * final Project strips them back off; the test branch gains an Eval that nulls the _file.* columns
     * while the dataset branch gains an Eval that nulls the test columns absent from the dataset schema.
     *
     *Project[[...test columns, no _file.*...]]
     * \_Limit[1000[INTEGER]]
     *   \_UnionAll[[...test columns + _file.* columns...]]
     *     |_Project[[...]]
     *     | \_Eval[[null AS _file.path, _file.name, _file.directory, _file.size, _file.modified]]
     *     |   \_Filter[emp_no > 10000]
     *     |     \_EsRelation[test][...]
     *     |_EsqlProject[[...]]
     *       \_Eval[[null AS <test columns absent from the dataset schema>...]]
     *         \_Subquery[]
     *           \_Filter[salary > 50000 AND emp_no > 10000]
     *             \_ExternalRelation[s3://bucket/external_employees.parquet]
     */
    public void testPushDownFilterIntoExternalDatasetSubquery() {
        checkExternalDatasetSupport();
        var plan = planExternalDatasetSubquery("""
            FROM test, (FROM external_employees | WHERE salary > 50000)
            | WHERE emp_no > 10000
            """);

        // No top-level Project: the dataset is read via FROM with no METADATA, so it carries no
        // _file.* columns and the union output has nothing to strip from default output.
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // branch 1 — FROM test: the outer emp_no filter is pushed down onto the EsRelation.
        Project testProject = as(unionAll.children().get(0), Project.class);
        // No Eval on this branch: the union carries no _file.* (the dataset is FROM with no
        // METADATA), so the index branch already has every union column — nothing to null-fill.
        Filter testFilter = as(testProject.child(), Filter.class);
        GreaterThan testGt = as(testFilter.condition(), GreaterThan.class);
        assertEquals("emp_no", as(testGt.left(), FieldAttribute.class).name());
        assertEquals(10000, as(testGt.right(), Literal.class).value());
        EsRelation esRelation = as(testFilter.child(), EsRelation.class);
        assertEquals("test", esRelation.indexPattern());

        // branch 2 — FROM external_employees subquery: the pushed-down emp_no predicate is combined with
        // the subquery's own salary filter and lands directly above the ExternalRelation. The Eval fills
        // the UnionAll columns that exist in test but not in the external dataset schema with nulls.
        Project datasetProject = as(unionAll.children().get(1), Project.class);
        Eval datasetEval = as(datasetProject.child(), Eval.class);
        Subquery subquery = as(datasetEval.child(), Subquery.class);
        Filter datasetFilter = as(subquery.child(), Filter.class);
        And and = as(datasetFilter.condition(), And.class);
        GreaterThan salaryGt = as(and.left(), GreaterThan.class);
        assertEquals("salary", as(salaryGt.left(), Attribute.class).name());
        assertEquals(50000, as(salaryGt.right(), Literal.class).value());
        GreaterThan empGt = as(and.right(), GreaterThan.class);
        assertEquals("emp_no", as(empGt.left(), Attribute.class).name());
        assertEquals(10000, as(empGt.right(), Literal.class).value());
        as(datasetFilter.child(), ExternalRelation.class);
    }

    /*
     * Same _file.* handling as testPushDownFilterIntoExternalDatasetSubquery: the UnionAll carries the
     * external _file.* columns and the final Project strips them off.
     *
     *Project[[...test columns, no _file.*...]]
     * \_Limit[10000[INTEGER]]
     *   \_UnionAll[[...test columns + _file.* columns...]]
     *     |_Project[[...]]
     *     | \_Eval[[null AS _file.path, _file.name, _file.directory, _file.size, _file.modified]]
     *     |   \_Filter[emp_no > 10000]
     *     |     \_EsRelation[test][...]
     *     |_EsqlProject[[...]]
     *       \_Eval[[null AS <test columns absent from the dataset schema>...]]
     *         \_Subquery[]
     *           \_TopN[[Order[emp_no,ASC,LAST]],10000[INTEGER]]
     *             \_Filter[salary > 50000 AND emp_no > 10000]
     *               \_ExternalRelation[s3://bucket/external_employees.parquet]
     */
    public void testPushDownFilterAndLimitIntoExternalDatasetSubqueryWithSort() {
        checkExternalDatasetSupport();
        var plan = planExternalDatasetSubquery("""
            FROM test, (FROM external_employees | WHERE salary > 50000 | SORT emp_no)
            | WHERE emp_no > 10000
            """);

        // No top-level Project: the dataset is read via FROM with no METADATA, so it carries no
        // _file.* columns and the union output has nothing to strip from default output.
        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // branch 1 — FROM test: the outer emp_no filter is pushed down onto the EsRelation.
        Project testProject = as(unionAll.children().get(0), Project.class);
        // No Eval on this branch: the union carries no _file.* (the dataset is FROM with no
        // METADATA), so the index branch already has every union column — nothing to null-fill.
        Filter testFilter = as(testProject.child(), Filter.class);
        GreaterThan testGt = as(testFilter.condition(), GreaterThan.class);
        assertEquals("emp_no", as(testGt.left(), FieldAttribute.class).name());
        assertEquals(10000, as(testGt.right(), Literal.class).value());
        as(testFilter.child(), EsRelation.class);

        // branch 2 — FROM external_employees subquery
        Project datasetProject = as(unionAll.children().get(1), Project.class);
        Eval datasetEval = as(datasetProject.child(), Eval.class);
        Subquery subquery = as(datasetEval.child(), Subquery.class);
        TopN topN = as(subquery.child(), TopN.class);
        Literal topNLimit = as(topN.limit(), Literal.class);
        assertEquals(10000, topNLimit.value());
        Filter datasetFilter = as(topN.child(), Filter.class);
        And and = as(datasetFilter.condition(), And.class);
        GreaterThan salaryGt = as(and.left(), GreaterThan.class);
        assertEquals("salary", as(salaryGt.left(), Attribute.class).name());
        assertEquals(50000, as(salaryGt.right(), Literal.class).value());
        GreaterThan empGt = as(and.right(), GreaterThan.class);
        assertEquals("emp_no", as(empGt.left(), Attribute.class).name());
        assertEquals(10000, as(empGt.right(), Literal.class).value());
        as(datasetFilter.child(), ExternalRelation.class);
    }

    /*
     * Filter using match is pushed down into each subquery.
     * For the first subquery, the match operates over a mapped index field and will eventually be pushed down
     * to the shard as a Lucene query.
     * For the second subquery, the match operates over a non-mapped index field.
     *
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll
     *   |_Project
     *   | \_Filter[:(first_name{f}#9,first[KEYWORD])]
     *   |   \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *   \_Project
     *     \_Eval
     *       \_Subquery[]
     *         \_Filter[salary{r}#6 > 50000[INTEGER] AND :(first_name{r}#5,first[KEYWORD])]
     *           \_ExternalRelation[s3://bucket/external_employees.parquet][parquet][emp_no{r}#4, first_name{r}#5, salary{r}#6]
     */
    public void testFullTextFunctionOnExternalDatasetField() {
        checkExternalDatasetSupport();
        // : supports runtime search and can operate on external (non-index) fields
        var plan = planExternalDatasetSubquery("""
            FROM test, (FROM external_employees | WHERE salary > 50000)
            | WHERE first_name:"first"
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);

        Project firstProject = as(unionAll.children().get(0), Project.class);
        Filter firstFilter = as(firstProject.child(), Filter.class);
        Match match = as(firstFilter.condition(), Match.class);
        FieldAttribute fieldAttribute = as(match.field(), FieldAttribute.class);
        assertEquals("first_name", fieldAttribute.name());
        assertThat(firstFilter.child(), instanceOf(EsRelation.class));

        Project secondProject = as(unionAll.children().get(1), Project.class);
        Eval secondEval = as(secondProject.child(), Eval.class);
        Subquery subquery = as(secondEval.child(), Subquery.class);
        Filter secondFilter = as(subquery.child(), Filter.class);
        And and = as(secondFilter.condition(), And.class);
        Match secondMatch = as(and.right(), Match.class);
        ReferenceAttribute secondFieldAttribute = as(secondMatch.field(), ReferenceAttribute.class);
        assertEquals("first_name", secondFieldAttribute.name());
        assertThat(secondFilter.child(), instanceOf(ExternalRelation.class));
    }

    private static final String EXTERNAL_DATASET = "external_employees";
    private static final String EXTERNAL_DATASET_RESOURCE = "s3://bucket/external_employees.parquet";

    /**
     * Build the analyzed-then-optimized plan for a {@code FROM ..., (FROM <dataset> | ...)} query whose
     * subquery source is a registered external dataset. Mirrors the production pipeline: {@code FROM
     * <dataset>} is rewritten by {@link DatasetRewriter} into the same {@code UnresolvedExternalRelation}
     * the {@code EXTERNAL} command produces, which the analyzer resolves against the configured external
     * source schema, so the optimizer sees a {@code UnionAll} branch backed by an {@link ExternalRelation}
     * (wrapped in a {@code Subquery}) exactly like a real dataset subquery would produce.
     *
     * <p>The dataset deliberately exposes a subset of the {@code test} index schema ({@code emp_no},
     * {@code first_name}, {@code salary}); the analyzer fills the remaining {@code UnionAll} columns on the
     * dataset branch with nulls via an {@code Eval}.
     */
    private LogicalPlan planExternalDatasetSubquery(String query) {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset dataset = new Dataset(EXTERNAL_DATASET, new DataSourceReference("external_ds"), EXTERNAL_DATASET_RESOURCE, null, Map.of());
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of(EXTERNAL_DATASET, dataset))
            .build();
        // FROM <dataset> -> UnresolvedExternalRelation, the same shape the EXTERNAL command would parse to.
        LogicalPlan rewritten = DatasetRewriter.rewriteUnsecured(
            TEST_PARSER.parseQuery(query),
            projectMetadata,
            TestIndexNameExpressionResolver.newInstance()
        );
        List<Attribute> externalSchema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("first_name", DataType.KEYWORD),
            referenceAttribute("salary", DataType.INTEGER)
        );
        LogicalPlan analyzed = subqueryAnalyzer().externalSourceResolution(EXTERNAL_DATASET_RESOURCE, externalSchema, FileList.UNRESOLVED)
            .buildAnalyzer()
            .analyze(rewritten);
        return optimize(analyzed);
    }
}
