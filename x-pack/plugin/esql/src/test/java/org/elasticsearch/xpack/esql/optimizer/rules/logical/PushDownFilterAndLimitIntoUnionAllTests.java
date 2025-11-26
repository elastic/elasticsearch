/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PushDownFilterAndLimitIntoUnionAllTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkSubqueryInFromCommandSupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51,
     *                    languages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30,
     *                           language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false,false]
     *   |     \_Filter[emp_no{f}#6 > 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32,
     *                           language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false,false]
     *   |       \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40,
     *                               languages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28,
     *                               language_name{f}#29],EMPTY]
     */
    public void testPushDownSimpleFilterPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
            | WHERE emp_no > 10000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Eval eval = as(child1.child(), Eval.class);
        Limit childLimit = as(eval.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#27, emp_no{r}#28, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *                    languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *   |_EsqlProject[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *                           languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10]]
     *   | \_Limit[1000[INTEGER],false,false]
     *   |   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *                            languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21]]
     *     \_Subquery[]
     *       \_TopN[[Order[emp_no{f}#16,ASC,LAST]],1000[INTEGER],false]
     *         \_Filter[languages{f}#19 > 0[INTEGER]]
     *           \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testPushDownLimitPastSubqueryWithSort() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 | SORT emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        EsRelation relation = as(childLimit.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
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
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   | \_Limit[1000[INTEGER],false,false]
     *   |   \_Filter[emp_no{f}#6 > 10000[INTEGER]]
     *   |     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   \_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *     \_Subquery[]
     *       \_TopN[[Order[emp_no{f}#17,ASC,LAST]],1000[INTEGER],false]
     *         \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER]]
     *           \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPushDownFilterAndLimitPastSubqueryWithSort() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 | SORT emp_no)
            | WHERE emp_no > 10000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
        GreaterThan greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute empNo = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        TopN topN = as(subquery.child(), TopN.class);
        childFilter = as(topN.child(), Filter.class);
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
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *                    languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, language_code{r}#57, language_name{r}#58]]
     *   |_EsqlProject[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Limit[1000[INTEGER],false,false]
     *   |     \_Filter[emp_no{f}#7 > 10000[INTEGER] AND salary{f}#12 > 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, language_code{r}#33,
     *                           language_name{r}#34]]
     *   | \_Eval[[null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false,false]
     *   |       \_Filter[languages{f}#21 > 0[INTEGER] AND emp_no{f}#18 > 10000[INTEGER] AND salary{f}#23 > 50000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *   \_LocalRelation[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41,
     *                               languages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, language_code{f}#29,
     *                               language_name{f}#30], EMPTY]
     */
    public void testPushDownConjunctiveFilterPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
            | WHERE emp_no > 10000 and salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Eval eval = as(child1.child(), Eval.class);
        Limit childLimit = as(eval.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *                    languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, language_code{r}#57, language_name{r}#58]]
     *   |_EsqlProject[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Limit[1000[INTEGER],false,false]
     *   |     \_Filter[emp_no{f}#7 > 10000[INTEGER] OR salary{f}#12 > 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, language_code{r}#33,
     *                           language_name{r}#34]]
     *   | \_Eval[[null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false,false]
     *   |       \_Filter[languages{f}#21 > 0[INTEGER] AND emp_no{f}#18 > 10000[INTEGER] OR salary{f}#23 > 50000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *   \_LocalRelation[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41,
     *                              languages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, language_code{f}#29,
     *                              language_name{f}#30],EMPTY]
     */
    public void testPushDownDisjunctiveFilterPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
            | WHERE emp_no > 10000 or salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Eval eval = as(child1.child(), Eval.class);
        Limit childLimit = as(eval.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /*
     *Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#46, emp_no{r}#47, first_name{r}#48, gender{r}#49, hire_date{r}#50, job{r}#51, job.raw{r}#52,
     *                    languages{r}#53, last_name{r}#54, long_noidx{r}#55, salary{r}#56, language_code{r}#57, language_name{r}#58]]
     *   |_EsqlProject[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Limit[1000[INTEGER],false,false]
     *   |     \_Filter[emp_no{f}#7 > 10000[INTEGER] AND salary{f}#12 < 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, language_code{r}#33,
     *                           language_name{r}#34]]
     *   | \_Eval[[null[INTEGER] AS language_code#33, null[KEYWORD] AS language_name#34]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false,false]
     *   |       \_Filter[salary{f}#23 < 50000[INTEGER] AND emp_no{f}#18 > 10000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *   \_LocalRelation[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41,
     *                               languages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, language_code{f}#29,
     *                               language_name{f}#30],EMPTY]
     */
    public void testPushDownFilterPastUnionAllAndCombineWithFilterInSubquery() {
        var plan = planSubquery("""
            FROM test, (FROM test | where salary < 100000), (FROM languages  | WHERE language_code > 0)
            | WHERE emp_no > 10000 and salary < 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Eval eval = as(child1.child(), Eval.class);
        Limit childLimit = as(eval.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /*
     *Project[[_meta_field{r}#102, emp_no{r}#103, first_name{r}#104, gender{r}#105, hire_date{r}#106, job{r}#107, job.raw{r}#108,
     *               languages{r}#109, last_name{r}#110, long_noidx{r}#111, salary{r}#112, z{r}#115, language_name{r}#116,
     *               $$x$converted_to$long{r$}#125 AS x#38, $$y$converted_to$long{r$}#126 AS y#41]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_UnionAll[[_meta_field{r}#102, emp_no{r}#103, first_name{r}#104, gender{r}#105, hire_date{r}#106, job{r}#107, job.raw{r}#108,
     *                      languages{r}#109, last_name{r}#110, long_noidx{r}#111, salary{r}#112, x{r}#113, $$x$converted_to$long{r$}#125,
     *                      y{r}#114, $$y$converted_to$long{r$}#126, z{r}#115, language_name{r}#116]]
     *     |_LocalRelation[[_meta_field{f}#51, emp_no{f}#45, first_name{f}#46, gender{f}#47, hire_date{f}#52, job{f}#53, job.raw{f}#54,
     *                                 languages{f}#48, last_name{f}#49, long_noidx{f}#55, salary{f}#50, x{r}#82,
     *                                 $$x$converted_to$long{r}#117, y{r}#127, $$y$converted_to$long{r}#118, z{r}#84,
     *                                 language_name{r}#85],EMPTY]
     *     |_EsqlProject[[_meta_field{f}#62, emp_no{f}#56, first_name{f}#57, gender{f}#58, hire_date{f}#63, job{f}#64, job.raw{f}#65,
     *                             languages{f}#59, last_name{f}#60, long_noidx{f}#66, salary{f}#61, x{r}#5, $$x$converted_to$long{r}#119,
     *                             y{r}#128, $$y$converted_to$long{r}#120, z{r}#11, language_name{r}#86]]
     *     | \_Filter[ISNOTNULL($$y$converted_to$long{r}#120)]
     *     |   \_Eval[[null[KEYWORD] AS language_name#86, 1[LONG] AS $$x$converted_to$long#119,
     *                     TOLONG(y{r}#8) AS $$y$converted_to$long#120, null[KEYWORD] AS y#128]]
     *     |     \_Subquery[]
     *     |       \_Project[[_meta_field{f}#62, emp_no{f}#56, first_name{f}#57, gender{f}#58, hire_date{f}#63, job{f}#64, job.raw{f}#65,
     *                              languages{f}#59, last_name{f}#60, long_noidx{f}#66, salary{f}#61, x{r}#5, emp_no{f}#56 AS y#8, z{r}#11]]
     *     |         \_Limit[1000[INTEGER],false,false]
     *     |           \_Filter[z{r}#11 > 0[INTEGER]]
     *     |             \_Eval[[1[INTEGER] AS x#5, emp_no{f}#56 + 1[INTEGER] AS z#11]]
     *     |               \_Filter[salary{f}#61 < 100000[INTEGER]]
     *     |                 \_EsRelation[test][_meta_field{f}#62, emp_no{f}#56, first_name{f}#57, ..]
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
     *     |         \_Limit[1000[INTEGER],false,false]
     *     |           \_Filter[z{r}#17 > 0[INTEGER]]
     *     |             \_Aggregate[[language_code{f}#67],[COUNT(*[KEYWORD],true[BOOLEAN]) AS y#20, language_code{f}#67 AS z#17]]
     *     |               \_EsRelation[languages][language_code{f}#67, language_name{f}#68]
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
     *               \_Limit[1000[INTEGER],true,false]
     *                 \_Join[LEFT,[languages{f}#72],[language_code{f}#80],null]
     *                   |_Limit[1000[INTEGER],false,false]
     *                   | \_Filter[languages{f}#72 > 0[INTEGER]]
     *                   |   \_EsRelation[test][_meta_field{f}#75, emp_no{f}#69, first_name{f}#70, ..]
     *                   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#80, language_name{f}#81]
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
        assertEquals(4, unionAll.children().size());

        LocalRelation child1 = as(unionAll.children().get(0), LocalRelation.class);

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
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
        Limit childLimit = as(project.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child3 = as(unionAll.children().get(2), EsqlProject.class);
        filter = as(child3.child(), Filter.class);
        isNotNull = as(filter.condition(), IsNotNull.class);
        y = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$y$converted_to$long", y.name());
        eval = as(filter.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        eval = as(subquery.child(), Eval.class);
        limit = as(eval.child(), Limit.class);
        filter = as(limit.child(), Filter.class);
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

        EsqlProject child4 = as(unionAll.children().get(3), EsqlProject.class);
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
        limit = as(project.child(), Limit.class);
        Join lookupJoin = as(limit.child(), Join.class);
        limit = as(lookupJoin.left(), Limit.class);
        filter = as(limit.child(), Filter.class);
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
     *   |_LocalRelation[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21,
     *                               languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17, x{r}#34, y{r}#35],EMPTY]
     *   \_EsqlProject[[_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, gender{f}#25, hire_date{f}#30, job{f}#31, job.raw{f}#32,
     *                           languages{f}#26, last_name{f}#27, long_noidx{f}#33, salary{f}#28, x{r}#5, y{r}#8]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Filter[y{r}#8 > 0[INTEGER]]
     *           \_Eval[[1[INTEGER] AS x#5, emp_no{f}#23 + 1[INTEGER] AS y#8]]
     *             \_Filter[salary{f}#28 < 100000[INTEGER] AND emp_no{f}#23 > 0[INTEGER]]
     *               \_EsRelation[test][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     */
    public void testPushDownFilterOnReferenceAttributesAndFieldAttributesPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | where salary < 100000 | EVAL x = 1, y = emp_no + 1)
            | WHERE x is not null and y > 0 and emp_no > 0
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        LocalRelation child1 = as(unionAll.children().get(0), LocalRelation.class);

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        Limit childLimit = as(subquery.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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
     *       |_EsqlProject[[_meta_field{f}#25, emp_no{r}#100, $$emp_no$converted_to$double{r}#91, $$emp_no$converted_to$long{r}#92,
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
     *       |   \_Limit[1000[INTEGER],false,false]
     *       |     \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
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
     *             \_Limit[1000[INTEGER],false,false]
     *               \_Filter[languages{f}#37 > 1[INTEGER]]
     *                 \_EsRelation[test_mixed_types][avg_worked_seconds{f}#46, birth_date{f}#34, emp_no{..]
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
        Filter filter = as(limit.child(), Filter.class);
        And and = as(filter.condition(), And.class);
        LessThan lessThan = as(and.right(), LessThan.class);
        ReferenceAttribute salary = as(lessThan.left(), ReferenceAttribute.class);
        assertEquals("languages", salary.name());
        Literal right = as(lessThan.right(), Literal.class);
        assertEquals(5, right.value());
        and = as(and.left(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        ReferenceAttribute emp_no = as(greaterThan.left(), ReferenceAttribute.class);
        assertEquals("$$emp_no$converted_to$long", emp_no.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(10000, right.value());
        IsNotNull isNotNull = as(and.right(), IsNotNull.class);
        ReferenceAttribute gender = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("$$gender$converted_to$keyword", gender.name());
        UnionAll unionAll = as(filter.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Eval eval = as(child1.child(), Eval.class);
        limit = as(eval.child(), Limit.class);
        EsRelation relation = as(limit.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        Project child2 = as(unionAll.children().get(1), Project.class);
        eval = as(child2.child(), Eval.class);
        Subquery subquery = as(eval.child(), Subquery.class);
        limit = as(subquery.child(), Limit.class);
        Filter childFilter = as(limit.child(), Filter.class);
        greaterThan = as(childFilter.condition(), GreaterThan.class);
        FieldAttribute salaryField = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", salaryField.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(1, right.value());
        relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test_mixed_types", relation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_UnionAll[[_meta_field{r}#27, emp_no{r}#28, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *                    languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *   |_EsqlProject[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *                           languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10]]
     *   | \_Limit[1000[INTEGER],false,false]
     *   |   \_Filter[:(first_name{f}#6,first[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *                           languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Filter[languages{f}#19 > 0[INTEGER] AND :(first_name{f}#17,first[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testPushDownSingleFullTextFunctionPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0)
            | WHERE first_name:"first"
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
        MatchOperator match = as(childFilter.condition(), MatchOperator.class);
        FieldAttribute first_name = as(match.field(), FieldAttribute.class);
        assertEquals("first_name", first_name.name());
        Literal right = as(match.query(), Literal.class);
        assertEquals(new BytesRef("first"), right.value());
        EsRelation relation = as(childFilter.child(), EsRelation.class);
        assertEquals("test", relation.indexPattern());

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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
     *   |_EsqlProject[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13,
     *                           languages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9]]
     *   | \_Limit[1000[INTEGER],false,false]
     *   |   \_Filter[QSTR(first_name:first[KEYWORD]) AND KQL(last_name:last[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     *   \_EsqlProject[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24,
     *                           languages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Filter[languages{f}#18 > 0[INTEGER] AND QSTR(gender:female[KEYWORD]) AND
     *                      QSTR(first_name:first[KEYWORD]) AND KQL(last_name:last[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDownFullTextFunctionNoFieldRequiredPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 AND qstr("gender:female"))
            | WHERE qstr("first_name:first") == true AND kql("last_name:last") == false
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   | \_Limit[1000[INTEGER],false,false]
     *   |   \_Filter[:(first_name{f}#7,first[KEYWORD]) AND MATCH(last_name{f}#10,last[KEYWORD]) AND QSTR(gender:female[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   \_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Filter[languages{f}#20 > 0[INTEGER] AND :(first_name{f}#18,first[KEYWORD]) AND
     *                      MATCH(last_name{f}#21,last[KEYWORD]) AND QSTR(gender:female[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPushDownConjunctiveFullTextFunctionPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0)
            | WHERE first_name:"first" and match(last_name, "last") and qstr("gender:female")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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
     *   |_EsqlProject[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16,
     *                           languages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12]]
     *   | \_Limit[1000[INTEGER],false,false]
     *   |   \_Filter[:(first_name{f}#8,first[KEYWORD]) OR MatchPhrase(last_name{f}#11,last[KEYWORD]) OR KQL(gender:female[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   \_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                           languages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Filter[languages{f}#21 > 0[INTEGER] AND MATCH(gender{f}#20,F[KEYWORD]) AND :(first_name{f}#19,first[KEYWORD]) OR
     *                     MatchPhrase(last_name{f}#22,last[KEYWORD]) OR KQL(gender:female[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testPushDownDisjunctiveFullTextFunctionPastUnionAll() {
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 and match(gender , "F"))
            | WHERE first_name:"first" or match_phrase(last_name, "last") or kql("gender:female")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
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

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Subquery subquery = as(child2.child(), Subquery.class);
        childLimit = as(subquery.child(), Limit.class);
        childFilter = as(childLimit.child(), Filter.class);
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
        assertEquals(2, unionAll.children().size());

        // First child: LocalRelation with EMPTY data since filter on language_name can't be applied to test index
        LocalRelation child1 = as(unionAll.children().get(0), LocalRelation.class);

        // Second child: languages subquery with MATCH filter pushed down
        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Eval eval2 = as(child2.child(), Eval.class);
        List<Alias> aliases = eval2.fields();
        assertEquals(11, aliases.size());

        Subquery subquery = as(eval2.child(), Subquery.class);
        Limit childLimit = as(subquery.child(), Limit.class);
        Filter filter = as(childLimit.child(), Filter.class);
        Match match = as(filter.condition(), Match.class);
        FieldAttribute languageName = as(match.field(), FieldAttribute.class);
        assertEquals("language_name", languageName.name());
        Literal queryLiteral = as(match.query(), Literal.class);
        assertEquals(new BytesRef("text"), queryLiteral.value());

        EsRelation relation = as(filter.child(), EsRelation.class);
        assertEquals("languages", relation.indexPattern());
    }
}
