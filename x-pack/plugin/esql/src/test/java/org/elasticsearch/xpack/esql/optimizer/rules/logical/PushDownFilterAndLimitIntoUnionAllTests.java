/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
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

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PushDownFilterAndLimitIntoUnionAllTests extends AbstractLogicalPlanOptimizerTests {

    /*
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#44, emp_no{r}#45, first_name{r}#46, gender{r}#47, hire_date{r}#48, job{r}#49, job.raw{r}#50,
     *                    languages{r}#51, last_name{r}#52, long_noidx{r}#53, salary{r}#54, language_code{r}#55, language_name{r}#56]]
     *   |_EsqlProject[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *                           languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10, language_code{r}#29, language_name{r}#30]]
     *   | \_Eval[[null[INTEGER] AS language_code#29, null[KEYWORD] AS language_name#30]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#5 > 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   |_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *                           languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21, language_code{r}#31,
     *                           language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[languages{f}#19 > 0[INTEGER] AND emp_no{f}#16 > 10000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *   \_LocalRelation[[_meta_field{r}#33, emp_no{r}#34, first_name{r}#35, gender{r}#36, hire_date{r}#37, job{r}#38, job.raw{r}#39,
     *                               languages{r}#40, last_name{r}#41, long_noidx{r}#42, salary{r}#43, language_code{f}#27,
     *                               language_name{f}#28],EMPTY]
     */
    public void testPushDownSimpleFilterPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#26, emp_no{r}#27, first_name{r}#28, gender{r}#29, hire_date{r}#30, job{r}#31, job.raw{r}#32,
     *                    languages{r}#33, last_name{r}#34, long_noidx{r}#35, salary{r}#36]]
     *   |_EsqlProject[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13,
     *                           languages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9]]
     *   | \_Limit[1000[INTEGER],false]
     *   |   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     *   \_EsqlProject[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24,
     *                           languages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *     \_Subquery[]
     *       \_TopN[[Order[emp_no{f}#15,ASC,LAST]],1000[INTEGER]]
     *         \_Filter[languages{f}#18 > 0[INTEGER]]
     *           \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDownLimitPastSubqueryWithSort() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51,
     *                    languages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30,
     *                           language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 > 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32,
     *                           language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_TopN[[Order[emp_no{f}#17,ASC,LAST]],1000[INTEGER]]
     *   |       \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40,
     *                               languages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28,
     *                               language_name{f}#29],EMPTY]
     */
    public void testPushDownFilterAndLimitPastSubqueryWithSort() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51,
     *                    languages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30,
     *                           language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 > 10000[INTEGER] AND salary{f}#11 > 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32,
     *                           language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER] AND salary{f}#22 > 50000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40,
     *                               languages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28,
     *                               language_name{f}#29],EMPTY]
     */
    public void testPushDownConjunctiveFilterPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51,
     *                    languages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30,
     *                           language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 > 10000[INTEGER] OR salary{f}#11 > 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32,
     *                           language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[languages{f}#20 > 0[INTEGER] AND emp_no{f}#17 > 10000[INTEGER] OR salary{f}#22 > 50000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40,
     *                               languages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28,
     *                               language_name{f}#29],EMPTY]
     */
    public void testPushDownDisjunctiveFilterPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51,
     *                    languages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30,
     *                           language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 > 10000[INTEGER] AND salary{f}#11 < 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32,
     *                           language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[salary{f}#22 < 50000[INTEGER] AND emp_no{f}#17 > 10000[INTEGER]]
     *   |         \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40,
     *                               languages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28,
     *                               language_name{f}#29],EMPTY]
     */
    public void testPushDownFilterPastUnionAllAndCombineWithFilterInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Project[[_meta_field{r}#101, emp_no{r}#102, first_name{r}#103, gender{r}#104, hire_date{r}#105, job{r}#106, job.raw{r}#107,
     *               languages{r}#108, last_name{r}#109, long_noidx{r}#110, salary{r}#111, z{r}#114, language_name{r}#115,
     *               $$x$converted_to$long{r$}#124 AS x#37, $$y$converted_to$long{r$}#125 AS y#40]]
     * \_Limit[1000[INTEGER],false]
     *   \_UnionAll[[_meta_field{r}#101, emp_no{r}#102, first_name{r}#103, gender{r}#104, hire_date{r}#105, job{r}#106, job.raw{r}#107,
     *                      languages{r}#108, last_name{r}#109, long_noidx{r}#110, salary{r}#111, x{r}#112, $$x$converted_to$long{r$}#124,
     *                      y{r}#113, $$y$converted_to$long{r$}#125, z{r}#114, language_name{r}#115]]
     *     |_LocalRelation[[_meta_field{f}#50, emp_no{f}#44, first_name{f}#45, gender{f}#46, hire_date{f}#51, job{f}#52, job.raw{f}#53,
     *                                 languages{f}#47, last_name{f}#48, long_noidx{f}#54, salary{f}#49, x{r}#81,
     *                                 $$x$converted_to$long{r}#116, y{r}#126, $$y$converted_to$long{r}#117, z{r}#83,
     *                                 language_name{r}#84],EMPTY]
     *     |_EsqlProject[[_meta_field{f}#61, emp_no{f}#55, first_name{f}#56, gender{f}#57, hire_date{f}#62, job{f}#63, job.raw{f}#64,
     *                             languages{f}#58, last_name{f}#59, long_noidx{f}#65, salary{f}#60, x{r}#4, $$x$converted_to$long{r}#118,
     *                             y{r}#127, $$y$converted_to$long{r}#119, z{r}#10, language_name{r}#85]]
     *     | \_Filter[ISNOTNULL($$y$converted_to$long{r}#119)]
     *     |   \_Eval[[null[KEYWORD] AS language_name#85, 1[LONG] AS $$x$converted_to$long#118,
     *                      TOLONG(y{r}#7) AS $$y$converted_to$long#119, null[KEYWORD] AS y#127]]
     *     |     \_Subquery[]
     *     |       \_Project[[_meta_field{f}#61, emp_no{f}#55, first_name{f}#56, gender{f}#57, hire_date{f}#62, job{f}#63, job.raw{f}#64,
     *                              languages{f}#58, last_name{f}#59, long_noidx{f}#65, salary{f}#60, x{r}#4, emp_no{f}#55 AS y#7,
     *                              z{r}#10]]
     *     |         \_Limit[1000[INTEGER],false]
     *     |           \_Filter[z{r}#10 > 0[INTEGER]]
     *     |             \_Eval[[1[INTEGER] AS x#4, emp_no{f}#55 + 1[INTEGER] AS z#10]]
     *     |               \_Filter[salary{f}#60 < 100000[INTEGER]]
     *     |                 \_EsRelation[test][_meta_field{f}#61, emp_no{f}#55, first_name{f}#56, ..]
     *     |_EsqlProject[[_meta_field{r}#86, emp_no{r}#87, first_name{r}#88, gender{r}#89, hire_date{r}#90, job{r}#91, job.raw{r}#92,
     *                             languages{r}#93, last_name{r}#94, long_noidx{r}#95, salary{r}#96, x{r}#21,
     *                             $$x$converted_to$long{r}#120, y{r}#128, $$y$converted_to$long{r}#121, z{r}#16, language_name{r}#97]]
     *     | \_Filter[ISNOTNULL($$y$converted_to$long{r}#121)]
     *     |   \_Eval[[null[KEYWORD] AS _meta_field#86, null[INTEGER] AS emp_no#87, null[KEYWORD] AS first_name#88,
     *                      null[TEXT] AS gender#89, null[DATETIME] AS hire_date#90, null[TEXT] AS job#91, null[KEYWORD] AS job.raw#92,
     *                      null[INTEGER] AS languages#93, null[KEYWORD] AS last_name#94, null[LONG] AS long_noidx#95,
     *                      null[INTEGER] AS salary#96, null[KEYWORD] AS language_name#97, 1[LONG] AS $$x$converted_to$long#120,
     *                      TOLONG(y{r}#19) AS $$y$converted_to$long#121, null[KEYWORD] AS y#128]]
     *     |     \_Subquery[]
     *     |       \_Eval[[1[INTEGER] AS x#21]]
     *     |         \_Limit[1000[INTEGER],false]
     *     |           \_Filter[z{r}#16 > 0[INTEGER]]
     *     |             \_Aggregate[[language_code{f}#66],[COUNT(*[KEYWORD],true[BOOLEAN]) AS y#19, language_code{f}#66 AS z#16]]
     *     |               \_EsRelation[languages][language_code{f}#66, language_name{f}#67]
     *     \_EsqlProject[[_meta_field{f}#74, emp_no{r}#98, first_name{f}#69, gender{f}#70, hire_date{f}#75, job{f}#76, job.raw{f}#77,
     *                             languages{r}#99, last_name{f}#72, long_noidx{f}#78, salary{r}#100, x{r}#28,
     *                             $$x$converted_to$long{r}#122, y{r}#129, $$y$converted_to$long{r}#123, z{r}#34, language_name{f}#80]]
     *       \_Filter[ISNOTNULL($$x$converted_to$long{r}#122) AND ISNOTNULL($$y$converted_to$long{r}#123)]
     *         \_Eval[[null[INTEGER] AS emp_no#98, null[INTEGER] AS languages#99, null[INTEGER] AS salary#100,
     *                     TOLONG(x{r}#28) AS $$x$converted_to$long#122,
     *                     TOLONG(y{r}#31) AS $$y$converted_to$long#123, null[KEYWORD] AS y#129]]
     *           \_Subquery[]
     *             \_Project[[_meta_field{f}#74, emp_no{f}#68 AS x#28, first_name{f}#69, gender{f}#70, hire_date{f}#75, job{f}#76,
     *                              job.raw{f}#77, languages{f}#71 AS z#34, last_name{f}#72, long_noidx{f}#78, salary{f}#73 AS y#31,
     *                              language_name{f}#80]]
     *               \_Limit[1000[INTEGER],true]
     *                 \_Join[LEFT,[languages{f}#71],[language_code{f}#79],null]
     *                   |_Limit[1000[INTEGER],false]
     *                   | \_Filter[languages{f}#71 > 0[INTEGER]]
     *                   |   \_EsRelation[test][_meta_field{f}#74, emp_no{f}#68, first_name{f}#69, ..]
     *                   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#79, language_name{f}#80]
     */
    public void testPushDownFilterOnReferenceAttributesPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41,
     *                    languages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, x{r}#46, y{r}#47]]
     *   |_LocalRelation[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20,
     *                               languages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16, x{r}#33, y{r}#34],EMPTY]
     *   \_EsqlProject[[_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, gender{f}#24, hire_date{f}#29, job{f}#30, job.raw{f}#31,
     *                           languages{f}#25, last_name{f}#26, long_noidx{f}#32, salary{f}#27, x{r}#4, y{r}#7]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false]
     *         \_Filter[y{r}#7 > 0[INTEGER]]
     *           \_Eval[[1[INTEGER] AS x#4, emp_no{f}#22 + 1[INTEGER] AS y#7]]
     *             \_Filter[salary{f}#27 < 100000[INTEGER] AND emp_no{f}#22 > 0[INTEGER]]
     *               \_EsRelation[test][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     */
    public void testPushDownFilterOnReferenceAttributesAndFieldAttributesPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * TODO push down filters on mixed typed attributes past UnionAll
     * Project[[_meta_field{r}#66, first_name{r}#68, hire_date{r}#70, job{r}#71, job.raw{r}#72, languages{r}#73, last_name{r}#74,
     *               long_noidx{r}#75, salary{r}#76, avg_worked_seconds{r}#77, birth_date{r}#78, height{r}#79, height.double{r}#80,
     *               height.half_float{r}#81, height.scaled_float{r}#82, is_rehired{r}#83, job_positions{r}#84, languages.int{r}#85,
     *               languages.long{r}#86, languages.short{r}#87, salary_change{r}#88, still_hired{r}#89,
     *               $$emp_no$converted_to$double{r$}#96 AS x#5, $$emp_no$converted_to$long{r$}#97 AS emp_no#8,
     *               $$gender$converted_to$keyword{r$}#98 AS gender#11, languages{r}#73 AS y#14]]
     * \_Limit[1000[INTEGER],false]
     *   \_Filter[$$emp_no$converted_to$long{r$}#97 > 10000[INTEGER] AND ISNOTNULL($$gender$converted_to$keyword{r$}#98)
     *               AND languages{r}#73 < 5[INTEGER]]
     *     \_UnionAll[[_meta_field{r}#66, emp_no{r}#67, $$emp_no$converted_to$double{r$}#96, $$emp_no$converted_to$long{r$}#97,
     *                        first_name{r}#68, gender{r}#69, $$gender$converted_to$keyword{r$}#98, hire_date{r}#70, job{r}#71,
     *                        job.raw{r}#72, languages{r}#73, last_name{r}#74, long_noidx{r}#75, salary{r}#76, avg_worked_seconds{r}#77,
     *                        birth_date{r}#78, height{r}#79, height.double{r}#80, height.half_float{r}#81, height.scaled_float{r}#82,
     *                        is_rehired{r}#83, job_positions{r}#84, languages.int{r}#85, languages.long{r}#86, languages.short{r}#87,
     *                        salary_change{r}#88, still_hired{r}#89]]
     *       |_EsqlProject[[_meta_field{f}#24, emp_no{r}#99, $$emp_no$converted_to$double{r}#90, $$emp_no$converted_to$long{r}#91,
     *                               first_name{r}#100, gender{f}#20, $$gender$converted_to$keyword{r}#92, hire_date{r}#101, job{f}#26,
     *                               job.raw{f}#27, languages{f}#21, last_name{r}#102, long_noidx{f}#28, salary{r}#103,
     *                               avg_worked_seconds{r}#49, birth_date{r}#50, height{r}#51, height.double{r}#52,
     *                               height.half_float{r}#53, height.scaled_float{r}#54, is_rehired{r}#55, job_positions{r}#56,
     *                               languages.int{r}#57, languages.long{r}#58, languages.short{r}#59, salary_change{r}#60,
     *                               still_hired{r}#61]]
     *       | \_Eval[[null[UNSIGNED_LONG] AS avg_worked_seconds#49, null[DATETIME] AS birth_date#50, null[DOUBLE] AS height#51,
     *                      null[DOUBLE] AS height.double#52, null[DOUBLE] AS height.half_float#53, null[DOUBLE] AS height.scaled_float#54,
     *                      null[KEYWORD] AS is_rehired#55, null[TEXT] AS job_positions#56, null[INTEGER] AS languages.int#57,
     *                      null[LONG] AS languages.long#58, null[INTEGER] AS languages.short#59, null[DOUBLE] AS salary_change#60,
     *                      null[KEYWORD] AS still_hired#61, TODOUBLE(emp_no{f}#18) AS $$emp_no$converted_to$double#90,
     *                      TOLONG(emp_no{f}#18) AS $$emp_no$converted_to$long#91,
     *                      TOSTRING(gender{f}#20) AS $$gender$converted_to$keyword#92, null[KEYWORD] AS emp_no#99,
     *                      null[KEYWORD] AS first_name#100, TODATENANOS(hire_date{f}#25) AS hire_date#101,
     *                      null[KEYWORD] AS last_name#102, null[KEYWORD] AS salary#103]]
     *       |   \_Limit[1000[INTEGER],false]
     *       |     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *       \_Project[[_meta_field{r}#62, $$emp_no$temp_name$108{r}#109 AS emp_no#104, $$emp_no$converted_to$double{r}#93,
     *                        emp_no{f}#29 AS $$emp_no$converted_to$long#94, first_name{r}#105, gender{f}#32,
     *                        $$gender$converted_to$keyword{r}#95, hire_date{f}#34, job{r}#63, job.raw{r}#64, languages{f}#36,
     *                        last_name{r}#106, long_noidx{r}#65, salary{r}#107, avg_worked_seconds{f}#45, birth_date{f}#33, height{f}#40,
     *                        height.double{f}#41, height.half_float{f}#43, height.scaled_float{f}#42, is_rehired{f}#47,
     *                        job_positions{f}#46, languages.int{f}#39, languages.long{f}#37, languages.short{f}#38, salary_change{f}#48,
     *                        still_hired{f}#44]]
     *         \_Eval[[null[KEYWORD] AS _meta_field#62, null[TEXT] AS job#63, null[KEYWORD] AS job.raw#64, null[LONG] AS long_noidx#65,
     *                     TODOUBLE(emp_no{f}#29) AS $$emp_no$converted_to$double#93,
     *                     TOSTRING(gender{f}#32) AS $$gender$converted_to$keyword#95, null[KEYWORD] AS $$emp_no$temp_name$108#109,
     *                     null[KEYWORD] AS first_name#105, null[KEYWORD] AS last_name#106, null[KEYWORD] AS salary#107]]
     *           \_Subquery[]
     *             \_Limit[1000[INTEGER],false]
     *               \_Filter[languages{f}#36 > 1[INTEGER]]
     *                 \_EsRelation[test_mixed_types][avg_worked_seconds{f}#45, birth_date{f}#33, emp_no{..]
     */
    public void testFilterOnMixedDataTypesFields() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#26, emp_no{r}#27, first_name{r}#28, gender{r}#29, hire_date{r}#30, job{r}#31, job.raw{r}#32,
     *                    languages{r}#33, last_name{r}#34, long_noidx{r}#35, salary{r}#36]]
     *   |_EsqlProject[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13,
     *                           languages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9]]
     *   | \_Limit[1000[INTEGER],false]
     *   |   \_Filter[:(first_name{f}#5,first[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     *   \_EsqlProject[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24,
     *                           languages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false]
     *         \_Filter[languages{f}#18 > 0[INTEGER] AND :(first_name{f}#16,first[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDownSingleFullTextFunctionPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#27, emp_no{r}#28, first_name{r}#29, gender{r}#30, hire_date{r}#31, job{r}#32, job.raw{r}#33,
     *                    languages{r}#34, last_name{r}#35, long_noidx{r}#36, salary{r}#37]]
     *   |_EsqlProject[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14,
     *                           languages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10]]
     *   | \_Limit[1000[INTEGER],false]
     *   |   \_Filter[:(first_name{f}#6,first[KEYWORD]) AND MATCH(last_name{f}#9,last[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   \_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *                           languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false]
     *         \_Filter[languages{f}#19 > 0[INTEGER] AND :(first_name{f}#17,first[KEYWORD]) AND MATCH(last_name{f}#20,last[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     */
    public void testPushDownConjunctiveFullTextFunctionPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0)
            | WHERE first_name:"first" and match(last_name, "last")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
        And and = as(childFilter.condition(), And.class);
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
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#28, emp_no{r}#29, first_name{r}#30, gender{r}#31, hire_date{r}#32, job{r}#33, job.raw{r}#34,
     *                    languages{r}#35, last_name{r}#36, long_noidx{r}#37, salary{r}#38]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15,
     *                           languages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11]]
     *   | \_Limit[1000[INTEGER],false]
     *   |   \_Filter[:(first_name{f}#7,first[KEYWORD]) OR MatchPhrase(last_name{f}#10,last[KEYWORD])]
     *   |     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   \_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *                           languages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false]
     *         \_Filter[languages{f}#20 > 0[INTEGER] AND MATCH(gender{f}#19,F[KEYWORD]) AND :(first_name{f}#18,first[KEYWORD])
     *                      OR MatchPhrase(last_name{f}#21,last[KEYWORD])]
     *           \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     */
    public void testPushDownDisjunctiveFullTextFunctionPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test | WHERE languages > 0 and match(gender , "F"))
            | WHERE first_name:"first" or match_phrase(last_name, "last")
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        EsqlProject child1 = as(unionAll.children().get(0), EsqlProject.class);
        Limit childLimit = as(child1.child(), Limit.class);
        Filter childFilter = as(childLimit.child(), Filter.class);
        Or or = as(childFilter.condition(), Or.class);
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

    public void testFullTextFunctionCannotBePushedDownPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM test, (FROM languages)
            | WHERE match(language_name, "text")
            """));
        assertTrue(e.getMessage().startsWith("Found "));
        final String header = "Found 2 problems\nline ";
        // TODO improve the error message to indicate the field is missing from an index pattern under a subquery context
        // FullTextFunction.checkCommandsBeforeExpression adds the first failure
        // FullTextFunction.fieldVerifier adds the second failure
        // resolveFork creates the reference attribute, provide a better source text for it
        assertEquals(
            "1:1: [MATCH] function cannot be used after FROM\n"
                + "line 1:1: [MATCH] function cannot operate on [FROM test, (FROM languages)], which is not a field from an index mapping",
            e.getMessage().substring(header.length())
        );
    }
}
