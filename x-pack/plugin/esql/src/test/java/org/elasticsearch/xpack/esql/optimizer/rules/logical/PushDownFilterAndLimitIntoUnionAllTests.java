/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
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

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#44, emp_no{r}#45, first_name{r}#46, gender{r}#47, hire_date{r}#48, job{r}#49, job.raw{r}#50, l
     * anguages{r}#51, last_name{r}#52, long_noidx{r}#53, salary{r}#54, language_code{r}#55, language_name{r}#56]]
     *   |_EsqlProject[[_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, gender{f}#7, hire_date{f}#12, job{f}#13, job.raw{f}#14, lang
     * uages{f}#8, last_name{f}#9, long_noidx{f}#15, salary{f}#10, language_code{r}#29, language_name{r}#30]]
     *   | \_Eval[[null[INTEGER] AS language_code#29, null[KEYWORD] AS language_name#30]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#5 &gt; 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     *   |_EsqlProject[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25, l
     * anguages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21, language_code{r}#31, language_name{r}#32]]
     *   | \_Eval[[null[INTEGER] AS language_code#31, null[KEYWORD] AS language_name#32]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[languages{f}#19 &gt; 0[INTEGER] AND emp_no{f}#16 &gt; 10000[INTEGER]]
     *   |         \_EsRelation[test1][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *   \_LocalRelation[[_meta_field{r}#33, emp_no{r}#34, first_name{r}#35, gender{r}#36, hire_date{r}#37, job{r}#38, job.raw{r}#39, l
     * anguages{r}#40, last_name{r}#41, long_noidx{r}#42, salary{r}#43, language_code{f}#27, language_name{f}#28],EMPT
     * Y]
     */
    public void testPushDownSimpleFilterPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
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
        assertEquals("test1", relation.indexPattern());

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#26, emp_no{r}#27, first_name{r}#28, gender{r}#29, hire_date{r}#30, job{r}#31, job.raw{r}#32, l
     * anguages{r}#33, last_name{r}#34, long_noidx{r}#35, salary{r}#36]]
     *   |_EsqlProject[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13, lang
     * uages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9]]
     *   | \_Limit[1000[INTEGER],false]
     *   |   \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     *   \_EsqlProject[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24, l
     * anguages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     *     \_Subquery[]
     *       \_TopN[[Order[emp_no{f}#15,ASC,LAST]],1000[INTEGER]]
     *         \_Filter[languages{f}#18 &gt; 0[INTEGER]]
     *           \_EsRelation[test1][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     */
    public void testPushDownLimitPastSubqueryWithSort() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | WHERE languages > 0 | SORT emp_no)
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
        assertEquals("test1", relation.indexPattern());
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51, l
     * anguages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30, language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 &gt; 10000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26, l
     * anguages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32, language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_TopN[[Order[emp_no{f}#17,ASC,LAST]],1000[INTEGER]]
     *   |       \_Filter[languages{f}#20 &gt; 0[INTEGER] AND emp_no{f}#17 &gt; 10000[INTEGER]]
     *   |         \_EsRelation[test1][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40, l
     * anguages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28, language_name{f}#29],EMPT
     * Y]
     */
    public void testPushDownFilterAndLimitPastSubqueryWithSort() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | WHERE languages > 0 | SORT emp_no)
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
        assertEquals("test1", relation.indexPattern());
    }

    /**
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51, l
     * anguages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30, language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 &gt; 10000[INTEGER] AND salary{f}#11 &gt; 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26, l
     * anguages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32, language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[languages{f}#20 &gt; 0[INTEGER] AND emp_no{f}#17 &gt; 10000[INTEGER] AND salary{f}#22 &gt; 50000[INTEGER]]
     *   |         \_EsRelation[test1][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40, l
     * anguages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28, language_name{f}#29],EMPT
     * Y]
     */
    public void testPushDownConjunctiveFilterPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
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
        assertEquals("test1", relation.indexPattern());

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51, l
     * anguages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30, language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 &gt; 10000[INTEGER] OR salary{f}#11 &gt; 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26, l
     * anguages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32, language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[languages{f}#20 &gt; 0[INTEGER] AND emp_no{f}#17 &gt; 10000[INTEGER] OR salary{f}#22 &gt; 50000[INTEGER]]
     *   |         \_EsRelation[test1][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40, l
     * anguages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28, language_name{f}#29],EMPT
     * Y]
     */
    public void testPushDownDisjunctiveFilterPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | WHERE languages > 0), (FROM languages | WHERE language_code > 0)
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
        assertEquals("test1", relation.indexPattern());

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#45, emp_no{r}#46, first_name{r}#47, gender{r}#48, hire_date{r}#49, job{r}#50, job.raw{r}#51, l
     * anguages{r}#52, last_name{r}#53, long_noidx{r}#54, salary{r}#55, language_code{r}#56, language_name{r}#57]]
     *   |_EsqlProject[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, language_code{r}#30, language_name{r}#31]]
     *   | \_Eval[[null[INTEGER] AS language_code#30, null[KEYWORD] AS language_name#31]]
     *   |   \_Limit[1000[INTEGER],false]
     *   |     \_Filter[emp_no{f}#6 &gt; 10000[INTEGER] AND salary{f}#11 &lt; 50000[INTEGER]]
     *   |       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26, l
     * anguages{f}#20, last_name{f}#21, long_noidx{f}#27, salary{f}#22, language_code{r}#32, language_name{r}#33]]
     *   | \_Eval[[null[INTEGER] AS language_code#32, null[KEYWORD] AS language_name#33]]
     *   |   \_Subquery[]
     *   |     \_Limit[1000[INTEGER],false]
     *   |       \_Filter[salary{f}#22 &lt; 50000[INTEGER] AND emp_no{f}#17 &gt; 10000[INTEGER]]
     *   |         \_EsRelation[test1][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_LocalRelation[[_meta_field{r}#34, emp_no{r}#35, first_name{r}#36, gender{r}#37, hire_date{r}#38, job{r}#39, job.raw{r}#40, l
     * anguages{r}#41, last_name{r}#42, long_noidx{r}#43, salary{r}#44, language_code{f}#28, language_name{f}#29],EMPT
     * Y]
     */
    public void testPushDownFilterPastUnionAllAndCombineWithFilterInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | where salary < 100000), (FROM languages  | WHERE language_code > 0)
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
        assertEquals("test1", relation.indexPattern());

        LocalRelation localRelation = as(unionAll.children().get(2), LocalRelation.class);
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#95, emp_no{r}#96, first_name{r}#97, gender{r}#98, hire_date{r}#99, job{r}#100, job.raw{r}#101,
     *  languages{r}#102, last_name{r}#103, long_noidx{r}#104, salary{r}#105, x{r}#106, y{r}#107, z{r}#108, language_name{r}#109]]
     *   |_LocalRelation[[_meta_field{f}#44, emp_no{f}#38, first_name{f}#39, gender{f}#40, hire_date{f}#45, job{f}#46, job.raw{f}#47, l
     * anguages{f}#41, last_name{f}#42, long_noidx{f}#48, salary{f}#43, x{r}#75, y{r}#110, z{r}#77, language_name{r}#78],
     * EMPTY]
     *   |_EsqlProject[[_meta_field{f}#55, emp_no{f}#49, first_name{f}#50, gender{f}#51, hire_date{f}#56, job{f}#57, job.raw{f}#58, l
     * anguages{f}#52, last_name{f}#53, long_noidx{f}#59, salary{f}#54, x{r}#4, y{r}#111, z{r}#10, language_name{r}#79]]
     *   | \_Filter[ISNOTNULL(y{r}#111)]
     *   |   \_Eval[[null[KEYWORD] AS language_name#79, TOLONG(y{r}#7) AS y#111]]
     *   |     \_Subquery[]
     *   |       \_Project[[_meta_field{f}#55, emp_no{f}#49, first_name{f}#50, gender{f}#51, hire_date{f}#56, job{f}#57, job.raw{f}#58, l
     * anguages{f}#52, last_name{f}#53, long_noidx{f}#59, salary{f}#54, x{r}#4, emp_no{f}#49 AS y#7, z{r}#10]]
     *   |         \_Limit[1000[INTEGER],false]
     *   |           \_Filter[z{r}#10 &gt; 0[INTEGER]]
     *   |             \_Eval[[1[INTEGER] AS x#4, emp_no{f}#49 + 1[INTEGER] AS z#10]]
     *   |               \_Filter[salary{f}#54 &lt; 100000[INTEGER]]
     *   |                 \_EsRelation[test1][_meta_field{f}#55, emp_no{f}#49, first_name{f}#50, ..]
     *   |_EsqlProject[[_meta_field{r}#80, emp_no{r}#81, first_name{r}#82, gender{r}#83, hire_date{r}#84, job{r}#85, job.raw{r}#86, l
     * anguages{r}#87, last_name{r}#88, long_noidx{r}#89, salary{r}#90, x{r}#21, y{r}#19, z{r}#16, language_name{r}#91]]
     *   | \_Eval[[null[KEYWORD] AS _meta_field#80, null[INTEGER] AS emp_no#81, null[KEYWORD] AS first_name#82, null[TEXT] AS ge
     * nder#83, null[DATETIME] AS hire_date#84, null[TEXT] AS job#85, null[KEYWORD] AS job.raw#86, null[INTEGER] AS languages#87,
     * null[KEYWORD] AS last_name#88, null[LONG] AS long_noidx#89, null[INTEGER] AS salary#90, null[KEYWORD] AS language_name#91]]
     *   |   \_Subquery[]
     *   |     \_Eval[[1[INTEGER] AS x#21]]
     *   |       \_Limit[1000[INTEGER],false]
     *   |         \_Filter[ISNOTNULL(y{r}#19) AND z{r}#16 &gt; 0[INTEGER]]
     *   |           \_Aggregate[[language_code{f}#60],[COUNT(*[KEYWORD],true[BOOLEAN]) AS y#19, language_code{f}#60 AS z#16]]
     *   |             \_EsRelation[languages][language_code{f}#60, language_name{f}#61]
     *   \_EsqlProject[[_meta_field{f}#68, emp_no{r}#92, first_name{f}#63, gender{f}#64, hire_date{f}#69, job{f}#70, job.raw{f}#71, l
     * anguages{r}#93, last_name{f}#66, long_noidx{f}#72, salary{r}#94, x{r}#28, y{r}#112, z{r}#34, language_name{f}#74]]
     *     \_Filter[ISNOTNULL(y{r}#112)]
     *       \_Eval[[null[INTEGER] AS emp_no#92, null[INTEGER] AS languages#93, null[INTEGER] AS salary#94, TOLONG(y{r}#31) AS y#1
     * 12]]
     *         \_Subquery[]
     *           \_Project[[_meta_field{f}#68, emp_no{f}#62 AS x#28, first_name{f}#63, gender{f}#64, hire_date{f}#69, job{f}#70, job.raw{
     * f}#71, languages{f}#65 AS z#34, last_name{f}#66, long_noidx{f}#72, salary{f}#67 AS y#31, language_name{f}#74]]
     *             \_Limit[1000[INTEGER],true]
     *               \_Join[LEFT,[languages{f}#65],[language_code{f}#73],null]
     *                 |_Limit[1000[INTEGER],false]
     *                 | \_Filter[ISNOTNULL(emp_no{f}#62) AND languages{f}#65 &gt; 0[INTEGER]]
     *                 |   \_EsRelation[test1][_meta_field{f}#68, emp_no{f}#62, first_name{f}#63, ..]
     *                 \_EsRelation[languages_lookup][LOOKUP][language_code{f}#73, language_name{f}#74]
     */
    public void testPushDownFilterOnReferenceAttributesPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test
                       , (FROM test1
                          | where salary < 100000
                          | EVAL x = 1, y = emp_no, z = emp_no + 1)
                       , (FROM languages
                          | STATS cnt = COUNT(*) by language_code
                          | RENAME language_code AS z, cnt AS y
                          | EVAL x = 1)
                       , (FROM test1
                          | RENAME languages AS language_code
                          | LOOKUP JOIN languages_lookup ON language_code
                          | RENAME emp_no AS x, salary AS y, language_code AS z)
            | WHERE x is not null and y is not null and z > 0
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        LocalRelation child1 = as(unionAll.children().get(0), LocalRelation.class);

        EsqlProject child2 = as(unionAll.children().get(1), EsqlProject.class);
        Filter filter = as(child2.child(), Filter.class);
        IsNotNull isNotNull = as(filter.condition(), IsNotNull.class);
        ReferenceAttribute y = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("y", y.name());
        Eval eval = as(filter.child(), Eval.class);
        List<Alias> aliases = eval.fields();
        assertEquals(2, aliases.size());
        assertEquals("language_name", aliases.get(0).name());
        assertEquals("y", aliases.get(1).name());
        Subquery subquery = as(eval.child(), Subquery.class);
        Project project = as(subquery.child(), Project.class);
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
        assertEquals("test1", relation.indexPattern());

        EsqlProject child3 = as(unionAll.children().get(2), EsqlProject.class);
        eval = as(child3.child(), Eval.class);
        subquery = as(eval.child(), Subquery.class);
        eval = as(subquery.child(), Eval.class);
        limit = as(eval.child(), Limit.class);
        filter = as(limit.child(), Filter.class);
        And and = as(filter.condition(), And.class);
        isNotNull = as(and.left(), IsNotNull.class);
        y = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("y", y.name());
        greaterThan = as(and.right(), GreaterThan.class);
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
        isNotNull = as(filter.condition(), IsNotNull.class);
        ReferenceAttribute x = as(isNotNull.field(), ReferenceAttribute.class);
        assertEquals("y", x.name());
        eval = as(filter.child(), Eval.class);
        aliases = eval.fields();
        assertEquals(4, aliases.size());
        subquery = as(eval.child(), Subquery.class);
        project = as(subquery.child(), Project.class);
        limit = as(project.child(), Limit.class);
        Join lookupJoin = as(limit.child(), Join.class);
        limit = as(lookupJoin.left(), Limit.class);
        Filter leftFilter = as(limit.child(), Filter.class);
        and = as(leftFilter.condition(), And.class);
        isNotNull = as(and.left(), IsNotNull.class);
        FieldAttribute emp_no = as(isNotNull.field(), FieldAttribute.class);
        assertEquals("emp_no", emp_no.name());
        greaterThan = as(and.right(), GreaterThan.class);
        language_code = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("languages", language_code.name());
        right = as(greaterThan.right(), Literal.class);
        assertEquals(0, right.value());
        relation = as(leftFilter.child(), EsRelation.class);
        assertEquals("test1", relation.indexPattern());
        relation = as(lookupJoin.right(), EsRelation.class);
        assertEquals("languages_lookup", relation.indexPattern());
    }

    /**
     * Limit[1000[INTEGER],false]
     * \_UnionAll[[_meta_field{r}#35, emp_no{r}#36, first_name{r}#37, gender{r}#38, hire_date{r}#39, job{r}#40, job.raw{r}#41, l
     * anguages{r}#42, last_name{r}#43, long_noidx{r}#44, salary{r}#45, x{r}#46, y{r}#47]]
     *   |_LocalRelation[[_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, gender{f}#13, hire_date{f}#18, job{f}#19, job.raw{f}#20, l
     * anguages{f}#14, last_name{f}#15, long_noidx{f}#21, salary{f}#16, x{r}#33, y{r}#34],EMPTY]
     *   \_EsqlProject[[_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, gender{f}#24, hire_date{f}#29, job{f}#30, job.raw{f}#31, l
     * anguages{f}#25, last_name{f}#26, long_noidx{f}#32, salary{f}#27, x{r}#4, y{r}#7]]
     *     \_Subquery[]
     *       \_Limit[1000[INTEGER],false]
     *         \_Filter[y{r}#7 &gt; 0[INTEGER]]
     *           \_Eval[[1[INTEGER] AS x#4, emp_no{f}#22 + 1[INTEGER] AS y#7]]
     *             \_Filter[salary{f}#27 &lt; 100000[INTEGER] AND emp_no{f}#22 &gt; 0[INTEGER]]
     *               \_EsRelation[test1][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     */
    public void testPushDownFilterOnReferenceAttributesAndFieldAttributesPastUnionAll() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = planSubquery("""
            FROM test, (FROM test1 | where salary < 100000 | EVAL x = 1, y = emp_no + 1)
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
        assertEquals("test1", relation.indexPattern());
    }
}
