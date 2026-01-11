/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PushDownLimitAndOrderByIntoForkTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#18,ASC,LAST]],10[INTEGER],false]
     * \_Fork[[_meta_field{r}#17, emp_no{r}#18, first_name{r}#19, gender{r}#20, hire_date{r}#21, job{r}#22, job.raw{r}#23, l
     * anguages{r}#24, last_name{r}#25, long_noidx{r}#26, salary{r}#27, _fork{r}#28]]
     *   \_Project[[_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, gender{f}#8, hire_date{f}#13, job{f}#14, job.raw{f}#15, lang
     * uages{f}#9, last_name{f}#10, long_noidx{f}#16, salary{f}#11, _fork{r}#4]]
     *     \_TopN[[Order[emp_no{f}#6,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork1[KEYWORD] AS _fork#4]]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_Filter[emp_no{f}#6 > 100[INTEGER]]
     *             \_EsRelation[employees][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testWithASingleBranch() {
        var query = """
            from employees
             | fork (where emp_no > 100)
             | sort emp_no
             | LIMIT 10
            """;
        var plan = optimizedPlan(query);
        var topN = as(plan, TopN.class);
        assertThat(((Literal) topN.limit()).value(), equalTo(10));
        assertOrders(List.of("emp_no", "ASC"), topN);

        var fork = as(topN.child(), Fork.class);
        assertEquals(1, fork.children().size());
        var project = as(fork.children().getFirst(), Project.class);
        topN = as(project.child(), TopN.class);
        assertThat(((Literal) topN.limit()).value(), equalTo(10));
        assertOrders(List.of("emp_no", "ASC"), topN);
        var eval = as(topN.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        assertThat(filter.child(), instanceOf(EsRelation.class));
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#30,ASC,LAST]],10[INTEGER],false]
     * \_Fork[[_meta_field{r}#29, emp_no{r}#30, first_name{r}#31, gender{r}#32, hire_date{r}#33, job{r}#34, job.raw{r}#35, l
     * anguages{r}#36, last_name{r}#37, long_noidx{r}#38, salary{r}#39, _fork{r}#40]]
     *   |_Project[[_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, gender{f}#9, hire_date{f}#14, job{f}#15, job.raw{f}#16, lang
     * uages{f}#10, last_name{f}#11, long_noidx{f}#17, salary{f}#12, _fork{r}#4]]
     *   | \_TopN[[Order[emp_no{f}#7,ASC,LAST]],10[INTEGER],false]
     *   |   \_Eval[[fork1[KEYWORD] AS _fork#4]]
     *   |     \_Limit[1000[INTEGER],false,false]
     *   |       \_Filter[emp_no{f}#7 > 100[INTEGER]]
     *   |         \_EsRelation[employees][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     *   \_Project[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27, l
     * anguages{f}#21, last_name{f}#22, long_noidx{f}#28, salary{f}#23, _fork{r}#4]]
     *     \_TopN[[Order[emp_no{f}#18,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork2[KEYWORD] AS _fork#4]]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_Filter[emp_no{f}#18 < 10[INTEGER]]
     *             \_EsRelation[employees][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     * }</pre>
     */
    public void testSimple() {
        var query = """
            from employees
             | fork (where emp_no > 100)
                    (where emp_no < 10)
             | sort emp_no ASC
             | LIMIT 10
            """;
        var plan = optimizedPlan(query);
        var topN = as(plan, TopN.class);
        assertThat(((Literal) topN.limit()).value(), equalTo(10));
        assertOrders(List.of("emp_no", "ASC"), topN);

        var fork = as(topN.child(), Fork.class);
        assertEquals(2, fork.children().size());

        for (LogicalPlan child : fork.children()) {
            var project = as(child, Project.class);
            topN = as(project.child(), TopN.class);
            assertThat(((Literal) topN.limit()).value(), equalTo(10));
            assertOrders(List.of("emp_no", "ASC"), topN);
            var eval = as(topN.child(), Eval.class);
            var limit = as(eval.child(), Limit.class);
            var filter = as(limit.child(), Filter.class);
            assertThat(filter.child(), instanceOf(EsRelation.class));
        }
    }

    /**
     * <pre>{@code
     * EsqlProject[[_fork{r}#92, emp_no{r}#81, hd{r}#94, x{r}#91, y{r}#93]]
     * \_TopN[[Order[_fork{r}#92,DESC,FIRST], Order[emp_no{r}#81,ASC,LAST], Order[hd{r}#94,DESC,FIRST]],10[INTEGER],false]
     *   \_Fork[[emp_no{r}#81, x{r}#91, _fork{r}#92, y{r}#93, hd{r}#94]]
     *     |_Project[[emp_no{f}#30, x{r}#20, _fork{r}#5, y{r}#64, hd{r}#65]]
     *     | \_TopN[[Order[_fork{r}#5,DESC,FIRST], Order[emp_no{f}#30,ASC,LAST], Order[hd{r}#65,DESC,FIRST]],10[INTEGER],false]
     *     |   \_Eval[[1[LONG] AS x#20, fork1[KEYWORD] AS _fork#5, null[INTEGER] AS y#64, null[DATETIME] AS hd#65]]
     *     |     \_Limit[1000[INTEGER],false,false]
     *     |       \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#30)]
     *     |         \_EsRelation[employees][_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, ..]
     *     |_Project[[emp_no{f}#41, x{r}#66, _fork{r}#5, y{r}#67, hd{r}#68]]
     *     | \_Eval[[fork2[KEYWORD] AS _fork#5, null[LONG] AS x#66, null[INTEGER] AS y#67, null[DATETIME] AS hd#68]]
     *     |   \_TopN[[Order[salary{r}#63,ASC,LAST]],5[INTEGER],false]
     *     |     \_MvExpand[salary{f}#46,salary{r}#63]
     *     |       \_Filter[IN(10081[INTEGER],10087[INTEGER],emp_no{f}#41)]
     *     |         \_EsRelation[employees][_meta_field{f}#47, emp_no{f}#41, first_name{f}#42, ..]
     *     \_Project[[emp_no{r}#70, x{r}#14, _fork{r}#5, y{r}#17, hd{r}#12]]
     *       \_TopN[[Order[_fork{r}#5,DESC,FIRST], Order[emp_no{r}#70,ASC,LAST], Order[hd{r}#12,DESC,FIRST]],10[INTEGER],false]
     *         \_Eval[[fork3[KEYWORD] AS _fork#5, null[INTEGER] AS emp_no#70]]
     *           \_Limit[1000[INTEGER],false,false]
     *             \_Aggregate[[hd{r}#12],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS x#14, MIN(emp_no{f}#52,true[BOOLEAN],PT0S[TI
     * ME_DURATION]) AS y#17, hd{r}#12]]
     *               \_Eval[[DATETRUNC(P2Y[DATE_PERIOD],hire_date{f}#59) AS hd#12]]
     *                 \_EsRelation[employees][_meta_field{f}#58, emp_no{f}#52, first_name{f}#53, ..]
     * }</pre>
     */
    public void testWithMixedBranches() {
        var query = """
            FROM employees
            | FORK (WHERE emp_no == 10048 OR emp_no == 10081 | EVAL x = 1::long)
                   (WHERE emp_no == 10081 OR emp_no == 10087 | MV_EXPAND salary | SORT salary | LIMIT 5)
                   (STATS x = COUNT(*), y = MIN(emp_no) by hd = DATE_TRUNC(2 year, hire_date))
            | KEEP _fork, emp_no, hd, x, y
            | SORT _fork DESC, emp_no, hd DESC
            | LIMIT 10
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);

        assertOrders(List.of("_fork", "DESC", "emp_no", "ASC", "hd", "DESC"), topN);
        assertThat(((Literal) topN.limit()).value(), equalTo(10));

        var fork = as(topN.child(), Fork.class);
        assertEquals(3, fork.children().size());

        var firstFork = as(fork.children().getFirst(), Project.class);
        // in the first branch we push down TopN
        topN = as(firstFork.child(), TopN.class);
        assertOrders(List.of("_fork", "DESC", "emp_no", "ASC", "hd", "DESC"), topN);
        assertThat(((Literal) topN.limit()).value(), equalTo(10));
        var eval = as(topN.child(), Eval.class);
        var limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1000));
        var filter = as(limit.child(), Filter.class);
        assertThat(filter.child(), instanceOf(EsRelation.class));

        var secondFork = as(fork.children().get(1), Project.class);
        eval = as(secondFork.child(), Eval.class);
        topN = as(eval.child(), TopN.class);
        // in the second branch we don't push down TopN
        assertThat(((Literal) topN.limit()).value(), equalTo(5));
        assertOrders(List.of("salary", "ASC"), topN);
        var mvExpand = as(topN.child(), MvExpand.class);
        filter = as(mvExpand.child(), Filter.class);
        assertThat(filter.child(), instanceOf(EsRelation.class));

        var thirdFork = as(fork.children().get(2), Project.class);
        topN = as(thirdFork.child(), TopN.class);
        // in the third branch, we push down TopN
        assertOrders(List.of("_fork", "DESC", "emp_no", "ASC", "hd", "DESC"), topN);
        assertThat(((Literal) topN.limit()).value(), equalTo(10));
        eval = as(topN.child(), Eval.class);
        limit = as(eval.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1000));
        var aggregate = as(limit.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        assertThat(eval.child(), instanceOf(EsRelation.class));
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#32,ASC,LAST]],20[INTEGER],false]
     * \_Fork[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37, l
     * anguages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, _fork{r}#42]]
     *   |_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18, la
     * nguages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14, _fork{r}#5]]
     *   | \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *   |   \_TopN[[Order[salary{f}#14,ASC,LAST]],10[INTEGER],false]
     *   |     \_Filter[emp_no{f}#9 > 100[INTEGER]]
     *   |       \_EsRelation[employees][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *   \_Project[[_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, gender{f}#22, hire_date{f}#27, job{f}#28, job.raw{f}#29, l
     * anguages{f}#23, last_name{f}#24, long_noidx{f}#30, salary{f}#25, _fork{r}#5]]
     *     \_Eval[[fork2[KEYWORD] AS _fork#5]]
     *       \_TopN[[Order[salary{f}#25,ASC,LAST]],10[INTEGER],false]
     *         \_Filter[emp_no{f}#20 < 10[INTEGER]]
     *           \_EsRelation[employees][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     * }</pre>
     */
    public void testNoPushDownWhenSmallerLimitExists() {
        var query = """
            from employees
             | fork (where emp_no > 100 | SORT salary | LIMIT 10)
                    (where emp_no < 10  | SORT salary | LIMIT 10)
             | sort emp_no
             | LIMIT 20
            """;
        var plan = optimizedPlan(query);
        var topN = as(plan, TopN.class);
        assertOrders(List.of("emp_no", "ASC"), topN);
        assertThat(((Literal) topN.limit()).value(), equalTo(20));

        var fork = as(topN.child(), Fork.class);
        assertEquals(2, fork.children().size());

        for (LogicalPlan child : fork.children()) {
            var project = as(child, Project.class);
            var eval = as(project.child(), Eval.class);

            topN = as(eval.child(), TopN.class);
            assertThat(((Literal) topN.limit()).value(), equalTo(10));
            assertOrders(List.of("salary", "ASC"), topN);
            var filter = as(topN.child(), Filter.class);
            assertThat(filter.child(), instanceOf(EsRelation.class));
        }
    }

    private void assertOrders(List<String> expectedOrders, TopN topN) {
        assertEquals(expectedOrders.size(), topN.order().size() * 2);

        int i = 0;
        for (Order order : topN.order()) {
            String actualName = as(order.child(), NamedExpression.class).name();
            assertThat(actualName, equalTo(expectedOrders.get(i++)));
            assertThat(order.direction().name(), equalTo(expectedOrders.get(i++)));
        }
    }
}
