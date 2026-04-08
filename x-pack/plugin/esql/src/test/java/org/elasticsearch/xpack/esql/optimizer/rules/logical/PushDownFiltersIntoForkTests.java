/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PushDownFiltersIntoForkTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#32,ASC,LAST]],10[INTEGER],false]
     * \_Fork[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37, l
     * anguages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, _fork{r}#42]]
     *   |_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18, la
     * nguages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14, _fork{r}#5]]
     *   | \_TopN[[Order[emp_no{f}#9,ASC,LAST]],10[INTEGER],false]
     *   |   \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *   |     \_Filter[emp_no{f}#9 > 100[INTEGER] AND salary{f}#14 > 10[INTEGER]]
     *   |       \_EsRelation[employees][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *   \_Project[[_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, gender{f}#22, hire_date{f}#27, job{f}#28, job.raw{f}#29, l
     * anguages{f}#23, last_name{f}#24, long_noidx{f}#30, salary{f}#25, _fork{r}#6]]
     *     \_TopN[[Order[emp_no{f}#20,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork2[KEYWORD] AS _fork#6]]
     *         \_Filter[emp_no{f}#20 < 10[INTEGER] AND salary{f}#25 > 10[INTEGER]]
     *           \_EsRelation[employees][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     * }</pre>
     */
    public void testSingleFilterPushDown() {
        var query = """
            from employees
             | fork (where emp_no > 100)
                    (where emp_no < 10)
             | where salary > 10
             | sort emp_no
             | limit 10
            """;
        var plan = planWithoutForkImplicitLimit(query);
        var topN = as(plan, TopN.class);
        var fork = as(topN.child(), Fork.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#32,ASC,LAST]],10[INTEGER],false]
     * \_Fork[[_meta_field{r}#31, emp_no{r}#32, first_name{r}#33, gender{r}#34, hire_date{r}#35, job{r}#36, job.raw{r}#37, l
     * anguages{r}#38, last_name{r}#39, long_noidx{r}#40, salary{r}#41, _fork{r}#42]]
     *   \_Project[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18, la
     * nguages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14, _fork{r}#5]]
     *     \_TopN[[Order[emp_no{f}#9,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *         \_Filter[emp_no{f}#9 > 100[INTEGER]]
     *           \_EsRelation[employees][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testPushDownFilterAndBranchTrimming() {
        var query = """
            from employees
             | fork (where emp_no > 100)
                    (where emp_no < 10)
             | where _fork == "fork1"
             | sort emp_no
             | limit 10
            """;
        var plan = planWithoutForkImplicitLimit(query);
        var topN = as(plan, TopN.class);
        var fork = as(topN.child(), Fork.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#33,ASC,LAST]],10[INTEGER],false]
     * \_Fork[[_meta_field{r}#32, emp_no{r}#33, first_name{r}#34, gender{r}#35, hire_date{r}#36, job{r}#37, job.raw{r}#38, l
     * anguages{r}#39, last_name{r}#40, long_noidx{r}#41, salary{r}#42, _fork{r}#43]]
     *   |_Project[[_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18, job.raw{f}#19, l
     * anguages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15, _fork{r}#5]]
     *   | \_TopN[[Order[emp_no{f}#10,ASC,LAST]],10[INTEGER],false]
     *   |   \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *   |     \_Filter[emp_no{f}#10 > 100[INTEGER] AND salary{f}#15 > 10[INTEGER] AND first_name{f}#11 == John[KEYWORD]]
     *   |       \_EsRelation[employees][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *   \_Project[[_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, gender{f}#23, hire_date{f}#28, job{f}#29, job.raw{f}#30, l
     * anguages{f}#24, last_name{f}#25, long_noidx{f}#31, salary{f}#26, _fork{r}#6]]
     *     \_TopN[[Order[emp_no{f}#21,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork2[KEYWORD] AS _fork#6]]
     *         \_Filter[emp_no{f}#21 < 10[INTEGER] AND salary{f}#26 > 10[INTEGER] AND first_name{f}#22 == John[KEYWORD]]
     *           \_EsRelation[employees][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     * }</pre>
     */
    public void testMultiplePushDown() {
        var query = """
            from employees
             | fork (where emp_no > 100)
                    (where emp_no < 10)
             | where salary > 10
             | sort emp_no
             | where first_name == "John"
             | limit 10
            """;
        var plan = planWithoutForkImplicitLimit(query);
        var topN = as(plan, TopN.class);
        var fork = as(topN.child(), Fork.class);
    }

    /**
     * <pre>{@code
     * TopN[[Order[emp_no{r}#33,ASC,LAST]],10[INTEGER],false]
     * \_Filter[salary{r}#42 > 10[INTEGER]]
     *   \_Fork[[_meta_field{r}#32, emp_no{r}#33, first_name{r}#34, gender{r}#35, hire_date{r}#36, job{r}#37, job.raw{r}#38, l
     * anguages{r}#39, last_name{r}#40, long_noidx{r}#41, salary{r}#42, _fork{r}#43]]
     *     |_Project[[_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18, job.raw{f}#19, l
     * anguages{f}#13, last_name{f}#14, long_noidx{f}#20, salary{f}#15, _fork{r}#6]]
     *     | \_Eval[[fork1[KEYWORD] AS _fork#6]]
     *     |   \_Limit[100[INTEGER],false,false]
     *     |     \_Filter[emp_no{f}#10 > 100[INTEGER]]
     *     |       \_EsRelation[employees][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *     \_Project[[_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, gender{f}#23, hire_date{f}#28, job{f}#29, job.raw{f}#30, l
     * anguages{f}#24, last_name{f}#25, long_noidx{f}#31, salary{f}#26, _fork{r}#7]]
     *       \_Eval[[fork2[KEYWORD] AS _fork#7]]
     *         \_TopN[[Order[emp_no{f}#21,ASC,LAST]],10[INTEGER],false]
     *           \_Filter[emp_no{f}#21 < 10[INTEGER]]
     *             \_EsRelation[employees][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     * }</pre>
     */
    public void testNoPushDownWhenPipelineBreaker() {
        var query = """
            from employees
             | fork (where emp_no > 100 | LIMIT 100)
                    (where emp_no < 10 | SORT emp_no | LIMIT 10)
             | where salary > 10
             | sort emp_no
             | limit 10
            """;
        var plan = planWithoutForkImplicitLimit(query);
        var topN = as(plan, TopN.class);
        var fork = as(topN.child(), Fork.class);
    }
}
