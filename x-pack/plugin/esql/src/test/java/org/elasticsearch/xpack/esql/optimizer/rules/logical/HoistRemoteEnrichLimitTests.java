/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class HoistRemoteEnrichLimitTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * <pre>
     * Limit[10[INTEGER],true,false]
     * \_Enrich[REMOTE,languages_remote[KEYWORD],id{r}#4,{"match":{"indices":[],"match_field":"id",
     * "enrich_fields":["language_code","language_name"]}},{=languages_idx},[language_code{r}#20, language_name{r}#21]]
     *   \_Eval[[emp_no{f}#6 AS id#4]]
     *     \_Limit[10[INTEGER],false,true]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ...]
     * </pre>
     */
    public void testLimitWithinRemoteEnrich() {
        var plan = plan(randomFrom("""
            from test
            | EVAL id = emp_no
            | LIMIT 10
            | ENRICH _remote:languages_remote
            """, """
            from test
            | LIMIT 10
            | EVAL id = emp_no
            | ENRICH _remote:languages_remote
            """)); // it should be the same in any order

        var limit = as(plan, Limit.class);
        assertTrue(limit.duplicated());
        assertFalse(limit.local());
        var enrich = as(limit.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var eval = as(enrich.child(), Eval.class);
        var innerLimit = as(eval.child(), Limit.class);
        assertFalse(innerLimit.duplicated());
        assertTrue(innerLimit.local());
    }

    /**
     * <pre>
     * Limit[10[INTEGER],true,false]
     * \_Enrich[REMOTE,languages_remote[KEYWORD],id{r}#4,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_cod
     * e","language_name"]}},{=languages_idx},[language_code{r}#20, language_name{r}#21]]
     *   \_Eval[[emp_no{f}#6 AS id#4]]
     *     \_Limit[10[INTEGER],false,true]
     *       \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * </pre>
     */
    public void testLimitWithinRemoteEnrichAndAfter() {
        var plan = plan("""
            from test
            | LIMIT 20
            | EVAL id = emp_no
            | ENRICH _remote:languages_remote
            | LIMIT 10
            """); // it should be the same in any order

        var limit = as(plan, Limit.class);
        assertTrue(limit.duplicated());
        assertFalse(limit.local());
        var enrich = as(limit.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var eval = as(enrich.child(), Eval.class);
        var innerLimit = as(eval.child(), Limit.class);
        assertFalse(innerLimit.duplicated());
        assertTrue(innerLimit.local());
    }

    // Same as above but limits are reversed
    public void testLimitWithinRemoteEnrichAndAfterHigher() {
        var plan = plan("""
            from test
            | LIMIT 10
            | EVAL id = emp_no
            | ENRICH _remote:languages_remote
            | LIMIT 20
            """); // it should be the same in any order

        var limit = as(plan, Limit.class);
        assertTrue(limit.duplicated());
        assertFalse(limit.local());
        var enrich = as(limit.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var eval = as(enrich.child(), Eval.class);
        var innerLimit = as(eval.child(), Limit.class);
        assertFalse(innerLimit.duplicated());
        assertTrue(innerLimit.local());
    }

    /**
     * <pre>
     * Project[[salary{f}#19 AS wage#10, emp_no{f}#14 AS id#4, first_name{r}#11, language_code{r}#28, language_name{r}#29]]
     * \_Limit[5[INTEGER],true,false]
     *   \_Enrich[REMOTE,languages_remote[KEYWORD],emp_no{f}#14,{"match":{"indices":[],"match_field":"id","enrich_fields":["languag
     * e_code","language_name"]}},{=languages_idx},[language_code{r}#28, language_name{r}#29]]
     *     \_Dissect[first_name{f}#15,Parser[pattern=%{first_name}s, appendSeparator=, parser=org.elasticsearch.dissect.DissectParse
     * r@7d5c7931],[first_name{r}#11]]
     *       \_Limit[5[INTEGER],false,true]
     *         \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * </pre>
     */
    public void testManyLimitsWithinRemoteEnrich() {
        var plan = plan("""
            from test
            | LIMIT 10
            | EVAL id = emp_no
            | KEEP first_name, salary, id
            | RENAME salary AS wage
            | DISSECT first_name "%{first_name}s"
            | LIMIT 5
            | ENRICH _remote:languages_remote
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        assertTrue(limit.duplicated());
        assertFalse(limit.local());
        assertThat(Foldables.limitValue(limit.limit(), limit.sourceText()), equalTo(5));
        var enrich = as(limit.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var dissect = as(enrich.child(), Dissect.class);
        var innerLimit = as(dissect.child(), Limit.class);
        assertFalse(innerLimit.duplicated());
        assertTrue(innerLimit.local());
        assertThat(Foldables.limitValue(innerLimit.limit(), innerLimit.sourceText()), equalTo(5));
    }

    /**
     * Project[[first_name{f}#14, salary{f}#18 AS wage#11, emp_no{f}#13 AS id#4, language_code{r}#32, language_name{r}#33]]
     * \_Limit[5[INTEGER],true,false]
     *   \_Enrich[REMOTE,languages_remote[KEYWORD],emp_no{f}#13,{"match":{"indices":[],"match_field":"id","enrich_fields":["languag
     * e_code","language_name"]}},{=languages_idx},[language_code{r}#32, language_name{r}#33]]
     *     \_Limit[5[INTEGER],true,true]
     *       \_Enrich[REMOTE,languages_remote[KEYWORD],emp_no{f}#13,{"match":{"indices":[],"match_field":"id","enrich_fields":["languag
     * e_code","language_name"]}},{=languages_idx},[language_code{r}#27, language_name{r}#28]]
     *         \_Limit[5[INTEGER],false,true]
     *           \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     */
    public void testLimitsWithinRemoteEnrichTwice() {
        var plan = plan("""
            from test
            | LIMIT 10
            | EVAL id = emp_no
            | KEEP first_name, salary, id
            | ENRICH _remote:languages_remote
            | RENAME salary AS wage
            | LIMIT 5
            | ENRICH _remote:languages_remote
            """);
        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        assertTrue(limit.duplicated());
        assertFalse(limit.local());
        assertThat(Foldables.limitValue(limit.limit(), limit.sourceText()), equalTo(5));
        var enrich = as(limit.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var innerLimit = as(enrich.child(), Limit.class);
        assertTrue(innerLimit.duplicated());
        assertTrue(innerLimit.local());
        assertThat(Foldables.limitValue(innerLimit.limit(), innerLimit.sourceText()), equalTo(5));
        var secondEnrich = as(innerLimit.child(), Enrich.class);
        assertThat(secondEnrich.mode(), is(Enrich.Mode.REMOTE));
        var innermostLimit = as(secondEnrich.child(), Limit.class);
        assertFalse(innermostLimit.duplicated());
        assertTrue(innermostLimit.local());
        assertThat(Foldables.limitValue(innermostLimit.limit(), innermostLimit.sourceText()), equalTo(5));
    }

    // These cases do not get hoisting, and it's ok
    public void testLimitWithinOtherEnrich() {
        String enrichPolicy = randomFrom("languages_idx", "_any:languages_idx", "_coordinator:languages_coordinator");
        var plan = plan(String.format(Locale.ROOT, """
            from test
            | EVAL id = emp_no
            | LIMIT 10
            | ENRICH %s
            """, enrichPolicy));
        // Here ENRICH is on top - no hoisting happens
        var enrich = as(plan, Enrich.class);
        assertThat(enrich.mode(), not(is(Enrich.Mode.REMOTE)));
    }

    // Non-cardinality preserving commands after limit
    public void testFilterLimitThenEnrich() {
        // Hoisting does not happen, so the verifier fails since LIMIT is before remote ENRICH
        failPlan("""
            from test
            | EVAL id = emp_no
            | LIMIT 10
            | WHERE first_name != "john"
            | ENRICH _remote:languages_remote
            """, "ENRICH with remote policy can't be executed after [LIMIT 10]");
    }

    public void testMvExpandLimitThenEnrich() {
        // Hoisting does not happen, so the verifier fails since LIMIT is before remote ENRICH
        failPlan("""
            from test
            | EVAL id = emp_no
            | LIMIT 10
            | MV_EXPAND languages
            | ENRICH _remote:languages_remote
            """, "MV_EXPAND after LIMIT is incompatible with remote ENRICH");
    }

    // Other cases where hoisting does not happen:
    // - ExecutesOn.COORDINATOR - this fails the verifier
    // - PipelineBreaker - all relevant ones are also ExecutesOn.COORDINATOR
}
