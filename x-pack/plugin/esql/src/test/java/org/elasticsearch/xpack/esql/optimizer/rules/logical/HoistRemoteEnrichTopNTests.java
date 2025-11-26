/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.Map;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

// @TestLogging(reason = "debug", value = "org.elasticsearch.xpack.esql.optimizer:TRACE")
public class HoistRemoteEnrichTopNTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * <pre>
     * TopN[[Order[emp_no{f}#7,ASC,LAST]],10[INTEGER],false]
     * \_Enrich[REMOTE,languages_remote[KEYWORD],id{r}#4,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_cod
     * e","language_name"]}},{=languages_idx},[language_code{r}#21, language_name{r}#22]]
     *   \_TopN[[Order[emp_no{f}#7,ASC,LAST]],10[INTEGER],true]
     *     \_Eval[[emp_no{f}#7 AS id#4]]
     *       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * </pre>
     */
    public void testLimitWithinRemoteEnrich() {
        var plan = plan("""
            from test
            | EVAL id = emp_no
            | SORT emp_no
            | LIMIT 10
            | ENRICH _remote:languages_remote
            """);

        var topn = as(plan, TopN.class);
        assertFalse(topn.local());
        var enrich = as(topn.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var innerTopN = as(enrich.child(), TopN.class);
        assertTrue(innerTopN.local());
        as(innerTopN.child(), Eval.class);
    }

    /** <pre>
     * Project[[_meta_field{f}#16, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18, job.raw{f}#19, languages{f}#13
     * , last_name{f}#14, long_noidx{f}#20, salary{f}#15, id{r}#4, emp_no{r}#8, language_code{r}#24, language_name{r}#25]]
     * \_TopN[[Order[$$emp_no$temp_name$26{r$}#27,ASC,LAST]],10[INTEGER],false]
     *   \_Enrich[REMOTE,languages_remote[KEYWORD],id{r}#4,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_cod
     * e","language_name"]}},{=languages_idx},[language_code{r}#24, language_name{r}#25]]
     *     \_Eval[[emp_no{f}#10 + 1[INTEGER] AS emp_no#8]]
     *       \_Eval[[emp_no{f}#10 AS $$emp_no$temp_name$26#27]]
     *         \_TopN[[Order[emp_no{f}#10,ASC,LAST]],10[INTEGER],true]
     *           \_Eval[[emp_no{f}#10 AS id#4]]
     *             \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * </pre>
     */
    public void testLimitWithinRemoteEnrichShadow() {
        var plan = plan("""
            from test
            | EVAL id = emp_no
            | SORT emp_no
            | LIMIT 10
            | EVAL emp_no = emp_no + 1
            | ENRICH _remote:languages_remote
            """);

        var proj = as(plan, Project.class);
        var topn = as(proj.child(), TopN.class);
        assertFalse(topn.local());
        Order topNOrder = topn.order().getFirst();
        NamedExpression expr = as(topNOrder.child(), NamedExpression.class);
        assertThat(expr.name(), startsWith("$$emp_no$temp_name$"));
        var enrich = as(topn.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        // EVAL emp_no = emp_no + 1
        var eval1 = as(enrich.child(), Eval.class);
        var evalAlias = as(eval1.expressions().getFirst(), Alias.class);
        assertThat(evalAlias.name(), equalTo("emp_no"));
        // Generated aliasing Eval
        var eval2 = as(eval1.child(), Eval.class);
        var evalAlias2 = as(eval2.expressions().getFirst(), Alias.class);
        var evalName2 = as(evalAlias2.child(), NamedExpression.class);
        assertThat(evalName2.name(), equalTo("emp_no"));
        var innerTopN = as(eval2.child(), TopN.class);
        assertTrue(innerTopN.local());
    }

    private LogicalPlan planWithPolicyOverride(String query) {
        // Set up index and enrich policy with overlapping fields
        var enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.REMOTE,
            MATCH_TYPE,
            "hosts",
            "host",
            "hosts",
            "mapping-hosts.json"
        );
        var mapping = loadMapping("mapping-host_inventory.json");
        EsIndex inventory = EsIndexGenerator.esIndex("host_inventory", mapping, Map.of("host_inventory", IndexMode.STANDARD));
        var analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(inventory),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var analyzed = analyzer.analyze(parser.createStatement(query));
        return logicalOptimizer.optimize(analyzed);
    }

    /**
     * Test case for aliasing within TopN + Enrich. This happens when Enrich had a field that overrides an existing field,
     * so we need to alias it.
     * <pre>
     *     Project[[host.name{f}#10, host.os{f}#11, host.version{f}#12, host.name{f}#10 AS host#5, host_group{r}#21, description{
     * r}#22, card{r}#23, ip0{r}#24, ip1{r}#25]]
     * \_Project[[host.name{f}#10, host.os{f}#11, host.version{f}#12, host_group{r}#21, description{r}#22, card{r}#23, ip0{r}#2
     * 4, ip1{r}#25]]
     *   \_TopN[[Order[$$description$temp_name$27{r$}#28,ASC,LAST]],10[INTEGER],false]
     *     \_Enrich[REMOTE,hosts[KEYWORD],host.name{f}#10,{"match":{"indices":[],"match_field":"host","enrich_fields":["host_group","
     * description","card","ip0","ip1"]}},{=hosts},[host_group{r}#21, description{r}#22, card{r}#23, ip0{r}#24, ip1{r}#
     * 25]]
     *       \_Eval[[description{f}#13 AS $$description$temp_name$27#28]]
     *         \_TopN[[Order[description{f}#13,ASC,LAST]],10[INTEGER],true]
     *           \_EsRelation[host_inventory][description{f}#13, host.name{f}#10, host.os{f}#11, ..]
     * </pre>
     * TODO: probably makes sense to remove double project, but this can be done later
     */
    public void testTopNWithinRemoteEnrichAliasing() {
        var query = ("""
            from host_inventory
            | SORT description
            | LIMIT 10
            | EVAL host = host.name
            | KEEP host*, description
            | ENRICH _remote:hosts
            """);
        LogicalPlan plan = planWithPolicyOverride(query);

        var proj1 = as(plan, Project.class);
        var proj2 = as(proj1.child(), Project.class);
        var topn = as(proj2.child(), TopN.class);
        assertFalse(topn.local());
        Order topNOrder = topn.order().getFirst();
        NamedExpression expr = as(topNOrder.child(), NamedExpression.class);
        assertThat(expr.name(), startsWith("$$description$temp_name$"));
        var enrich = as(topn.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var eval = as(enrich.child(), Eval.class);
        var evalAlias = as(eval.expressions().getFirst(), Alias.class);
        var evalName = as(evalAlias.child(), NamedExpression.class);
        assertThat(evalName.name(), equalTo("description"));
        var innerTopN = as(eval.child(), TopN.class);
        assertTrue(innerTopN.local());
    }

    public void testTopNSortFieldsWithinRemoteEnrichAliasing() {
        // Now let's try sort which has more than one field
        var query = ("""
            from host_inventory
            | SORT host.name, description
            | LIMIT 10
            | EVAL host = host.name
            | KEEP host*, description
            | ENRICH _remote:hosts
            """);
        LogicalPlan plan = planWithPolicyOverride(query);

        var proj1 = as(plan, Project.class);
        var proj2 = as(proj1.child(), Project.class);
        var topn = as(proj2.child(), TopN.class);
        assertFalse(topn.local());
        Order topNOrder = topn.order().get(1);
        NamedExpression expr = as(topNOrder.child(), NamedExpression.class);
        assertThat(expr.name(), startsWith("$$description$temp_name$"));
        var enrich = as(topn.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var eval = as(enrich.child(), Eval.class);
        var evalAlias = as(eval.expressions().getFirst(), Alias.class);
        var evalName = as(evalAlias.child(), NamedExpression.class);
        assertThat(evalName.name(), equalTo("description"));
        var innerTopN = as(eval.child(), TopN.class);
        assertTrue(innerTopN.local());
    }

    /**
     * Plan with functions in sort:
     * <pre>
     * Project[[host.name{f}#13, host.os{f}#14, host.version{f}#15, host.name{f}#13 AS host#8, host_group{r}#24, description{
     * r}#25, card{r}#26, ip0{r}#27, ip1{r}#28]]
     * \_TopN[[Order[host.version{f}#15,ASC,LAST], Order[$$order_by$1$0{r}#30,ASC,LAST], Order[$$order_by$2$1{r}#31,ASC,LAST
     * ], Order[host.os{f}#14,ASC,LAST]],10[INTEGER],false]
     *   \_Enrich[REMOTE,hosts[KEYWORD],host.name{f}#13,{"match":{"indices":[],"match_field":"host","enrich_fields":["host_group","
     * description","card","ip0","ip1"]}},{=hosts},[host_group{r}#24, description{r}#25, card{r}#26, ip0{r}#27, ip1{r}#
     * 28]]
     *     \_TopN[[Order[host.version{f}#15,ASC,LAST], Order[$$order_by$1$0{r}#30,ASC,LAST], Order[$$order_by$2$1{r}#31,ASC,LAST
     * ], Order[host.os{f}#14,ASC,LAST]],10[INTEGER],true]
     *       \_Eval[[LENGTH(description{f}#16) AS $$order_by$1$0#30, TOLOWER(description{f}#16) AS $$order_by$2$1#31]]
     *         \_EsRelation[host_inventory][description{f}#16, host.name{f}#13, host.os{f}#14, ..]
     *  </pre>
     */
    public void testTopNSortExpressionWithinRemoteEnrichAliasing() {
        // Now let's try sort which has more than one field
        var query = ("""
            from host_inventory
            | SORT host.version, LENGTH(description), to_lower(description), host.os
            | LIMIT 10
            | EVAL host = host.name
            | KEEP host*, description
            | ENRICH _remote:hosts
            """);
        LogicalPlan plan = planWithPolicyOverride(query);

        var proj1 = as(plan, Project.class);
        var topn = as(proj1.child(), TopN.class);
        assertFalse(topn.local());
        Order topNOrder = topn.order().get(1);
        NamedExpression expr = as(topNOrder.child(), NamedExpression.class);
        assertThat(expr.name(), startsWith("$$order_by$1$0"));
        var enrich = as(topn.child(), Enrich.class);
        assertThat(enrich.mode(), is(Enrich.Mode.REMOTE));
        var innerTopN = as(enrich.child(), TopN.class);
        assertTrue(innerTopN.local());
    }

    public void testFilterLimitThenEnrich() {
        // Hoisting does not happen, so the verifier fails since TopN is before remote ENRICH
        failPlan("""
            from test
            | EVAL id = emp_no
            | SORT emp_no
            | LIMIT 10
            | WHERE first_name != "john"
            | ENRICH _remote:languages_remote
            """, "ENRICH with remote policy can't be executed after [SORT emp_no]");
    }

    public void testMvExpandLimitThenEnrich() {
        failPlan("""
            from test
            | EVAL id = emp_no
            | SORT emp_no
            | LIMIT 10
            | MV_EXPAND languages
            | ENRICH _remote:languages_remote
            """, "MV_EXPAND after LIMIT is incompatible with remote ENRICH");
    }

    public void testTwoSortsWithinRemoteEnrich() {
        failPlan("""
            from test
            | EVAL id = emp_no
            | SORT emp_no
            | LIMIT 10
            | SORT id
            | LIMIT 5
            | ENRICH _remote:languages_remote
            """, "ENRICH with remote policy can't be executed after [SORT emp_no]");
    }
}
