/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.stats.Metrics;

import java.util.List;

import static org.elasticsearch.xpack.ql.type.DateUtils.UTC;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.startsWith;

public class AnalyzerTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();
    private final IndexResolution indexResolution = IndexResolution.valid(
        new EsIndex("test", loadMapping("mapping-multi-field-with-nested.json"))
    );

    private LogicalPlan analyze(String sql, SqlTypedParamValue... parameters) {
        Analyzer analyzer = new Analyzer(SqlTestUtils.TEST_CFG, new SqlFunctionRegistry(), indexResolution, new Verifier(new Metrics()));
        return analyzer.analyze(parser.createStatement(sql, List.of(parameters), UTC), true);
    }

    public void testMultipleParamLiteralsWithUnresolvedAliases() {
        LogicalPlan logicalPlan = analyze("SELECT ?, ? FROM test",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("integer", 200)
        );
        List<? extends NamedExpression> projections = ((Project) logicalPlan).projections();
        assertThat(projections, everyItem(isA(Alias.class)));
        assertThat(projections.get(0).toString(), startsWith("100 AS ?#"));
        assertThat(projections.get(1).toString(), startsWith("200 AS ?2#"));
    }

    public void testParamLiteralsWithUnresolvedAliasesAndMixedTypes() {
        LogicalPlan logicalPlan = analyze("SELECT ?, ? FROM test",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("text", "200")
        );
        List<? extends NamedExpression> projections = ((Project) logicalPlan).projections();
        assertThat(projections, everyItem(isA(Alias.class)));
        assertThat(projections.get(0).toString(), startsWith("100 AS ?#"));
        assertThat(projections.get(1).toString(), startsWith("200 AS ?2#"));
    }

    public void testParamLiteralsWithResolvedAndUnresolvedAliases() {
        LogicalPlan logicalPlan = analyze("SELECT ?, ? as x, ? FROM test",
            new SqlTypedParamValue("integer", 100),
            new SqlTypedParamValue("integer", 200),
            new SqlTypedParamValue("integer", 300)
        );
        List<? extends NamedExpression> projections = ((Project) logicalPlan).projections();
        assertThat(projections, everyItem(isA(Alias.class)));
        assertThat(projections.get(0).toString(), startsWith("100 AS ?#"));
        assertThat(projections.get(1).toString(), startsWith("200 AS x#"));;
        assertThat(projections.get(2).toString(), startsWith("300 AS ?2#"));;
    }

}
