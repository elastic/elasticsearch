/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.TypesTests.loadMapping;
import static org.hamcrest.Matchers.containsString;

public class QueryFolderTests extends ESTestCase {

    private EqlParser parser = new EqlParser();
    private PreAnalyzer preAnalyzer = new PreAnalyzer();
    private Analyzer analyzer = new Analyzer(new EqlFunctionRegistry(), new Verifier());
    private Optimizer optimizer = new Optimizer();
    private Planner planner = new Planner();

    private IndexResolution index = IndexResolution.valid(new EsIndex("test", loadMapping("mapping-default.json")));


    private PhysicalPlan plan(IndexResolution resolution, String eql) {
        return planner.plan(optimizer.optimize(analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution))));
    }

    private PhysicalPlan plan(String eql) {
        return plan(index, eql);
    }
    
    public void testBasicPlan() {
        PhysicalPlan p = plan("process where true");
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(22, eqe.output().size());
        assertEquals(KEYWORD, eqe.output().get(0).dataType());
        String query = eqe.queryContainer().toString().replaceAll("\\s+", "");
        // test query term
        assertThat(query, containsString("\"term\":{\"event_type\":{\"value\":\"process\""));
        // test field source extraction
        assertThat(query, containsString("\"_source\":{\"includes\":[],\"excludes\":[]"));
    }
}
