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
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.QlClientException;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import static org.elasticsearch.xpack.ql.type.TypesTests.loadMapping;

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

    public void testBasicPlan() throws Exception {
        expectThrows(QlClientException.class, "not yet implemented", () -> plan("process where true"));
    }
}
