/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlTestUtils;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import static org.elasticsearch.xpack.ql.type.TypesTests.loadMapping;

public abstract class AbstractQueryTranslatorTestCase extends ESTestCase {
    protected EqlParser parser = new EqlParser();
    protected PreAnalyzer preAnalyzer = new PreAnalyzer();
    protected PostAnalyzer postAnalyzer = new PostAnalyzer();
    protected EqlConfiguration configuration = EqlTestUtils.randomConfiguration();
    protected Analyzer analyzer = new Analyzer(configuration, new EqlFunctionRegistry(), new Verifier(new Metrics()));
    protected Optimizer optimizer = new Optimizer();
    protected Planner planner = new Planner();

    protected IndexResolution index = IndexResolution.valid(new EsIndex("test", loadMapping("mapping-default.json", true)));

    protected PhysicalPlan plan(IndexResolution resolution, String eql) {
        return planner.plan(optimizer.optimize(postAnalyzer.postAnalyze(analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql),
                resolution)), configuration)));
    }

    protected PhysicalPlan plan(String eql) {
        return plan(index, eql);
    }
}
