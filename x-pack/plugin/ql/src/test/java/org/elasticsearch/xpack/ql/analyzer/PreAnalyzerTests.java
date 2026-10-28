/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.analyzer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.analyzer.PreAnalyzer.PreAnalysis;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PreAnalyzerTests extends ESTestCase {

    private PreAnalyzer preAnalyzer = new PreAnalyzer();

    public void testBasicIndex() {
        LogicalPlan plan = new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "index"), null, false);
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), nullValue());
        assertThat(result.indices.get(0).id().index(), is("index"));
    }

    public void testBasicIndexWithCatalog() {
        LogicalPlan plan = new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, "elastic", "index"), null, false);
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), is("elastic"));
        assertThat(result.indices.get(0).id().index(), is("index"));
    }

    public void testBasicIndexWithSelector() {
        LogicalPlan plan = new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "index::failures"), null, false);
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), nullValue());
        assertThat(result.indices.get(0).id().index(), is("index::failures"));
    }

    public void testComplicatedQuery() {
        LogicalPlan plan = new Limit(
            EMPTY,
            new Literal(EMPTY, 10, INTEGER),
            new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, "aaa"), null, false)
        );
        PreAnalysis result = preAnalyzer.preAnalyze(plan);
        assertThat(plan.preAnalyzed(), is(true));
        assertThat(result.indices, hasSize(1));
        assertThat(result.indices.get(0).id().cluster(), nullValue());
        assertThat(result.indices.get(0).id().index(), is("aaa"));
    }
}
