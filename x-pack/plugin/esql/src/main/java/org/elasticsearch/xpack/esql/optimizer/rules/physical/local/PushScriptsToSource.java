/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

public class PushScriptsToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<EvalExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(EvalExec evalExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = evalExec;
        if (evalExec.child() instanceof EsQueryExec esQueryExec) {
            List<Alias> aliases = evalExec.fields();
            boolean aliasPushed = false;
            List<Alias> nonPushedAliases = new ArrayList<>();
            EvalExec scoreEval = null;
            for (Alias alias : aliases) {
                if (aliasPushed == false && alias.child().isPushable()) {
                    QueryBuilder queryBuilder = esQueryExec.query();
                    if (queryBuilder == null) {
                        queryBuilder = new MatchAllQueryBuilder();
                    }
                    Script script = new Script(alias.child().asScript());
                    ScriptScoreQueryBuilder scriptScore = new ScriptScoreQueryBuilder(queryBuilder, script);
                    MetadataAttribute scoreAttr = new MetadataAttribute(Source.EMPTY, MetadataAttribute.SCORE, DataType.DOUBLE, false);
                    Alias scoreAlias = alias.replaceChild(scoreAttr);
                    EsQueryExec scriptQueryExec = esQueryExec.withQuery(scriptScore);
                    scriptQueryExec.attrs().add(scoreAttr);
                    scoreEval = new EvalExec(evalExec.source(), scriptQueryExec, List.of(scoreAlias));
                    aliasPushed = true;
                } else {
                    nonPushedAliases.add(alias);
                }
            }
            if (aliasPushed) {
                if (nonPushedAliases.isEmpty()) {
                    plan = scoreEval;
                } else {
                    plan = new EvalExec(evalExec.source(), scoreEval, nonPushedAliases);
                }
            }
        }

        return plan;
    }
}
