/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplingQueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.esql.planner.mapper.MapperUtils.hasScoreAttribute;

public class PushSampleToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<SampleExec, LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(SampleExec sample, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = sample;
        if (sample.child() instanceof EsQueryExec queryExec) {
            var fullQuery = boolQuery();
            if (queryExec.query() != null) {
                if (hasScoreAttribute(queryExec.output())) {
                    fullQuery.must(queryExec.query());
                } else {
                    fullQuery.filter(queryExec.query());
                }
            }

            var sampleQuery = new RandomSamplingQueryBuilder((double) Foldables.valueOf(ctx.foldCtx(), sample.probability()));

            fullQuery.filter(sampleQuery);

            plan = queryExec.withQuery(fullQuery);
        }
        return plan;
    }
}
