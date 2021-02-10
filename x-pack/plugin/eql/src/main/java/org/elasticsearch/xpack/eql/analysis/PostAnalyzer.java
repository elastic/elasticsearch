/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Holder;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.tree.Source.synthetic;

/**
 * Post processor of the user query once it got analyzed and verified.
 * The purpose of this class is to add implicit blocks to the query based on the user request
 * that help with the query execution not its semantics.
 *
 * This could have been done in the optimizer however due to its wrapping nature (which is clunky to do with rules)
 * and since the optimized is not parameterized, making this a separate step (similar to the pre-analyzer) is more natural.
 */
public class PostAnalyzer {

    private static final Logger log = LogManager.getLogger(PostAnalyzer.class);

    public LogicalPlan postAnalyze(LogicalPlan plan, EqlConfiguration configuration) {
        LogicalPlan initial = plan;
        if (plan.analyzed()) {
            // implicit limit

            // implicit sequence fetch size

            // implicit project + fetch size (if needed)

            Holder<Boolean> hasJoin = new Holder<>(Boolean.FALSE);

            Source projectCtx = synthetic("<implicit-project>");
            // first per KeyedFilter
            plan = plan.transformUp(KeyedFilter.class, k -> {
                hasJoin.set(Boolean.TRUE);
                Project p = new Project(projectCtx, k.child(), k.extractionAttributes());

                // TODO: this could be incorporated into the query generation
                LogicalPlan fetchSize = new LimitWithOffset(synthetic("<fetch-size>"),
                    new Literal(synthetic("<fetch-value>"), configuration.fetchSize(), DataTypes.INTEGER),
                    p);

                return new KeyedFilter(k.source(), fetchSize, k.keys(), k.timestamp(), k.tiebreaker());
            });

            // in case of event queries, filter everything
            if (hasJoin.get() == false) {
                plan = new Project(projectCtx, plan, emptyList());
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("Applied post-analysys\n{}", NodeUtils.diffString(initial, plan));
        }
        return plan;
    }

}
