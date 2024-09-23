/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.QueryPlan;

import java.util.List;

/**
 * A PhysicalPlan is "how" a LogicalPlan (the "what") actually gets translated into one or more queries.
 *
 * LogicalPlan = I want to get from DEN to SFO
 * PhysicalPlan = take Delta, DEN to SJC, then SJC to SFO
 */
public abstract class PhysicalPlan extends QueryPlan<PhysicalPlan> {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            AggregateExec.ENTRY,
            DissectExec.ENTRY,
            EnrichExec.ENTRY,
            EsQueryExec.ENTRY,
            EsSourceExec.ENTRY,
            EvalExec.ENTRY,
            ExchangeExec.ENTRY,
            ExchangeSinkExec.ENTRY,
            ExchangeSourceExec.ENTRY,
            FieldExtractExec.ENTRY,
            FilterExec.ENTRY,
            FragmentExec.ENTRY,
            GrokExec.ENTRY,
            HashJoinExec.ENTRY,
            LimitExec.ENTRY,
            LocalSourceExec.ENTRY,
            MvExpandExec.ENTRY,
            OrderExec.ENTRY,
            ProjectExec.ENTRY,
            RowExec.ENTRY,
            ShowExec.ENTRY,
            TopNExec.ENTRY
        );
    }

    public PhysicalPlan(Source source, List<PhysicalPlan> children) {
        super(source, children);
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

}
