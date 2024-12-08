/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.SubqueryExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.List;

public class PlanWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(logical());
        entries.addAll(phsyical());
        return entries;
    }

    public static List<NamedWriteableRegistry.Entry> logical() {
        return List.of(
            Aggregate.ENTRY,
            Dissect.ENTRY,
            Enrich.ENTRY,
            EsRelation.ENTRY,
            EsqlProject.ENTRY,
            Eval.ENTRY,
            Filter.ENTRY,
            Grok.ENTRY,
            InlineStats.ENTRY,
            InlineJoin.ENTRY,
            Join.ENTRY,
            LocalRelation.ENTRY,
            Limit.ENTRY,
            Lookup.ENTRY,
            MvExpand.ENTRY,
            OrderBy.ENTRY,
            Project.ENTRY,
            TopN.ENTRY
        );
    }

    public static List<NamedWriteableRegistry.Entry> phsyical() {
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
            ShowExec.ENTRY,
            SubqueryExec.ENTRY,
            TopNExec.ENTRY
        );
    }
}
