/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;

import java.util.List;

/**
 * Class for sharing code across Mappers.
 */
class MapperUtils {
    private MapperUtils() {}

    static PhysicalPlan mapLeaf(LeafPlan p) {
        if (p instanceof LocalRelation local) {
            return new LocalSourceExec(local.source(), local.output(), local.supplier());
        }

        // Commands
        if (p instanceof ShowInfo showInfo) {
            return new ShowExec(showInfo.source(), showInfo.output(), showInfo.values());
        }

        return unsupported(p);
    }

    static PhysicalPlan mapUnary(UnaryPlan p, PhysicalPlan child) {
        if (p instanceof Filter f) {
            return new FilterExec(f.source(), child, f.condition());
        }

        if (p instanceof Project pj) {
            return new ProjectExec(pj.source(), child, pj.projections());
        }

        if (p instanceof Eval eval) {
            return new EvalExec(eval.source(), child, eval.fields());
        }

        if (p instanceof Dissect dissect) {
            return new DissectExec(dissect.source(), child, dissect.input(), dissect.parser(), dissect.extractedFields());
        }

        if (p instanceof Grok grok) {
            return new GrokExec(grok.source(), child, grok.input(), grok.parser(), grok.extractedFields());
        }

        if (p instanceof Enrich enrich) {
            return new EnrichExec(
                enrich.source(),
                child,
                enrich.mode(),
                enrich.policy().getType(),
                enrich.matchField(),
                BytesRefs.toString(enrich.policyName().fold(FoldContext.small() /* TODO remove me */)),
                enrich.policy().getMatchField(),
                enrich.concreteIndices(),
                enrich.enrichFields()
            );
        }

        if (p instanceof MvExpand mvExpand) {
            MvExpandExec result = new MvExpandExec(mvExpand.source(), child, mvExpand.target(), mvExpand.expanded());
            if (mvExpand.limit() != null) {
                // MvExpand could have an inner limit
                // see PushDownAndCombineLimits rule
                return new LimitExec(result.source(), result, new Literal(Source.EMPTY, mvExpand.limit(), DataType.INTEGER));
            }
            return result;
        }

        return unsupported(p);
    }

    static List<Attribute> intermediateAttributes(Aggregate aggregate) {
        List<Attribute> intermediateAttributes = AbstractPhysicalOperationProviders.intermediateAttributes(
            aggregate.aggregates(),
            aggregate.groupings()
        );
        return intermediateAttributes;
    }

    static AggregateExec aggExec(Aggregate aggregate, PhysicalPlan child, AggregatorMode aggMode, List<Attribute> intermediateAttributes) {
        return new AggregateExec(
            aggregate.source(),
            child,
            aggregate.groupings(),
            aggregate.aggregates(),
            aggMode,
            intermediateAttributes,
            null
        );
    }

    static PhysicalPlan unsupported(LogicalPlan p) {
        throw new EsqlIllegalArgumentException("unsupported logical plan node [" + p.nodeName() + "]");
    }
}
