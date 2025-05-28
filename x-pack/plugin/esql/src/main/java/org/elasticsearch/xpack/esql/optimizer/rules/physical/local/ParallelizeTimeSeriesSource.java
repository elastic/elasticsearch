/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.ParallelExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesFieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An optimization rule vertically partitions the time-series into three parts: time-series source, field extraction,
 * and time-series aggregation so that they can run parallel to speed up time-series query.
 * For the field-extraction part, it will use a specialized version for time-series indices.
 */
public class ParallelizeTimeSeriesSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesAggregateExec,
    LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(TimeSeriesAggregateExec plan, LocalPhysicalOptimizerContext context) {
        if (plan.getMode().isInputPartial()) {
            return plan;
        }
        if (plan.anyMatch(p -> p instanceof EsQueryExec q && q.indexMode() == IndexMode.TIME_SERIES) == false) {
            return plan;
        }
        final List<FieldExtractExec> pushDownExtracts = new ArrayList<>();
        plan.forEachDown(p -> {
            if (p instanceof FieldExtractExec) {
                pushDownExtracts.add((FieldExtractExec) p);
            } else if (stopPushDownExtract(p)) {
                if (pushDownExtracts.isEmpty() == false) {
                    pushDownExtracts.clear();
                }
            }
        });
        final Holder<Boolean> aborted = new Holder<>(Boolean.FALSE);
        PhysicalPlan newChild = plan.child().transformUp(PhysicalPlan.class, p -> {
            if (aborted.get()) {
                return p;
            }
            if (p instanceof EsQueryExec q && q.indexMode() == IndexMode.TIME_SERIES) {
                return addFieldExtract(context, q, pushDownExtracts);
            }
            if (stopPushDownExtract(p)) {
                aborted.set(Boolean.TRUE);
                return p;
            }
            if (p instanceof FieldExtractExec e) {
                return e.child();
            }
            return p;
        });
        return plan.replaceChild(new ParallelExec(plan.source(), newChild));
    }

    private static boolean stopPushDownExtract(PhysicalPlan p) {
        return p instanceof FilterExec || p instanceof TopNExec || p instanceof LimitExec;
    }

    private PhysicalPlan addFieldExtract(LocalPhysicalOptimizerContext context, EsQueryExec query, List<FieldExtractExec> extracts) {
        Set<Attribute> docValuesAttributes = new HashSet<>();
        Set<Attribute> boundsAttributes = new HashSet<>();
        List<Attribute> attributesToExtract = new ArrayList<>();
        for (FieldExtractExec extract : extracts) {
            docValuesAttributes.addAll(extract.docValuesAttributes());
            boundsAttributes.addAll(extract.boundsAttributes());
            attributesToExtract.addAll(extract.attributesToExtract());
        }
        List<Attribute> attrs = query.attrs();
        var tsSource = new TimeSeriesSourceExec(query.source(), attrs, query.query(), query.limit(), query.estimatedRowSize());
        return new TimeSeriesFieldExtractExec(
            query.source(),
            new ParallelExec(query.source(), tsSource),
            attributesToExtract,
            context.configuration().pragmas().fieldExtractPreference(),
            docValuesAttributes,
            boundsAttributes
        );
    }
}
