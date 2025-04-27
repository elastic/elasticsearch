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
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A rule that pushes down field extractions to occur before filter/limit/topN in the time-series source plan.
 */
public class PushDownFieldExtractionToTimeSeriesSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        if (plan.anyMatch(p -> p instanceof EsQueryExec q && q.indexMode() == IndexMode.TIME_SERIES) == false) {
            return plan;
        }
        final List<FieldExtractExec> pushDownExtracts = new ArrayList<>();
        final Holder<Boolean> keepDocIds = new Holder<>(Boolean.FALSE);
        plan.forEachDown(p -> {
            if (p instanceof FieldExtractExec) {
                pushDownExtracts.add((FieldExtractExec) p);
            } else if (stopPushDownExtract(p)) {
                if (pushDownExtracts.isEmpty() == false) {
                    keepDocIds.set(Boolean.TRUE);
                    pushDownExtracts.clear();
                }
            }
        });
        final Holder<Boolean> aborted = new Holder<>(Boolean.FALSE);
        return plan.transformUp(PhysicalPlan.class, p -> {
            if (aborted.get()) {
                return p;
            }
            if (p instanceof EsQueryExec q && q.indexMode() == IndexMode.TIME_SERIES) {
                return addFieldExtract(context, q, keepDocIds.get(), pushDownExtracts);
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
    }

    private static boolean stopPushDownExtract(PhysicalPlan p) {
        return p instanceof FilterExec || p instanceof TopNExec || p instanceof LimitExec;
    }

    private TimeSeriesSourceExec addFieldExtract(
        LocalPhysicalOptimizerContext context,
        EsQueryExec query,
        boolean keepDocAttribute,
        List<FieldExtractExec> extracts
    ) {
        Set<Attribute> docValuesAttributes = new HashSet<>();
        Set<Attribute> boundsAttributes = new HashSet<>();
        List<Attribute> attributesToExtract = new ArrayList<>();
        for (FieldExtractExec extract : extracts) {
            docValuesAttributes.addAll(extract.docValuesAttributes());
            boundsAttributes.addAll(extract.boundsAttributes());
            attributesToExtract.addAll(extract.attributesToExtract());
        }
        List<Attribute> attrs = query.attrs();
        if (keepDocAttribute == false) {
            attrs = attrs.stream().filter(a -> EsQueryExec.isSourceAttribute(a) == false).toList();
        }
        return new TimeSeriesSourceExec(
            query.source(),
            attrs,
            query.query(),
            query.limit(),
            context.configuration().pragmas().fieldExtractPreference(),
            docValuesAttributes,
            boundsAttributes,
            attributesToExtract,
            query.estimatedRowSize()
        );
    }
}
