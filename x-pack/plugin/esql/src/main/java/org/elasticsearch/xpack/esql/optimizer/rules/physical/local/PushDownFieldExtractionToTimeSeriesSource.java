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
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An optimization rule that pushes down field extractions to occur at the lowest filter, limit, or topN in the time-series source plan.
 * For example:
 * `TS index | WHERE host = 'a' AND cluster = 'b' | STATS max(rate(counter)) BY host, bucket(1minute)`
 * In this query, the extraction of the `host` and `cluster` fields will be pushed down to the time-series source,
 * while the extraction of the `counter` field will occur later. In such cases, the `doc_ids` still need to be returned
 * for the later extraction. However, if the filter (`host = 'a' AND cluster = 'b'`) is pushed down to Lucene, all field extractions
 * (e.g., `host` and `counter`) will be pushed down to the time-series source, and `doc_ids` will not be returned.
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

    private PhysicalPlan addFieldExtract(
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
        var tsSource = new TimeSeriesSourceExec(
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
        // Use a separate driver for the time-series source to split the pipeline to increase parallelism,
        // since the time-series source must be executed with a single driver at the shard level.
        return new ParallelExec(query.source(), tsSource);
    }
}
