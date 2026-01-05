/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Streaming version of LookupFromIndexOperator.
 */
public class StreamingLookupFromIndexOperator extends LookupFromIndexOperator {
    private static final AtomicLong lookupJoinSessionIdGenerator = new AtomicLong(0);

    public StreamingLookupFromIndexOperator(
        List<MatchConfig> matchFields,
        String sessionId,
        DriverContext driverContext,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        LookupFromIndexService lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        Expression joinOnConditions
    ) {
        super(
            matchFields,
            sessionId,
            driverContext,
            parentTask,
            maxOutstandingRequests,
            lookupService,
            lookupIndexPattern,
            lookupIndex,
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions
        );
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<OngoingJoin> listener) {
        MatchFieldsMapping mapping = buildMatchFieldsMapping();
        Block[] inputBlockArray = applyMatchFieldsMapping(inputPage, mapping.channelMapping());

        // Generate subSessionId for streaming lookup
        String streamingSessionId = sessionId + "/lookup/" + lookupJoinSessionIdGenerator.incrementAndGet();

        LookupFromIndexService.Request request = new LookupFromIndexService.Request(
            sessionId,
            lookupIndex,
            lookupIndexPattern,
            mapping.reindexedMatchFields(),
            new Page(inputBlockArray),
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions,
            streamingSessionId
        );
        lookupService.lookupAsync(
            request,
            parentTask,
            listener.map(pages -> new OngoingJoin(new RightChunkedLeftJoin(inputPage, loadFields.size()), pages.iterator()))
        );
    }
}
