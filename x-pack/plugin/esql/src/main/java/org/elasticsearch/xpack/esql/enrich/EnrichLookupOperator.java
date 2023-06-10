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
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;

public final class EnrichLookupOperator extends AsyncOperator {
    private final EnrichLookupService enrichLookupService;
    private final String sessionId;
    private final CancellableTask parentTask;
    private final int inputChannel;
    private final String enrichIndex;
    private final String matchType;
    private final String matchField;
    private final List<Attribute> enrichFields;

    public record Factory(
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        int inputChannel,
        EnrichLookupService enrichLookupService,
        String enrichIndex,
        String matchType,
        String matchField,
        List<Attribute> enrichFields
    ) implements OperatorFactory {
        @Override
        public String describe() {
            return "EnrichOperator[index="
                + enrichIndex
                + " match_field="
                + matchField
                + " enrich_fields="
                + enrichFields
                + " inputChannel="
                + inputChannel
                + "]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new EnrichLookupOperator(
                sessionId,
                parentTask,
                maxOutstandingRequests,
                inputChannel,
                enrichLookupService,
                enrichIndex,
                matchType,
                matchField,
                enrichFields
            );
        }
    }

    public EnrichLookupOperator(
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        int inputChannel,
        EnrichLookupService enrichLookupService,
        String enrichIndex,
        String matchType,
        String matchField,
        List<Attribute> enrichFields
    ) {
        super(maxOutstandingRequests);
        this.sessionId = sessionId;
        this.parentTask = parentTask;
        this.inputChannel = inputChannel;
        this.enrichLookupService = enrichLookupService;
        this.enrichIndex = enrichIndex;
        this.matchType = matchType;
        this.matchField = matchField;
        this.enrichFields = enrichFields;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        final Block inputBlock = inputPage.getBlock(inputChannel);
        enrichLookupService.lookupAsync(
            sessionId,
            parentTask,
            enrichIndex,
            matchType,
            matchField,
            enrichFields,
            new Page(inputBlock),
            listener.map(inputPage::appendPage)
        );
    }

    @Override
    public void close() {
        // TODO: Maybe create a sub-task as the parent task of all the lookup tasks
        // then cancel it when this operator terminates early (e.g., have enough result).
    }
}
