/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

// TODO rename package
public final class LookupFromIndexOperator extends AsyncOperator {
    public record Factory(
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        int inputChannel,
        LookupFromIndexService lookupService,
        DataType inputDataType,
        String lookupIndex,
        String matchField,
        List<NamedExpression> loadFields,
        Source source
    ) implements OperatorFactory {
        @Override
        public String describe() {
            return "LookupOperator[index="
                + lookupIndex
                + " match_field="
                + matchField
                + " load_fields="
                + loadFields
                + " inputChannel="
                + inputChannel
                + "]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new LookupFromIndexOperator(
                sessionId,
                driverContext,
                parentTask,
                maxOutstandingRequests,
                inputChannel,
                lookupService,
                inputDataType,
                lookupIndex,
                matchField,
                loadFields,
                source
            );
        }
    }

    private final LookupFromIndexService lookupService;
    private final String sessionId;
    private final CancellableTask parentTask;
    private final int inputChannel;
    private final DataType inputDataType;
    private final String lookupIndex;
    private final String matchField;
    private final List<NamedExpression> loadFields;
    private final Source source;
    private long totalTerms = 0L;

    public LookupFromIndexOperator(
        String sessionId,
        DriverContext driverContext,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        int inputChannel,
        LookupFromIndexService lookupService,
        DataType inputDataType,
        String lookupIndex,
        String matchField,
        List<NamedExpression> loadFields,
        Source source
    ) {
        super(driverContext, maxOutstandingRequests);
        this.sessionId = sessionId;
        this.parentTask = parentTask;
        this.inputChannel = inputChannel;
        this.lookupService = lookupService;
        this.inputDataType = inputDataType;
        this.lookupIndex = lookupIndex;
        this.matchField = matchField;
        this.loadFields = loadFields;
        this.source = source;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        final Block inputBlock = inputPage.getBlock(inputChannel);
        totalTerms += inputBlock.getTotalValueCount();
        LookupFromIndexService.Request request = new LookupFromIndexService.Request(
            sessionId,
            lookupIndex,
            inputDataType,
            matchField,
            new Page(inputBlock),
            loadFields,
            source
        );
        lookupService.lookupAsync(request, parentTask, listener.map(inputPage::appendPage));
    }

    @Override
    public String toString() {
        return "LookupOperator[index="
            + lookupIndex
            + " input_type="
            + inputDataType
            + " match_field="
            + matchField
            + " load_fields="
            + loadFields
            + " inputChannel="
            + inputChannel
            + "]";
    }

    @Override
    protected void doClose() {
        // TODO: Maybe create a sub-task as the parent task of all the lookup tasks
        // then cancel it when this operator terminates early (e.g., have enough result).
    }

    @Override
    protected Operator.Status status(long receivedPages, long completedPages, long totalTimeInMillis) {
        return new LookupFromIndexOperator.Status(receivedPages, completedPages, totalTimeInMillis, totalTerms);
    }

    public static class Status extends AsyncOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "lookup",
            Status::new
        );

        final long totalTerms;

        Status(long receivedPages, long completedPages, long totalTimeInMillis, long totalTerms) {
            super(receivedPages, completedPages, totalTimeInMillis);
            this.totalTerms = totalTerms;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            this.totalTerms = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(totalTerms);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerToXContent(builder);
            builder.field("total_terms", totalTerms);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass() || super.equals(o) == false) {
                return false;
            }
            Status status = (Status) o;
            return totalTerms == status.totalTerms;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), totalTerms);
        }
    }
}
