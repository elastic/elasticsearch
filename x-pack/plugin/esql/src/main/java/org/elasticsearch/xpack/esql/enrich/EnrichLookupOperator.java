/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ql.expression.NamedExpression;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public final class EnrichLookupOperator extends AsyncOperator {
    private final EnrichLookupService enrichLookupService;
    private final String sessionId;
    private final CancellableTask parentTask;
    private final int inputChannel;
    private final String enrichIndex;
    private final String matchType;
    private final String matchField;
    private final List<NamedExpression> enrichFields;
    private long totalTerms = 0L;

    public record Factory(
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        int inputChannel,
        EnrichLookupService enrichLookupService,
        String enrichIndex,
        String matchType,
        String matchField,
        List<NamedExpression> enrichFields
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
                driverContext,
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
        DriverContext driverContext,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        int inputChannel,
        EnrichLookupService enrichLookupService,
        String enrichIndex,
        String matchType,
        String matchField,
        List<NamedExpression> enrichFields
    ) {
        super(driverContext, maxOutstandingRequests);
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
        totalTerms += inputBlock.getTotalValueCount();
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
    public String toString() {
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
    protected void doClose() {
        // TODO: Maybe create a sub-task as the parent task of all the lookup tasks
        // then cancel it when this operator terminates early (e.g., have enough result).
    }

    @Override
    public Operator.Status status() {
        final PageStats stats = pageStats();
        assert stats.received() >= stats.completed() : stats.received() + " < " + stats.completed();
        return new Status(stats.received(), stats.completed(), totalTerms, TimeValue.timeValueNanos(totalTimeInNanos()).millis());
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "enrich",
            Status::new
        );

        final long receivedPages;
        final long completedPages;
        final long totalTerms;
        final long totalTimeInMillis;

        Status(long receivedPages, long completedPages, long totalTerms, long totalTimeInMillis) {
            this.receivedPages = receivedPages;
            this.completedPages = completedPages;
            this.totalTerms = totalTerms;
            this.totalTimeInMillis = totalTimeInMillis;
        }

        Status(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ENRICH_OPERATOR_STATUS)) {
                this.receivedPages = in.readVLong();
                this.completedPages = in.readVLong();
                this.totalTerms = in.readVLong();
                this.totalTimeInMillis = in.readVLong();
            } else {
                this.receivedPages = -1L;
                this.completedPages = -1L;
                this.totalTerms = -1L;
                this.totalTimeInMillis = -1L;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ENRICH_OPERATOR_STATUS)) {
                out.writeVLong(receivedPages);
                out.writeVLong(completedPages);
                out.writeVLong(totalTerms);
                out.writeVLong(totalTimeInMillis);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("received_pages", receivedPages);
            builder.field("completed_pages", completedPages);
            builder.field("total_terms", totalTerms);
            builder.field("total_time_in_millis", totalTimeInMillis);
            if (totalTimeInMillis >= 0) {
                builder.field("total_time", TimeValue.timeValueMillis(totalTimeInMillis));
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return receivedPages == status.receivedPages
                && completedPages == status.completedPages
                && totalTerms == status.totalTerms
                && totalTimeInMillis == status.totalTimeInMillis;
        }

        @Override
        public int hashCode() {
            return Objects.hash(receivedPages, completedPages, totalTerms, totalTimeInMillis);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X;
        }
    }
}
