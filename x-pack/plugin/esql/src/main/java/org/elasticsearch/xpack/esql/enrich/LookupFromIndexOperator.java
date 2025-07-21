/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

// TODO rename package
public final class LookupFromIndexOperator extends AsyncOperator<LookupFromIndexOperator.OngoingJoin> {
    public record MatchConfig(FieldAttribute.FieldName fieldName, int channel, DataType type) implements Writeable {
        public MatchConfig(FieldAttribute match, Layout.ChannelAndType input) {
            // TODO: Using exactAttribute was supposed to handle TEXT fields with KEYWORD subfields - but we don't allow these in lookup
            // indices, so the call to exactAttribute looks redundant now.
            this(match.exactAttribute().fieldName(), input.channel(), input.type());
        }

        public MatchConfig(StreamInput in) throws IOException {
            this(new FieldAttribute.FieldName(in.readString()), in.readInt(), DataType.readFrom(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName.string());
            out.writeInt(channel);
            type.writeTo(out);
        }

    }

    public record Factory(
        List<MatchConfig> matchFields,
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        Function<DriverContext, LookupFromIndexService> lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source
    ) implements OperatorFactory {
        @Override
        public String describe() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("LookupOperator[index=").append(lookupIndex).append(" load_fields=").append(loadFields);
            for (MatchConfig matchField : matchFields) {
                stringBuilder.append(" input_type=")
                    .append(matchField.type)
                    .append(" match_field=")
                    .append(matchField.fieldName.string())
                    .append(" inputChannel=")
                    .append(matchField.channel);
            }
            stringBuilder.append("]");
            return stringBuilder.toString();
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new LookupFromIndexOperator(
                matchFields,
                sessionId,
                driverContext,
                parentTask,
                maxOutstandingRequests,
                lookupService.apply(driverContext),
                lookupIndexPattern,
                lookupIndex,
                loadFields,
                source
            );
        }
    }

    private final LookupFromIndexService lookupService;
    private final String sessionId;
    private final CancellableTask parentTask;
    private final String lookupIndexPattern;
    private final String lookupIndex;
    private final List<NamedExpression> loadFields;
    private final Source source;
    private long totalRows = 0L;
    private List<MatchConfig> matchFields;
    /**
     * Total number of pages emitted by this {@link Operator}.
     */
    private long emittedPages = 0L;
    /**
     * The ongoing join or {@code null} none is ongoing at the moment.
     */
    private OngoingJoin ongoing = null;

    public LookupFromIndexOperator(
        List<MatchConfig> matchFields,
        String sessionId,
        DriverContext driverContext,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        LookupFromIndexService lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source
    ) {
        super(driverContext, lookupService.getThreadContext(), maxOutstandingRequests);
        this.matchFields = matchFields;
        this.sessionId = sessionId;
        this.parentTask = parentTask;
        this.lookupService = lookupService;
        this.lookupIndexPattern = lookupIndexPattern;
        this.lookupIndex = lookupIndex;
        this.loadFields = loadFields;
        this.source = source;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<OngoingJoin> listener) {
        Block[] inputBlockArray = new Block[matchFields.size()];
        for (int i = 0; i < matchFields.size(); i++) {
            MatchConfig matchField = matchFields.get(i);
            int inputChannel = matchField.channel;
            final Block inputBlock = inputPage.getBlock(inputChannel);
            if (i == 0) {
                // we only add to the totalRows once, so we can use the first block
                totalRows += inputBlock.getTotalValueCount();
            }
            inputBlockArray[i] = inputBlock;
        }
        LookupFromIndexService.Request request = new LookupFromIndexService.Request(
            sessionId,
            lookupIndex,
            lookupIndexPattern,
            matchFields,
            new Page(inputBlockArray),
            loadFields,
            source
        );
        lookupService.lookupAsync(
            request,
            parentTask,
            listener.map(pages -> new OngoingJoin(new RightChunkedLeftJoin(inputPage, loadFields.size()), pages.iterator()))
        );
    }

    @Override
    public Page getOutput() {
        if (ongoing == null) {
            // No ongoing join, start a new one if we can.
            ongoing = fetchFromBuffer();
            if (ongoing == null) {
                // Buffer empty, wait for the next time we're called.
                return null;
            }
        }
        if (ongoing.itr.hasNext()) {
            // There's more to do in the ongoing join.
            Page right = ongoing.itr.next();
            emittedPages++;
            try {
                return ongoing.join.join(right);
            } finally {
                right.releaseBlocks();
            }
        }
        // Current join is all done. Emit any trailing unmatched rows.
        Optional<Page> remaining = ongoing.join.noMoreRightHandPages();
        ongoing.close();
        ongoing = null;
        if (remaining.isEmpty()) {
            return null;
        }
        emittedPages++;
        return remaining.get();
    }

    @Override
    protected void releaseFetchedOnAnyThread(OngoingJoin ongoingJoin) {
        ongoingJoin.releaseOnAnyThread();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("LookupOperator[index=").append(lookupIndex).append(" load_fields=").append(loadFields);
        for (MatchConfig matchField : matchFields) {
            stringBuilder.append(" input_type=")
                .append(matchField.type)
                .append(" match_field=")
                .append(matchField.fieldName.string())
                .append(" inputChannel=")
                .append(matchField.channel);
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    @Override
    public boolean isFinished() {
        return ongoing == null && super.isFinished();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (ongoing != null) {
            return NOT_BLOCKED;
        }
        return super.isBlocked();
    }

    @Override
    protected void doClose() {
        // TODO: Maybe create a sub-task as the parent task of all the lookup tasks
        // then cancel it when this operator terminates early (e.g., have enough result).
        Releasables.close(ongoing);
    }

    @Override
    protected Operator.Status status(long receivedPages, long completedPages, long processNanos) {
        return new LookupFromIndexOperator.Status(receivedPages, completedPages, processNanos, totalRows, emittedPages);
    }

    public static class Status extends AsyncOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "lookup",
            Status::new
        );

        private final long totalTerms;
        /**
         * Total number of pages emitted by this {@link Operator}.
         */
        private final long emittedPages;

        Status(long receivedPages, long completedPages, long totalTimeInMillis, long totalTerms, long emittedPages) {
            super(receivedPages, completedPages, totalTimeInMillis);
            this.totalTerms = totalTerms;
            this.emittedPages = emittedPages;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            this.totalTerms = in.readVLong();
            this.emittedPages = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(totalTerms);
            out.writeVLong(emittedPages);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long emittedPages() {
            return emittedPages;
        }

        public long totalTerms() {
            return totalTerms;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            super.innerToXContent(builder);
            builder.field("emitted_pages", emittedPages());
            builder.field("total_terms", totalTerms());
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
            return totalTerms == status.totalTerms && emittedPages == status.emittedPages;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), totalTerms, emittedPages);
        }
    }

    protected record OngoingJoin(RightChunkedLeftJoin join, Iterator<Page> itr) implements Releasable {
        @Override
        public void close() {
            Releasables.close(join, Releasables.wrap(() -> Iterators.map(itr, page -> page::releaseBlocks)));
        }

        public void releaseOnAnyThread() {
            Releasables.close(
                join::releaseOnAnyThread,
                Releasables.wrap(() -> Iterators.map(itr, page -> () -> releasePageOnAnyThread(page)))
            );
        }
    }
}
