/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

// TODO rename package
public class LookupFromIndexOperator extends AsyncOperator<LookupFromIndexOperator.OngoingJoin> {

    public record Factory(
        List<MatchConfig> matchFields,
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        Function<DriverContext, LookupFromIndexService> lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        Expression joinOnConditions,
        boolean useStreamingOperator,
        int exchangeBufferSize,
        boolean profile
    ) implements OperatorFactory {

        private String operatorName() {
            return useStreamingOperator ? "StreamingLookupOperator" : "LookupOperator";
        }

        @Override
        public String describe() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(operatorName()).append("[index=").append(lookupIndex).append(" load_fields=").append(loadFields);
            for (MatchConfig matchField : matchFields) {
                stringBuilder.append(" input_type=")
                    .append(matchField.type())
                    .append(" match_field=")
                    .append(matchField.fieldName())
                    .append(" inputChannel=")
                    .append(matchField.channel());
            }
            stringBuilder.append(" right_pre_join_plan=").append(rightPreJoinPlan == null ? "null" : rightPreJoinPlan.toString());
            stringBuilder.append(" join_on_expression=").append(joinOnConditions == null ? "null" : joinOnConditions.toString());
            stringBuilder.append("]");
            return stringBuilder.toString();
        }

        @Override
        public Operator get(DriverContext driverContext) {
            if (useStreamingOperator) {
                return new StreamingLookupFromIndexOperator(
                    matchFields,
                    sessionId,
                    parentTask,
                    maxOutstandingRequests,
                    lookupService.apply(driverContext),
                    lookupIndexPattern,
                    lookupIndex,
                    loadFields,
                    source,
                    rightPreJoinPlan,
                    joinOnConditions,
                    exchangeBufferSize,
                    profile
                );
            } else {
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
                    source,
                    rightPreJoinPlan,
                    joinOnConditions
                );
            }
        }
    }

    protected final LookupFromIndexService lookupService;
    protected final String sessionId;
    protected final CancellableTask parentTask;
    protected final String lookupIndexPattern;
    protected final String lookupIndex;
    protected final List<NamedExpression> loadFields;
    protected final Source source;
    protected long totalRows = 0L;
    protected final List<MatchConfig> matchFields;
    protected final PhysicalPlan rightPreJoinPlan;
    protected final Expression joinOnConditions;
    // MatchFieldsMapping is the same for all batches (based on operator configuration, not input pages)
    protected final MatchFieldsMapping matchFieldsMapping;
    /**
     * Total number of pages emitted by this {@link Operator}.
     */
    protected long emittedPages = 0L;
    /**
     * Total number of rows emitted by this {@link Operator}.
     */
    protected long emittedRows = 0L;
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
        Source source,
        PhysicalPlan rightPreJoinPlan,
        Expression joinOnConditions
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
        this.rightPreJoinPlan = rightPreJoinPlan;
        this.joinOnConditions = joinOnConditions;
        this.matchFieldsMapping = buildMatchFieldsMapping(matchFields, joinOnConditions);
    }

    /**
     * Build MatchFieldsMapping from matchFields and joinOnConditions.
     * This is a static method that can be called from constructors without "this-escape" warnings.
     */
    protected static MatchFieldsMapping buildMatchFieldsMapping(List<MatchConfig> matchFields, Expression joinOnConditions) {
        List<MatchConfig> newMatchFields = new ArrayList<>();
        List<MatchConfig> uniqueMatchFields = uniqueMatchFieldsByName(matchFields, joinOnConditions);
        Map<Integer, Integer> channelMapping = new HashMap<>();
        for (int i = 0; i < uniqueMatchFields.size(); i++) {
            MatchConfig matchField = uniqueMatchFields.get(i);
            int inputChannel = matchField.channel();
            // the matchFields we have are indexed by the input channel on the left side of the join
            // create a new MatchConfig that uses the field name and type from the matchField
            // but the new channel index in the inputBlockArray
            newMatchFields.add(new MatchConfig(matchField.fieldName(), i, matchField.type()));
            // Map new channel index (i) to original input page channel offset (inputChannel)
            channelMapping.put(i, inputChannel);
        }
        return new MatchFieldsMapping(newMatchFields, channelMapping);
    }

    protected Block[] applyMatchFieldsMapping(Page inputPage, Map<Integer, Integer> channelMapping) {
        Block[] inputBlockArray = new Block[channelMapping.size()];
        for (Map.Entry<Integer, Integer> entry : channelMapping.entrySet()) {
            int newIndex = entry.getKey();
            int originalChannel = entry.getValue();
            inputBlockArray[newIndex] = inputPage.getBlock(originalChannel);
        }
        // we only add to the totalRows once, so we can use the first block
        totalRows += inputPage.getBlock(0).getTotalValueCount();
        return inputBlockArray;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<OngoingJoin> listener) {
        Block[] inputBlockArray = applyMatchFieldsMapping(inputPage, matchFieldsMapping.channelMapping());

        LookupFromIndexService.Request request = new LookupFromIndexService.Request(
            sessionId,
            lookupIndex,
            lookupIndexPattern,
            matchFieldsMapping.reindexedMatchFields(),
            new Page(inputBlockArray),
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions,
            null, // clientToServerId - set only by StreamingLookupFromIndexOperator
            null, // serverToClientId - set only by StreamingLookupFromIndexOperator
            false // profile - non-streaming lookup doesn't support plan output
        );
        lookupService.lookupAsync(request, parentTask, listener.map(response -> {
            List<Page> pages = response.takePages();
            return new OngoingJoin(new RightChunkedLeftJoin(inputPage, loadFields.size()), pages.iterator());
        }));
    }

    /**
     * Get unique match fields by name, filtering duplicates if joinOnConditions is present.
     * This is a static method that can be called from constructors without "this-escape" warnings.
     */
    protected static List<MatchConfig> uniqueMatchFieldsByName(List<MatchConfig> matchFields, Expression joinOnConditions) {
        if (joinOnConditions == null) {
            return matchFields;
        }
        List<MatchConfig> uniqueFields = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();
        for (MatchConfig matchField : matchFields) {
            if (seenFieldNames.add(matchField.fieldName())) {
                uniqueFields.add(matchField);
            }
        }
        return uniqueFields;
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
                Page joinedPage = ongoing.join.join(right);
                emittedRows += joinedPage.getPositionCount();
                return joinedPage;
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
        emittedRows += remaining.get().getPositionCount();
        return remaining.get();
    }

    @Override
    protected void releaseFetchedOnAnyThread(OngoingJoin ongoingJoin) {
        ongoingJoin.releaseOnAnyThread();
    }

    /**
     * Returns the name of this operator for use in toString() and describe().
     * Subclasses can override this to provide a different name.
     */
    protected String getOperatorName() {
        return "LookupOperator";
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getOperatorName()).append("[index=").append(lookupIndex).append(" load_fields=").append(loadFields);
        for (MatchConfig matchField : matchFields) {
            stringBuilder.append(" input_type=")
                .append(matchField.type())
                .append(" match_field=")
                .append(matchField.fieldName())
                .append(" inputChannel=")
                .append(matchField.channel());
        }

        stringBuilder.append(" right_pre_join_plan=").append(rightPreJoinPlan == null ? "null" : rightPreJoinPlan.toString());
        stringBuilder.append(" join_on_expression=").append(joinOnConditions == null ? "null" : joinOnConditions.toString());
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
        return new LookupFromIndexOperator.Status(receivedPages, completedPages, processNanos, totalRows, emittedPages, emittedRows);
    }

    public static class Status extends AsyncOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "lookup",
            Status::new
        );

        private static final TransportVersion ESQL_LOOKUP_OPERATOR_EMITTED_ROWS = TransportVersion.fromName(
            "esql_lookup_operator_emitted_rows"
        );

        private final long totalRows;
        /**
         * Total number of pages emitted by this {@link Operator}.
         */
        private final long emittedPages;
        /**
         * Total number of rows emitted by this {@link Operator}.
         */
        private final long emittedRows;

        Status(long receivedPages, long completedPages, long processNanos, long totalRows, long emittedPages, long emittedRows) {
            super(receivedPages, completedPages, processNanos);
            this.totalRows = totalRows;
            this.emittedPages = emittedPages;
            this.emittedRows = emittedRows;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            this.totalRows = in.readVLong();
            this.emittedPages = in.readVLong();
            if (in.getTransportVersion().supports(ESQL_LOOKUP_OPERATOR_EMITTED_ROWS)) {
                this.emittedRows = in.readVLong();
            } else {
                this.emittedRows = 0L;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(totalRows);
            out.writeVLong(emittedPages);
            if (out.getTransportVersion().supports(ESQL_LOOKUP_OPERATOR_EMITTED_ROWS)) {
                out.writeVLong(emittedRows);
            }

        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long emittedPages() {
            return emittedPages;
        }

        public long emittedRows() {
            return emittedPages;
        }

        public long totalRows() {
            return totalRows;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            super.innerToXContent(builder);
            builder.field("pages_emitted", emittedPages);
            builder.field("rows_emitted", emittedRows);
            builder.field("total_rows", totalRows);
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
            return totalRows == status.totalRows && emittedPages == status.emittedPages && emittedRows == status.emittedRows;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), totalRows, emittedPages, emittedRows);
        }
    }

    /**
     * Result of building match fields mapping - contains reindexed match fields and channel mapping.
     */
    protected record MatchFieldsMapping(List<MatchConfig> reindexedMatchFields, Map<Integer, Integer> channelMapping) {}

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
