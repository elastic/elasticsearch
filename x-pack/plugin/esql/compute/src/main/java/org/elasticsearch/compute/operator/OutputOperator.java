/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Sink operator that calls a given listener for each page received. The listener receives both the page as well as schema information,
 * i.e. the names of the rows that are outputted.
 */
public class OutputOperator extends SinkOperator {

    /**
     * A page consumer with a blocked state of its own, which the operator forwards: a consumer that writes pages out
     * tells the driver, and through the exchange the data nodes, to wait rather than hand it more.
     */
    public interface BlockingPageConsumer extends Consumer<Page> {
        IsBlockedResult isBlocked();

        /** Rows the consumer wrote out rather than kept, for the operator's status. */
        default long rowsSpilled() {
            return 0;
        }

        /** The consumer's spill counters, for the operator's status. */
        default SpillStatus spillStatus() {
            return SpillStatus.NONE;
        }
    }

    /**
     * The sink's status, given for a consumer that may write pages out: the pages and rows received, and what the
     * consumer wrote out, counted as every spilling operator counts.
     */
    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "output",
            Status::new
        );
        public static final ProfileSchema<Status> PROFILE_SCHEMA = SpillStatus.addTo(
            ProfileSchema.of(Status.class, (builder, status, params) -> status.writeProfile(builder))
                .required("pages_received", ProfileSchema.integer())
                .required("rows_received", ProfileSchema.integer())
                .optional("rows_spilled", ProfileSchema.integer())
        ).build();

        private final int pagesReceived;
        private final long rowsReceived;
        private final long rowsSpilled;
        private final SpillStatus spill;

        /**
         * @param rowsSpilled rows the consumer wrote to its run rather than kept in memory
         * @param spill the consumer's spill counters
         */
        public Status(int pagesReceived, long rowsReceived, long rowsSpilled, SpillStatus spill) {
            this.pagesReceived = pagesReceived;
            this.rowsReceived = rowsReceived;
            this.rowsSpilled = rowsSpilled;
            this.spill = spill;
        }

        Status(StreamInput in) throws IOException {
            pagesReceived = in.readVInt();
            rowsReceived = in.readVLong();
            rowsSpilled = in.readVLong();
            spill = SpillStatus.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesReceived);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsSpilled);
            spill.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesReceived() {
            return pagesReceived;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsSpilled() {
            return rowsSpilled;
        }

        public SpillStatus spill() {
            return spill;
        }

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("pages_received", pagesReceived);
            builder.field("rows_received", rowsReceived);
            // present by value: a consumer that kept everything in memory writes none of these
            if (rowsSpilled > 0 || spill.isNone() == false) {
                builder.field("rows_spilled", rowsSpilled);
                spill.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            return builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesReceived == status.pagesReceived
                && rowsReceived == status.rowsReceived
                && rowsSpilled == status.rowsSpilled
                && spill.equals(status.spill);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesReceived, rowsReceived, rowsSpilled, spill);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        /**
         * The status exists since the spill feature's version: a node before it gets none, as the sink had none.
         */
        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return SpillStatus.ESQL_SPILL_STATUS;
        }
    }

    /**
     * Interface for DriverContext implementations that provide a collectedPages list.
     * This allows OutputOperatorFactory to retrieve collectedPages without creating a circular dependency.
     */
    public interface CollectedPagesProvider {
        /**
         * Returns the collectedPages list that should be used by OutputOperator.
         * @return the collectedPages list
         */
        List<Page> collectedPages();
    }

    private final List<String> columns;
    private final Consumer<Page> pageConsumer;
    private final Function<Page, Page> mapper;
    private int pagesReceived;
    private long rowsReceived;

    public record OutputOperatorFactory(List<String> columns, Function<Page, Page> mapper, Consumer<Page> pageConsumer)
        implements
            SinkOperatorFactory {

        @Override
        public SinkOperator get(DriverContext driverContext) {
            // If this DriverContext provides collectedPages, use them
            if (driverContext instanceof CollectedPagesProvider collectedPagesProvider) {
                List<Page> collectedPages = collectedPagesProvider.collectedPages();
                return new OutputOperator(columns, mapper, collectedPages::add);
            } else {
                // Normal execution path: use the provided pageConsumer
                return new OutputOperator(columns, mapper, pageConsumer);
            }
        }

        @Override
        public String describe() {
            return OutputOperator.describe(columns);
        }
    }

    public OutputOperator(List<String> columns, Function<Page, Page> mapper, Consumer<Page> pageConsumer) {
        this.columns = columns;
        this.mapper = mapper;
        this.pageConsumer = pageConsumer;
    }

    public List<String> getColumns() {
        return columns;
    }

    boolean finished = false;

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return finished == false && isBlocked().listener().isDone();
    }

    @Override
    public IsBlockedResult isBlocked() {
        return pageConsumer instanceof BlockingPageConsumer blocking ? blocking.isBlocked() : NOT_BLOCKED;
    }

    @Override
    protected void doAddInput(Page page) {
        pagesReceived++;
        rowsReceived += page.getPositionCount();
        pageConsumer.accept(mapper.apply(page));
    }

    /**
     * A status for a consumer that may write pages out, the result collector; the plain list consumer reports none,
     * as before, so a profile changes only where spilling may happen.
     */
    @Override
    public Status status() {
        if (pageConsumer instanceof BlockingPageConsumer blocking) {
            return new Status(pagesReceived, rowsReceived, blocking.rowsSpilled(), blocking.spillStatus());
        }
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return describe(columns);
    }

    private static String describe(List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("OutputOperator").append("[");
        sb.append("columns = ");
        if (columns.size() <= 10) {
            sb.append(columns);
        } else {
            sb.append('[').append(columns.size()).append(" columns").append(']');
        }
        sb.append("]");
        return sb.toString();
    }
}
