/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class LimitOperator implements Operator {
    /**
     * Total number of position that are emitted by this operator.
     */
    private final int limit;

    /**
     * Remaining number of positions that will be emitted by this operator.
     */
    private int limitRemaining;

    /**
     * Count of pages that have been processed by this operator.
     */
    private int pagesProcessed;

    private Page lastInput;

    private boolean finished;

    public LimitOperator(int limit) {
        this.limit = this.limitRemaining = limit;
    }

    public record Factory(int limit) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new LimitOperator(limit);
        }

        @Override
        public String describe() {
            return "LimitOperator[limit = " + limit + "]";
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        lastInput = page;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && lastInput == null;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }

        Page result;
        if (lastInput.getPositionCount() <= limitRemaining) {
            result = lastInput;
            limitRemaining -= lastInput.getPositionCount();
        } else {
            int[] filter = new int[limitRemaining];
            for (int i = 0; i < limitRemaining; i++) {
                filter[i] = i;
            }
            Block[] blocks = new Block[lastInput.getBlockCount()];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = lastInput.getBlock(b).filter(filter);
            }
            result = new Page(blocks);
            limitRemaining = 0;
        }
        if (limitRemaining == 0) {
            finished = true;
        }
        lastInput = null;
        pagesProcessed++;

        return result;
    }

    @Override
    public Status status() {
        return new Status(limit, limitRemaining, pagesProcessed);
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "LimitOperator[limit = " + limitRemaining + "/" + limit + "]";
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "limit",
            Status::new
        );

        /**
         * Total number of position that are emitted by this operator.
         */
        private final int limit;

        /**
         * Remaining number of positions that will be emitted by this operator.
         */
        private final int limitRemaining;

        /**
         * Count of pages that have been processed by this operator.
         */
        private final int pagesProcessed;

        protected Status(int limit, int limitRemaining, int pagesProcessed) {
            this.limit = limit;
            this.limitRemaining = limitRemaining;
            this.pagesProcessed = pagesProcessed;
        }

        protected Status(StreamInput in) throws IOException {
            limit = in.readVInt();
            limitRemaining = in.readVInt();
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(limit);
            out.writeVInt(limitRemaining);
            out.writeVInt(pagesProcessed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Total number of position that are emitted by this operator.
         */
        public int limit() {
            return limit;
        }

        /**
         * Count of pages that have been processed by this operator.
         */
        public int limitRemaining() {
            return limitRemaining;
        }

        /**
         * Count of pages that have been processed by this operator.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("limit", limit);
            builder.field("limit_remaining", limitRemaining);
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return limit == status.limit && limitRemaining == status.limitRemaining && pagesProcessed == status.pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(limit, limitRemaining, pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
