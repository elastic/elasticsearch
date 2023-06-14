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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Abstract superclass for operators that accept a single page, modify it, and then return it.
 */
public abstract class AbstractPageMappingOperator implements Operator {
    private Page prev;
    private boolean finished = false;
    private int pagesProcessed;

    protected abstract Page process(Page page);

    @Override
    public abstract String toString();

    @Override
    public final boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public final void addInput(Page page) {
        prev = page;
    }

    @Override
    public final void finish() {
        finished = true;
    }

    @Override
    public final boolean isFinished() {
        return finished && prev == null;
    }

    @Override
    public final Page getOutput() {
        if (prev == null) {
            return null;
        }
        if (prev.getPositionCount() == 0) {
            return prev;
        }
        pagesProcessed++;
        Page p = process(prev);
        prev = null;
        return p;
    }

    @Override
    public final Status status() {
        return status(pagesProcessed);
    }

    protected Status status(int pagesProcessed) {
        return new Status(pagesProcessed);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "page_mapping",
            Status::new
        );

        private final int pagesProcessed;

        protected Status(int pagesProcessed) {
            this.pagesProcessed = pagesProcessed;
        }

        protected Status(StreamInput in) throws IOException {
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesProcessed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesProcessed == status.pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    @Override
    public final void close() {}
}
