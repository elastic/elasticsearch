/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Source operator implementation that retrieves data from an {@link ExchangeSource}
 */
public class ExchangeSourceOperator extends SourceOperator {

    private final ExchangeSource source;
    private IsBlockedResult isBlocked = NOT_BLOCKED;
    private int pagesEmitted;
    private long rowsEmitted;

    public record ExchangeSourceOperatorFactory(Supplier<ExchangeSource> exchangeSources) implements SourceOperatorFactory {

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new ExchangeSourceOperator(exchangeSources.get());
        }

        @Override
        public String describe() {
            return "ExchangeSourceOperator[]";
        }
    }

    public ExchangeSourceOperator(ExchangeSource source) {
        this.source = source;
    }

    @Override
    public Page getOutput() {
        final var page = source.pollPage();
        if (page != null) {
            pagesEmitted++;
            rowsEmitted += page.getPositionCount();
        }
        return page;
    }

    @Override
    public boolean isFinished() {
        return source.isFinished();
    }

    @Override
    public void finish() {
        source.finish();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (isBlocked.listener().isDone()) {
            isBlocked = source.waitForReading();
            if (isBlocked.listener().isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public void close() {
        finish();
    }

    @Override
    public String toString() {
        return "ExchangeSourceOperator";
    }

    @Override
    public Status status() {
        return new Status(source.bufferSize(), pagesEmitted, rowsEmitted);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "exchange_source",
            Status::new
        );

        private final int pagesWaiting;
        private final int pagesEmitted;
        private final long rowsEmitted;

        Status(int pagesWaiting, int pagesEmitted, long rowsEmitted) {
            this.pagesWaiting = pagesWaiting;
            this.pagesEmitted = pagesEmitted;
            this.rowsEmitted = rowsEmitted;
        }

        Status(StreamInput in) throws IOException {
            pagesWaiting = in.readVInt();
            pagesEmitted = in.readVInt();

            if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                rowsEmitted = in.readVLong();
            } else {
                rowsEmitted = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(pagesWaiting);
            out.writeVInt(pagesEmitted);

            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ROWS_PROCESSED)) {
                out.writeVLong(rowsEmitted);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public int pagesWaiting() {
            return pagesWaiting;
        }

        public int pagesEmitted() {
            return pagesEmitted;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_waiting", pagesWaiting);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesWaiting == status.pagesWaiting && pagesEmitted == status.pagesEmitted && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesWaiting, pagesEmitted, rowsEmitted);
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
